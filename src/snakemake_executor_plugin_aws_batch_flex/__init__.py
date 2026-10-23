"""AWS Batch executor for Snakemake that runs each rule in the image from its
`container:` directive. Images don't need Snakemake installed."""

import asyncio
import importlib.metadata as md
import math
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import AsyncGenerator, List, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface
from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)

from .bootstrap import MODES, build_host_prelude, build_prelude, conda_snippet, image_python_snippet, job_tools_source
from .hostenv import DEFAULT_PATH as DEFAULT_HOST_ENV
from .jobdefs import DEFAULT_LOG_GROUP, JobDefinitions, describe_existing, is_ecr
from .naming import batch_job_name
from .orphans import find_orphans, workflow_id
from .costs import Usage
from .instances import Instances
from .preflight import SUBMIT_HINT, ami_snakemake_version, check_queue, check_role, queue_uses_spot, version_note
from .sizing import DEFAULT_MEM_OVERHEAD_MIB, fargate_size, job_requirements
from .tags import job_tags, parse_pairs, parse_resource_string


@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    queue: Optional[str] = field(
        default=None,
        metadata={
            "help": "AWS Batch job queue to submit to. A rule can override this with "
            "the resource `batch_queue`.",
            "env_var": False,
            "required": True,
        },
    )
    region: Optional[str] = field(
        default=None,
        metadata={
            "help": "AWS region. Defaults to the standard boto3 resolution "
            "(AWS_REGION, AWS_PROFILE, ~/.aws/config).",
            "env_var": False,
            "required": False,
        },
    )
    job_role_arn: Optional[str] = field(
        default=None,
        metadata={
            "help": "IAM role assumed by the job container (needs access to the "
            "storage bucket). If omitted, the compute environment's instance role "
            "is used (EC2 only).",
            "env_var": False,
            "required": False,
        },
    )
    execution_role_arn: Optional[str] = field(
        default=None,
        metadata={
            "help": "ECS task execution role. Required for Fargate and for pulling "
            "from private registries.",
            "env_var": False,
            "required": False,
        },
    )
    head_role_arn: Optional[str] = field(
        default=None,
        metadata={
            "help": "Only for `smkflex run`: IAM role of the head job that runs Snakemake on "
            "AWS (Terraform prints it as head_role_arn). Snakemake itself ignores it.",
            "env_var": False,
            "required": False,
        },
    )
    platform: Optional[str] = field(
        default=None,
        metadata={
            "help": "Compute platform of the main queue: 'ec2' (also covers EC2 Spot) or "
            "'fargate'. Default: detected per queue from its compute environment (EC2 and "
            "Fargate queues can be mixed via the batch_queue resource); falls back to ec2 "
            "when the queue cannot be inspected. A rule can force it with the "
            "batch_platform resource.",
            "env_var": False,
            "required": False,
        },
    )
    default_image: Optional[str] = field(
        default=None,
        metadata={
            "help": "Image for rules without a `container:` directive. Defaults to "
            "Snakemake's --container-image.",
            "env_var": False,
            "required": False,
        },
    )
    bootstrap: Optional[str] = field(
        default="auto",
        metadata={
            "help": "How to get Snakemake into job containers. 'host' (EC2, "
            "recommended; Nextflow-style): mount a Snakemake env pre-installed on the "
            "host (see snakemake-batch-flex-host-env), so images need only /bin/sh. "
            "'auto': install at job start only if the image lacks the matching version; "
            "'always': always install at job start; 'never': the image must already "
            "contain Snakemake and the storage plugins.",
            "env_var": False,
            "required": False,
        },
    )
    host_env_path: Optional[str] = field(
        default=DEFAULT_HOST_ENV,
        metadata={
            "help": "Path of the host-installed Snakemake env, mounted read-only at "
            "the same path in every container (bootstrap=host).",
            "env_var": False,
            "required": False,
        },
    )
    host_env_wait_seconds: Optional[int] = field(
        default=120,
        metadata={
            "help": "bootstrap=host: how long a job waits for the host env to become "
            "ready (instances installing it via user-data). Use 0 on a baked AMI.",
            "env_var": False,
            "required": False,
        },
    )
    install_prereqs: bool = field(
        default=False,
        metadata={
            "help": "Install-at-start modes (auto/always): if the image has no curl, wget or "
            "python3+pip, install curl/ca-certificates/bash with the image's package manager "
            "(apt/apk/dnf/yum). Needs root in the image and access to the distro mirrors.",
            "env_var": False,
            "required": False,
        },
    )
    script_python: Optional[str] = field(
        default="image",
        metadata={
            "help": "Which python runs Python script: rules. 'image': the image's python if it "
            "is 3.10 or newer (so the script sees the image's packages), else Snakemake's. "
            "'host': always Snakemake's own python. Per rule: the batch_script_python resource.",
            "env_var": False,
            "required": False,
        },
    )
    mem_overhead_mib: Optional[int] = field(
        default=DEFAULT_MEM_OVERHEAD_MIB,
        metadata={
            "help": "MiB added to each rule's mem_mb/mem_mib for the Snakemake process that "
            "runs inside the job and shares its memory limit. 0 = request exactly mem_mb.",
            "env_var": False,
            "required": False,
        },
    )
    skip_preflight: bool = field(
        default=False,
        metadata={
            "help": "Skip the startup checks of the queue, its compute environment and the job role.",
            "env_var": False,
            "required": False,
        },
    )
    conda_path: Optional[str] = field(
        default=None,
        metadata={
            "help": "Host-installed conda (see snakemake-batch-flex-host-env conda / bake-ami.sh "
            "--conda), mounted read-only at the same path. Lets any image run conda: rules, "
            "including existing named environments in <path>/envs. EC2 only.",
            "env_var": False,
            "required": False,
        },
    )
    volumes: List[str] = field(
        default_factory=list,
        metadata={
            "help": "Extra host bind mounts, HOST:CONTAINER[:ro] (EC2 only), like "
            "Nextflow's aws.batch.volumes. Example: /scratch:/scratch",
            "env_var": False,
            "required": False,
            "nargs": "+",
        },
    )
    extra_packages: List[str] = field(
        default_factory=list,
        metadata={
            "help": "Extra pip requirements installed alongside Snakemake when "
            "bootstrapping (e.g. additional Snakemake plugins).",
            "env_var": False,
            "required": False,
            "nargs": "+",
        },
    )
    python_version: Optional[str] = field(
        default=None,
        metadata={
            "help": "Python version for the bootstrap venv. Defaults to the version "
            "running this Snakemake.",
            "env_var": False,
            "required": False,
        },
    )
    spot_retries: Optional[int] = field(
        default=2,
        metadata={
            "help": "How many times AWS Batch itself retries a job whose host was "
            "reclaimed (Spot interruption), 0-9; 0 = none. Independent of Snakemake's --retries. "
            "Per rule: the batch_spot_retries resource.",
            "env_var": False,
            "required": False,
        },
    )
    scheduling_priority: Optional[int] = field(
        default=None,
        metadata={
            "help": "Scheduling priority (0-9999, higher runs first) for every job. Only works on "
            "queues with a fair-share scheduling policy; skipped with a warning otherwise. Per "
            "rule: the batch_priority resource.",
            "env_var": False,
            "required": False,
        },
    )
    log_group: Optional[str] = field(
        default=DEFAULT_LOG_GROUP,
        metadata={
            "help": "CloudWatch log group for job logs. The default needs no setup.",
            "env_var": False,
            "required": False,
        },
    )
    job_definition: Optional[str] = field(
        default=None,
        metadata={
            "help": "Use this existing job definition (name, name:revision or ARN) instead of "
            "registering one per image; per rule: the batch_job_definition resource. It fixes "
            "the image, job role, mounts and /dev/shm; vCPUs, memory, timeout and tags still "
            "come from the rules. Without the host-env mount, jobs install Snakemake at start.",
            "env_var": False,
            "required": False,
        },
    )
    job_definition_prefix: Optional[str] = field(
        default="snakemake",
        metadata={
            "help": "Prefix for auto-registered job definitions.",
            "env_var": False,
            "required": False,
        },
    )
    assign_public_ip: Optional[str] = field(
        default="ENABLED",
        metadata={
            "help": "Fargate only: 'ENABLED' or 'DISABLED'. DISABLED needs a NAT "
            "gateway or VPC endpoints for image pulls and bootstrapping.",
            "env_var": False,
            "required": False,
        },
    )
    ephemeral_storage_gib: Optional[int] = field(
        default=None,
        metadata={
            "help": "Fargate only: minimum scratch disk in GiB (21-200); a rule's disk_mb can "
            "raise it for that rule.",
            "env_var": False,
            "required": False,
        },
    )
    registry_credentials: List[str] = field(
        default_factory=list,
        metadata={
            "help": "REGISTRY=SECRET_ARN pairs for private registries outside AWS, e.g. "
            "docker.io=arn:aws:secretsmanager:...:secret:dockerhub (the secret holds "
            "{\"username\": ..., \"password\": ...}). Only images from that registry use it. "
            "Needs --aws-batch-flex-execution-role-arn with secretsmanager:GetSecretValue on "
            "the secret. ECR images need no credentials (the instance/execution role pulls them).",
            "env_var": False,
            "required": False,
            "nargs": "+",
        },
    )
    orphan_jobs: Optional[str] = field(
        default=None,
        metadata={
            "help": "What to do with jobs still running from an earlier, interrupted run of "
            "the same workflow (same directory, Snakefile and storage prefix), which "
            "Snakemake would otherwise run a second time: 'cancel' (default), 'warn' "
            "(default with --nolock) or 'ignore'. Needs batch:ListJobs.",
            "env_var": False,
            "required": False,
        },
    )
    tags: List[str] = field(
        default_factory=list,
        metadata={
            "help": "KEY=VALUE tags applied to every job and to job definitions "
            "(cost allocation). Rules can add or override tags for their own jobs with "
            "the resource batch_tags=\"k1=v1,k2=v2\".",
            "env_var": False,
            "required": False,
            "nargs": "+",
        },
    )
    conda_cache: Optional[str] = field(
        default="instance",
        metadata={
            "help": "Reuse conda environments instead of building them in every job: "
            "'instance' (jobs on the same EC2 machine share them), an S3 URL such as "
            "s3://BUCKET/snakemake/.smkflex/conda-cache (also kept there for later jobs and "
            "runs), or 'none'.",
            "env_var": False,
            "required": False,
        },
    )
    oom_memory_factor: Optional[float] = field(
        default=2.0,
        metadata={
            "help": "When a job ran out of memory and Snakemake retries it (--retries, or a "
            "rule's retries:), ask for this many times more memory on each retry. 1 = never.",
            "env_var": False,
            "required": False,
        },
    )
    no_cost_summary: bool = field(
        default=False,
        metadata={
            "help": "Don't print the per-rule summary of vCPU-hours, memory and estimated cost "
            "at the end of the run.",
            "env_var": False,
            "required": False,
        },
    )
    no_auto_tags: bool = field(
        default=False,
        metadata={
            "help": "Disable the automatic job tags snakemake-run-id, snakemake-workflow, "
            "snakemake-rule and snakemake-job-id (on by default, for per-rule cost reporting). "
            "Also turns off the check for jobs left over from an interrupted run.",
            "env_var": False,
            "required": False,
        },
    )
    runnable_warn_minutes: Optional[int] = field(
        default=15,
        metadata={
            "help": "Warn when a job has been RUNNABLE (waiting for capacity) this "
            "long; usually means the request exceeds what the compute environment "
            "can provide.",
            "env_var": False,
            "required": False,
        },
    )


common_settings = CommonSettings(
    non_local_exec=True,
    implies_no_shared_fs=True,
    job_deploy_sources=True,
    pass_default_storage_provider_args=True,
    pass_default_resources_args=True,
    # Env vars go into the Batch job's `environment`, not into the command line.
    pass_envvar_declarations_to_cmd=False,
    # We install storage plugins ourselves, version-pinned, inside the bootstrap venv.
    auto_deploy_default_storage_provider=False,
    init_seconds_before_status_checks=10,
)

FARGATE_MAX_DISK_GIB = 200
CONDA_CACHE_HOST_DIR = "/var/cache/smkflex-conda"
CONDA_CACHE_JOB_DIR = "/tmp/smkflex-conda"  # condacache.ROOT
FARGATE_MAX_MEM_MIB = 122880
WRAPPER_HTTP_PREFIX = "https://github.com/snakemake/snakemake-wrappers/raw/"

_STATUS_RUNNING = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}


def _normalize_image(url: str) -> str:
    if url.startswith("docker://"):
        return url[len("docker://") :]
    if re.match(r"^[a-z]+://", url):
        raise WorkflowError(
            f"Unsupported container URL '{url}': AWS Batch only runs Docker/OCI images from a "
            "registry such as Docker Hub, quay.io, ghcr.io or ECR (e.g. "
            "'docker://quay.io/biocontainers/samtools:1.21--h50ea8bc_0'). Singularity/Apptainer "
            "images (.sif, oras://, shub://) can't be used."
        )
    return url


def _known_problem_hints(log_lines: List[str]) -> List[str]:
    """Plain-language explanations for failures with a known cause, from the job's log."""
    text = "\n".join(log_lines)
    hints = []
    bad = " ".join(re.findall(r"unrecognized arguments:(.*)", text))
    plugins = sorted(set(re.findall(r"--storage-([a-z0-9]+)-", bad)))
    for name in plugins:
        hints.append(
            f"  hint: this machine has snakemake-storage-plugin-{name} installed but the job's "
            f"Snakemake (from the AMI, or installed at job start) doesn't, and Snakemake passes "
            f"every installed storage "
            f"plugin's options to its jobs. Uninstall it here (pip uninstall "
            f"snakemake-storage-plugin-{name}) or bake an AMI with --storage-plugin {name}."
        )
    missing = sorted(set(re.findall(r"ModuleNotFoundError: No module named '([A-Za-z0-9_.]+)'", text)))
    for mod in missing:
        hints.append(
            f"  hint: Python couldn't import {mod}. If the Snakefile (or a file it includes) imports "
            f"it, it's needed in every job too, because Snakemake reads the Snakefile there as well: "
            f"add it to the AMI with scripts/update-ami.sh --extra-package {mod.split('.')[0]} (the pip "
            f"name can differ), or to --aws-batch-flex-extra-packages for jobs that install Snakemake "
            f"at start. If a script: imports it, it belongs in the rule's image or conda env."
        )
    if re.search(r"\(403\).*(HeadObject|GetObject|PutObject|ListObjects).*Forbidden|AccessDenied.*(GetObject|PutObject)", text):
        hints.append(
            "  hint: the job wasn't allowed to read or write the bucket. Jobs use the job role "
            "(aws-batch-flex-job-role-arn), not your own login, and the Terraform setup only lets "
            "it use the s3_prefix folder (s3://BUCKET/snakemake/...). Keep default-storage-prefix "
            "inside that folder, or give the role access to the other one."
        )
    if "No space left on device" in text:
        hints.append(
            "  hint: the job ran out of disk. On EC2, all jobs on an instance share its disk "
            "(root_volume_gib in Terraform), and disk_mb isn't enforced there: use a bigger "
            "volume, a queue with bigger disks for this rule (batch_queue), or fewer jobs per "
            "instance. On Fargate, raise the rule's disk_mb (up to 200 GiB)."
        )
    if not plugins and "unrecognized arguments" in text:
        hints.append(
            "  hint: the job's Snakemake didn't understand an option from this machine's "
            "Snakemake; the two versions are probably too far apart (compare "
            "`snakemake --version` here with the job's; 9.5 to 9.27 are known to mix fine)."
        )
    return hints


def _skip_remote_module_scripts():
    """Two fixes to what Snakemake packs for the jobs. (1) Work around Snakemake (9.27): packing the workflow's sources for the jobs crashes with
    "TypeError: ... not GithubFile" when a rule comes from a module fetched from GitHub (as
    snakedeploy sets up), because it treats the rule's script as a local file. Those scripts
    don't need packing: each job fetches them from GitHub itself. So they're hidden while the
    sources are collected. (2) Everything in config/ is packed, see below."""
    try:
        from snakemake.dag import DAG
        from snakemake.sourcecache import LocalSourceFile, SourceFile
    except Exception:
        return
    orig = getattr(DAG, "get_sources", None)
    if orig is None or getattr(orig, "_skips_remote", False):
        return

    def get_sources(self):
        hidden = []
        for rule in getattr(self.workflow, "rules", []):
            base = getattr(rule, "basedir", None)
            if isinstance(base, SourceFile) and not isinstance(base, LocalSourceFile):
                for attr in ("script", "notebook"):
                    value = getattr(rule, attr, None)
                    if value:
                        hidden.append((rule, attr, value))
                        setattr(rule, attr, None)
        try:
            files = orig(self)
        finally:
            for rule, attr, value in hidden:
                setattr(rule, attr, value)
        # Sample sheets etc. in config/ (the standard place) only reach the jobs through
        # `git ls-files`, i.e. not at all in a folder that isn't a git checkout, such as one
        # made by snakedeploy. Send them anyway.
        if isinstance(files, set) and os.path.isdir("config"):
            for d, _dirs, names in os.walk("config"):
                files.update(os.path.join(d, n) for n in names)
        return files

    get_sources._skips_remote = True
    DAG.get_sources = get_sources


# Snakemake packs the sources before it creates the executor, so this can't wait for it.
_skip_remote_module_scripts()


def _recheck_s3_directories():
    """Work around snakemake-storage-plugin-s3 (0.3.6): a storage object remembers is_dir() =
    False from before the job ran, so a directory() output the job then uploads is reported
    "missing in storage". A remembered False is looked up again; a True still counts."""
    try:
        from snakemake_storage_plugin_s3 import StorageObject
    except Exception:
        return
    orig = getattr(StorageObject, "is_dir", None)
    if orig is None or getattr(orig, "_rechecks", False):
        return

    def is_dir(self):
        if getattr(self, "_is_dir", None) is False:
            self._is_dir = None
        return orig(self)

    is_dir._rechecks = True
    StorageObject.is_dir = is_dir


def _keep_unneeded_temp(cmd: str) -> str:
    """Drop --unneeded-temp-files from a job's command. Snakemake (9.x) tells the job to delete
    temp() outputs that no other job needs, but then waits for every output in storage after
    the job, and fails with "Missing files". Without the option the job uploads them, and
    Snakemake removes them from storage itself as with any other temp file."""
    return re.sub(r" --unneeded-temp-files(?: (?!-)(?:'[^']*'|\S+))+", "", cmd)


def _out_of_memory(d: dict) -> bool:
    """Batch/ECS says why a container stopped; out of memory is OutOfMemoryError."""
    c = d.get("container") or {}
    return "OutOfMemory" in (c.get("reason") or "") or "OutOfMemory" in (d.get("statusReason") or "")


def _pinned(package: str) -> str:
    try:
        return f"{package}=={md.version(package)}"
    except md.PackageNotFoundError:
        return package


def storage_requirements(default_provider: Optional[str] = None) -> List[str]:
    """pip requirements for the storage plugins installed here, pinned to these versions."""
    packages = set()
    try:
        from snakemake_interface_storage_plugins.registry import StoragePluginRegistry

        reg = StoragePluginRegistry()
        packages = {reg.get_plugin_package_name(n) for n in reg.get_registered_plugins()}
    except Exception:  # registry API drift: fall back to the default provider
        if default_provider:
            packages = {f"snakemake-storage-plugin-{default_provider}"}
    return [_pinned(p) for p in sorted(packages)]


class Executor(RemoteExecutor):
    def __post_init__(self):
        s: ExecutorSettings = self.workflow.executor_settings
        if s.platform not in (None, "ec2", "fargate"):
            raise WorkflowError("--aws-batch-flex-platform must be 'ec2' or 'fargate'.")
        if s.script_python not in (None, "image", "host"):
            raise WorkflowError("--aws-batch-flex-script-python must be 'image' or 'host'.")
        _recheck_s3_directories()
        if s.oom_memory_factor is not None and s.oom_memory_factor < 1:
            raise WorkflowError("--aws-batch-flex-oom-memory-factor must be 1 or more.")
        if s.bootstrap not in MODES:
            raise WorkflowError(f"--aws-batch-flex-bootstrap must be one of {MODES}.")
        mounts = []
        for item in s.volumes or []:
            parts = item.split(":")
            if len(parts) not in (2, 3) or (len(parts) == 3 and parts[2] != "ro"):
                raise WorkflowError(f"Invalid volume '{item}', expected HOST:CONTAINER[:ro].")
            mounts.append((parts[0], parts[1], len(parts) == 3))
        if s.bootstrap == "host":
            mounts.append((s.host_env_path, s.host_env_path, True))
        if s.conda_path:
            mounts.append((s.conda_path, s.conda_path, True))
        cache = (s.conda_cache or "none").strip()
        if cache not in ("none", "instance") and not cache.startswith("s3://"):
            raise WorkflowError(
                f"--aws-batch-flex-conda-cache must be none, instance or an s3:// URL, got '{cache}'."
            )
        self.conda_cache = cache
        if cache != "none":
            # the instance's folder, shared by its jobs (EC2; Fargate jobs keep their own)
            mounts.append((CONDA_CACHE_HOST_DIR, CONDA_CACHE_JOB_DIR, False))
        self.mounts = mounts
        registry_credentials = {}
        for item in s.registry_credentials or []:
            reg, sep, arn = item.partition("=")
            reg, arn = reg.strip().lower(), arn.strip()
            if not sep or not reg or not arn.startswith("arn:"):
                raise WorkflowError(
                    f"Invalid --aws-batch-flex-registry-credentials entry '{item}', "
                    "expected REGISTRY=SECRET_ARN (e.g. docker.io=arn:aws:secretsmanager:...)."
                )
            if is_ecr(reg):
                raise WorkflowError(
                    f"{reg} is ECR: it needs no credentials, the instance/execution role pulls from it."
                )
            registry_credentials["docker.io" if reg in ("index.docker.io", "registry-1.docker.io") else reg] = arn
        if registry_credentials and not s.execution_role_arn:
            raise WorkflowError(
                "--aws-batch-flex-registry-credentials needs --aws-batch-flex-execution-role-arn: "
                "ECS reads the registry secret with the execution role."
            )
        self.settings = s
        lock = getattr(getattr(self.workflow, "execution_settings", None), "lock", True)
        self.orphan_mode = s.orphan_jobs or ("cancel" if lock else "warn")
        if self.orphan_mode not in ("cancel", "warn", "ignore"):
            raise WorkflowError("--aws-batch-flex-orphan-jobs must be 'cancel', 'warn' or 'ignore'.")
        self.workflow_id = workflow_id(
            os.getcwd(),
            getattr(self.workflow, "main_snakefile", "") or "",
            getattr(self.workflow.storage_settings, "default_storage_prefix", "") or "",
        )
        self._orphans_checked: set = set()
        tags = parse_pairs(s.tags or [], "--aws-batch-flex-tags")
        self.run_id = uuid.uuid4().hex[:12]

        cfg = BotoConfig(
            region_name=s.region, retries={"max_attempts": 10, "mode": "adaptive"}
        )
        self.batch = boto3.client("batch", config=cfg)
        self.cwlogs = boto3.client("logs", config=cfg)
        self._cfg = cfg
        # which EC2 instances the jobs run on, for the cost at the end
        self.instances = Instances(boto3.client("ecs", config=cfg), boto3.client("ec2", config=cfg),
                                   getattr(getattr(self.batch, "meta", None), "region_name", None) or s.region)
        self.tags = tags
        self.jobdefs = JobDefinitions(
            self.batch,
            self.logger,
            prefix=s.job_definition_prefix,
            platform=s.platform or "ec2",
            job_role_arn=s.job_role_arn,
            execution_role_arn=s.execution_role_arn,
            log_group=s.log_group,
            assign_public_ip=s.assign_public_ip,
            ephemeral_storage_gib=s.ephemeral_storage_gib,
            tags=tags,
            mounts=mounts,
            registry_credentials=registry_credentials,
        )
        self.job_code = self._upload_job_code()
        self.default_image = s.default_image or getattr(
            self.workflow.remote_execution_settings, "container_image", None
        )
        if not self.default_image:
            raise WorkflowError(
                "No default image: set --aws-batch-flex-default-image or give every "
                "rule a `container:` directive."
            )
        self._platforms: dict = {}  # queue -> "ec2" | "fargate"
        self._preludes: dict = {}  # platform -> job prelude
        self._warned_fargate_mounts = False
        self._warned_fargate_shm = False
        self._warned_disk: set = set()
        self._existing_defs: dict = {}  # ref -> ExistingJobDefinition
        self._fair_share: dict = {}  # queue -> has a scheduling policy
        self._warned_existing: set = set()
        self.usage = Usage()
        self._oom: dict = {}  # job id -> times it ran out of memory
        self._spot: dict = {}  # queue -> runs on Spot
        if not s.skip_preflight:
            errors, warnings, detected = check_queue(self.batch, s.queue, s.platform)
            self._platforms[s.queue] = s.platform or detected or "ec2"
            if s.job_role_arn:
                e2, w2 = check_role(boto3.client("iam", config=cfg), s.job_role_arn, "Job role")
                errors, warnings = errors + e2, warnings + w2
            for w in warnings:
                self.logger.warning(f"preflight: {w}")
            if errors:
                raise WorkflowError("Startup check failed:\n  - " + "\n  - ".join(errors) +
                                    "\n(skip with --aws-batch-flex-skip-preflight)")
        main_platform = self._queue_platform(s.queue)
        self._handle_orphans(s.queue)
        if s.bootstrap == "host" and main_platform == "ec2" and not s.skip_preflight:
            ami = ami_snakemake_version(self.batch, boto3.client("ec2", config=cfg), s.queue)
            level, note = version_note(ami, md.version("snakemake"))
            if level == "warning":
                self.logger.warning(note)
            elif level == "info":
                self.logger.info(note)
        self._check_fargate_ok(main_platform, s.queue)
        self.logger.info(
            f"AWS Batch executor: queue={s.queue}, platform={main_platform}, "
            f"default image={self.default_image}, bootstrap={s.bootstrap}"
        )
        if not s.no_auto_tags:
            self.logger.info(f"Jobs are tagged snakemake-run-id={self.run_id}")

    def _upload_job_code(self) -> Optional[str]:
        """Put the job tools (conda env cache, usage figures) next to the workflow's files: too
        long to send with every job, the jobs' launcher loads them from there. None: jobs run
        without them."""
        import hashlib

        from . import headjob

        prefix = str(getattr(self.workflow.storage_settings, "default_storage_prefix", "") or "").rstrip("/")
        if not prefix:
            return None
        data = job_tools_source().encode()
        url = f"{prefix}/.smkflex/code/jobtools-{hashlib.sha256(data).hexdigest()[:12]}.py"
        try:
            if headjob.get(url) != data:
                headjob.put(url, data)
        except Exception as e:
            self.logger.warning(f"jobs run without the conda env cache and usage figures: could not "
                                f"upload their code to {url} ({e})")
            return None
        return url

    def _queue_platform(self, queue: str) -> str:
        """Platform of a queue: detected once from its compute environment (startup check),
        else --aws-batch-flex-platform, else ec2."""
        if queue not in self._platforms:
            detected = None
            if not self.settings.skip_preflight:
                errors, warnings, detected = check_queue(self.batch, queue)
                for w in warnings:
                    self.logger.warning(f"preflight: {w}")
                if errors:
                    raise WorkflowError("Startup check failed:\n  - " + "\n  - ".join(errors) +
                                        "\n(skip with --aws-batch-flex-skip-preflight)")
            self._platforms[queue] = detected or self.settings.platform or "ec2"
        return self._platforms[queue]

    def _handle_orphans(self, queue: str):
        if queue in self._orphans_checked or self.orphan_mode == "ignore" or self.settings.no_auto_tags:
            return
        self._orphans_checked.add(queue)
        try:
            found = find_orphans(
                self.batch, queue, self.jobdefs.prefix, self.workflow_id, self.run_id
            )
        except (ClientError, BotoCoreError) as e:
            self.logger.warning(f"Could not look for leftover jobs in queue {queue}: {e}")
            return
        if not found:
            return
        names = ", ".join(f"{name} ({status})" for _, name, status in found[:10])
        more = f" and {len(found) - 10} more" if len(found) > 10 else ""
        if self.orphan_mode == "warn":
            self.logger.warning(
                f"{len(found)} job(s) from an earlier run of this workflow are still active in "
                f"queue {queue}: {names}{more}. They may run twice; cancel them or use "
                "--aws-batch-flex-orphan-jobs cancel."
            )
            return
        self.logger.info(
            f"Cancelling {len(found)} job(s) left over from an earlier run of this workflow "
            f"in queue {queue}: {names}{more}"
        )
        for jid, _, _ in found:
            try:
                self.batch.terminate_job(jobId=jid, reason="Left over from an earlier Snakemake run")
            except (ClientError, BotoCoreError) as e:
                self.logger.warning(f"Could not cancel leftover job {jid}: {e}")

    def _platform_for(self, job: JobExecutorInterface, queue: str) -> str:
        forced = job.resources.get("batch_platform")
        if forced:
            forced = str(forced).lower()
            if forced not in ("ec2", "fargate"):
                raise WorkflowError(
                    f"{job.name}: batch_platform must be 'ec2' or 'fargate', got '{forced}'."
                )
            return forced
        return self._queue_platform(queue)

    def _int_resource(self, job, names, low, high, default):
        """First of `names` set on the job (else `default`), as an int in [low, high]."""
        for name in names:
            value = job.resources.get(name)
            if value is not None:
                break
        else:
            name, value = "setting", default
        if value is None:
            return None
        try:
            n = int(value)
            if not low <= n <= high:
                raise ValueError
        except (TypeError, ValueError):
            raise WorkflowError(f"{job.name}: {name} must be a whole number from {low} to {high}, got {value!r}.")
        return n

    def _spot_retries_for(self, job: JobExecutorInterface) -> int:
        attempts = job.resources.get("aws_batch_spot_attempts")  # the aws-batch plugin's name: attempts
        if attempts is not None and job.resources.get("batch_spot_retries") is None:
            return self._int_resource(job, ["aws_batch_spot_attempts"], 1, 10, None) - 1
        return self._int_resource(job, ["batch_spot_retries"], 0, 9, self.settings.spot_retries) or 0

    def _priority_for(self, job: JobExecutorInterface, queue: str) -> Optional[int]:
        priority = self._int_resource(
            job, ["batch_priority", "aws_batch_scheduling_priority"], 0, 9999, self.settings.scheduling_priority
        )
        if priority is None:
            return None
        if queue not in self._fair_share:
            try:
                q = self.batch.describe_job_queues(jobQueues=[queue])["jobQueues"]
                self._fair_share[queue] = bool(q and q[0].get("schedulingPolicyArn"))
            except (ClientError, BotoCoreError):
                self._fair_share[queue] = True  # can't tell: send it and let Batch decide
            if not self._fair_share[queue]:
                self.logger.warning(
                    f"Queue {queue} has no fair-share scheduling policy, so job priorities are "
                    "ignored for it (batch_priority / --aws-batch-flex-scheduling-priority)."
                )
        return priority if self._fair_share[queue] else None

    def _shm_for(self, job: JobExecutorInterface, platform: str) -> Optional[int]:
        # batch_shm_mb, or shared_memory_size_mb as in the aws-batch plugin
        value = job.resources.get("batch_shm_mb") or job.resources.get("shared_memory_size_mb")
        if not value:
            return None
        try:
            shm = int(value)
            if shm <= 0:
                raise ValueError
        except (TypeError, ValueError):
            raise WorkflowError(f"{job.name}: batch_shm_mb must be a positive number of MiB, got {value!r}.")
        if platform == "fargate":
            if not self._warned_fargate_shm:
                self._warned_fargate_shm = True
                self.logger.warning("Fargate doesn't support batch_shm_mb; /dev/shm stays at its default there.")
            return None
        return shm

    def _more_memory_after_oom(self, job: JobExecutorInterface, req, platform: str):
        """A retry of a job that ran out of memory asks for oom_memory_factor times more,
        for every time it ran out (Fargate: at most 120 GiB)."""
        times = self._oom.get(job.jobid, 0)
        factor = 2.0 if self.settings.oom_memory_factor is None else self.settings.oom_memory_factor
        if not times or factor <= 1:
            return
        before = req.memory_mib
        mem = int(before * factor**times)
        if platform == "fargate":
            mem = min(mem, FARGATE_MAX_MEM_MIB)
            req.vcpu, mem = fargate_size(float(req.vcpu), mem)
        req.memory_mib = mem
        self.logger.info(
            f"{job.name} (attempt {job.attempt}) ran out of memory with {before} MiB; "
            f"retrying with {mem} MiB."
        )

    def _disk_for(self, job: JobExecutorInterface, platform: str) -> Optional[int]:
        """Fargate scratch disk in GiB for this job, from disk_mb; None keeps the default
        (20 GiB, or ephemeral-storage-gib). EC2 jobs share the instance's disk."""
        if platform != "fargate":
            return None
        value = job.resources.get("disk_mb")
        if not value:
            return None
        # Snakemake's default for rules without inputs is a flat 50000; that isn't a request
        if str(value) == "50000" and not list(job.input):
            return None
        try:
            gib = math.ceil(int(job.resources.get("disk_mib") or value) / 1024)
        except (TypeError, ValueError):
            raise WorkflowError(f"{job.name}: disk_mb must be a number of MB, got {value!r}.")
        if gib <= 20:  # Fargate's default; 21 is the smallest size that can be set
            return None
        if gib > FARGATE_MAX_DISK_GIB:
            if job.name not in self._warned_disk:
                self._warned_disk.add(job.name)
                self.logger.warning(
                    f"{job.name} asks for {gib} GiB of disk; Fargate allows at most "
                    f"{FARGATE_MAX_DISK_GIB}, so it gets {FARGATE_MAX_DISK_GIB}."
                )
        # The size is part of the job definition, so round up to 25 GiB steps; otherwise
        # sizes derived from input files would register a new definition for nearly every job.
        return min(FARGATE_MAX_DISK_GIB, math.ceil(gib / 25) * 25)

    def _check_fargate_ok(self, platform: str, queue: str):
        if platform != "fargate":
            return
        if not self.settings.execution_role_arn:
            raise WorkflowError(
                f"Queue '{queue}' runs on Fargate, which requires --aws-batch-flex-execution-role-arn."
            )
        if self.mounts and not self._warned_fargate_mounts:
            self._warned_fargate_mounts = True
            self.logger.warning(
                "Fargate jobs get no host mounts (bootstrap=host env, --aws-batch-flex-volumes, "
                "--aws-batch-flex-conda-path): they install Snakemake at job start instead, and "
                "conda: rules need conda inside the image there."
            )

    def _bootstrap_for(self, platform: str, host_mount: bool = True) -> str:
        # Without the host env (Fargate, or a job definition that doesn't mount it):
        # install at job start instead.
        if self.settings.bootstrap == "host" and (platform == "fargate" or not host_mount):
            return "auto"
        return self.settings.bootstrap

    def _prelude_for(self, platform: str, host_mount: bool = True, conda_mount: bool = True) -> str:
        key = (platform, host_mount, conda_mount)
        if key not in self._preludes:
            base = self._base_prelude(self._bootstrap_for(platform, host_mount))
            if self.settings.conda_path and platform == "ec2" and conda_mount:
                base += "\n" + conda_snippet(self.settings.conda_path)
            base += "\n" + image_python_snippet()  # SMK_FLEX_SCRIPT_PYTHON decides per job
            self._preludes[key] = base
        return self._preludes[key]

    def _existing_jobdef(self, job: JobExecutorInterface, platform: str, image: str, shm_mb, disk_gib=None):
        """The existing job definition this job should use, or None; warns once per rule
        about rule settings that the job definition overrides."""
        ref = job.resources.get("batch_job_definition") or self.settings.job_definition
        if not ref:
            return None
        ref = str(ref)
        if ref not in self._existing_defs:
            self._existing_defs[ref] = describe_existing(self.batch, ref)
        ex = self._existing_defs[ref]
        if ex.platform != platform:
            raise WorkflowError(
                f"{job.name}: job definition '{ref}' is for {ex.platform.upper()} but queue "
                f"{self._queue_for(job)} runs on {platform.upper()}."
            )
        key = (job.name, ref)
        if key not in self._warned_existing:
            self._warned_existing.add(key)
            notes = []
            subjobs = list(job.jobs) if job.is_group() else [job]
            if image != ex.image and any(getattr(j, "container_img_url", None) for j in subjobs):
                notes.append(f"its image {ex.image} is used, not {image}")
            if self.settings.bootstrap == "host" and self.settings.host_env_path not in ex.container_paths:
                notes.append(f"it doesn't mount {self.settings.host_env_path}, so Snakemake is installed at job start")
            if shm_mb:
                notes.append("batch_shm_mb is ignored (set sharedMemorySize in the job definition)")
            if disk_gib:
                notes.append("disk_mb is ignored (set ephemeralStorage in the job definition)")
            if self.settings.job_role_arn and ex.job_role and ex.job_role != self.settings.job_role_arn:
                notes.append(f"its job role {ex.job_role} is used")
            if notes:
                self.logger.warning(f"{job.name} uses job definition '{ref}': " + "; ".join(notes) + ".")
        return ex

    def _base_prelude(self, mode: str) -> str:
        if mode == "host":
            return build_host_prelude(
                self.settings.host_env_path, self.settings.host_env_wait_seconds
            )
        smk_version = md.version("snakemake")
        default = getattr(self.workflow.storage_settings, "default_storage_provider", None)
        reqs = [f"snakemake=={smk_version}"] + storage_requirements(default)
        reqs += list(self.settings.extra_packages or [])
        pyver = self.settings.python_version or f"{sys.version_info[0]}.{sys.version_info[1]}"
        return build_prelude(
            mode, smk_version, reqs, pyver, self.settings.install_prereqs
        )

    def _image_for(self, job: JobExecutorInterface) -> str:
        subjobs = list(job.jobs) if job.is_group() else [job]
        images = set()
        for j in subjobs:
            # `container_img_url` is a Snakemake Job attribute, not (yet) part of the
            # stable executor-plugin interface, hence getattr.
            url = getattr(j, "container_img_url", None)
            images.add(_normalize_image(url) if url else self.default_image)
        if len(images) > 1:
            raise WorkflowError(
                f"Group job {job.name} mixes container images {sorted(images)}; "
                "all rules in a group must use the same image."
            )
        return images.pop()

    def _queue_for(self, job: JobExecutorInterface) -> str:
        return str(job.resources.get("batch_queue") or self.settings.queue)

    def run_job(self, job: JobExecutorInterface):
        image = self._image_for(job)
        queue = self._queue_for(job)
        platform = self._platform_for(job, queue)
        self._handle_orphans(queue)
        self._check_fargate_ok(platform, queue)
        overhead = self.settings.mem_overhead_mib
        if overhead is None:
            overhead = DEFAULT_MEM_OVERHEAD_MIB
        req = job_requirements(job.resources, job.threads, platform, overhead)
        self._more_memory_after_oom(job, req, platform)
        script_python = str(
            job.resources.get("batch_script_python") or self.settings.script_python or "image"
        ).lower()
        if script_python not in ("image", "host"):
            raise WorkflowError(
                f"{job.name}: batch_script_python must be 'image' or 'host', got '{script_python}'."
            )
        shm_mb = self._shm_for(job, platform)
        disk_gib = self._disk_for(job, platform)
        existing = self._existing_jobdef(job, platform, image, shm_mb, disk_gib)
        if existing:
            jobdef, image = existing.arn, existing.image
            host_mount = self.settings.host_env_path in existing.container_paths
            conda_mount = bool(self.settings.conda_path) and self.settings.conda_path in existing.container_paths
        else:
            jobdef = self.jobdefs.ensure(image, platform, shm_mb, disk_gib)
            host_mount = conda_mount = True

        script = f"{self._prelude_for(platform, host_mount, conda_mount)}\n{_keep_unneeded_temp(self.format_job_exec(job))}"
        resource_reqs = [
            {"type": "VCPU", "value": req.vcpu},
            {"type": "MEMORY", "value": str(req.memory_mib)},
        ]
        if req.gpu:
            resource_reqs.append({"type": "GPU", "value": str(req.gpu)})

        kwargs = dict(
            jobName=batch_job_name(self.get_jobname(job)),
            jobQueue=queue,
            jobDefinition=jobdef,
            containerOverrides={
                "command": ["/bin/sh", "-c", script],
                "resourceRequirements": resource_reqs,
                "environment": [
                    {"name": k, "value": str(v)} for k, v in self.envvars().items()
                ]
                + [{"name": "SMK_FLEX_SCRIPT_PYTHON", "value": script_python}]
                + ([{"name": "SMK_FLEX_JOB_CODE", "value": self.job_code}] if self.job_code else [])
                + ([{"name": "SMK_FLEX_CONDA_CACHE", "value": self.conda_cache}]
                   if self.job_code and self.conda_cache != "none" else []),
            },
        )
        spot_retries = self._spot_retries_for(job)
        if spot_retries:
            kwargs["retryStrategy"] = {
                "attempts": spot_retries + 1,
                "evaluateOnExit": [
                    {"onStatusReason": "Host EC2*", "action": "RETRY"},
                    {"onReason": "*", "action": "EXIT"},
                ],
            }
        priority = self._priority_for(job, queue)
        if priority is not None:
            kwargs["schedulingPriorityOverride"] = priority
        if req.timeout_seconds:
            kwargs["timeout"] = {"attemptDurationSeconds": req.timeout_seconds}
        tags = job_tags(
            run_id=self.run_id,
            workflow_id=self.workflow_id,
            # Snakemake returns plain names; the interface type says rule objects. Accept both.
            rule_names=[getattr(r, "name", r) for r in job.rules],
            jobid=job.jobid,
            global_tags=self.tags,
            rule_tags=parse_resource_string(job.resources.get("batch_tags")),
            auto=not self.settings.no_auto_tags,
        )
        if tags:
            kwargs["tags"] = tags
            kwargs["propagateTags"] = True

        try:
            resp = self.batch.submit_job(**kwargs)
        except (ClientError, BotoCoreError) as e:
            hint = f"\n{SUBMIT_HINT}" if "AccessDenied" in str(e) or "not authorized" in str(e) else ""
            raise WorkflowError(f"AWS Batch submit_job failed: {e}{hint}")

        self.logger.info(
            f"Submitted {job.name} as AWS Batch job {resp['jobId']} "
            f"({image}, {req.vcpu} vCPU, {req.memory_mib} MiB, queue {queue}, {platform})"
        )
        self.report_job_submission(
            SubmittedJobInfo(
                job,
                external_jobid=resp["jobId"],
                aux={"image": image, "queue": queue, "platform": platform, "warned": False,
                     "vcpu": float(req.vcpu), "mem": req.memory_mib},
            )
        )

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        by_id = {j.external_jobid: j for j in active_jobs}
        ids = list(by_id)
        described = {}
        for i in range(0, len(ids), 100):  # describe_jobs takes at most 100 ids
            async with self.status_rate_limiter:
                try:
                    resp = await asyncio.to_thread(
                        self.batch.describe_jobs, jobs=ids[i : i + 100]
                    )
                except (ClientError, BotoCoreError) as e:
                    self.logger.warning(f"AWS Batch status query failed, will retry: {e}")
                    for jid in ids[i : i + 100]:
                        yield by_id[jid]
                    continue
            for d in resp["jobs"]:
                described[d["jobId"]] = d

        for jid, info in by_id.items():
            d = described.get(jid)
            if d is None:
                self.report_job_error(
                    info, msg=f"AWS Batch no longer knows job {jid} (expired?)."
                )
                continue
            status = d["status"]
            if status in ("SUCCEEDED", "FAILED"):
                self._record_usage(info, d)
            if status == "SUCCEEDED":
                self.report_job_success(info)
            elif status == "FAILED":
                if _out_of_memory(d):
                    self._oom[info.job.jobid] = self._oom.get(info.job.jobid, 0) + 1
                msg = await asyncio.to_thread(self._failure_message, d, info)
                self.report_job_error(info, msg=msg)
            elif status in _STATUS_RUNNING:
                arn = (d.get("container") or {}).get("containerInstanceArn")
                if arn and arn not in self.instances.ids:
                    await asyncio.to_thread(self.instances.note, arn)
                self._warn_if_stuck(d, info)
                yield info
            else:
                yield info

    def _record_usage(self, info: SubmittedJobInfo, d: dict):
        queue = info.aux.get("queue")
        if queue not in self._spot:
            self._spot[queue] = not self.settings.skip_preflight and queue_uses_spot(self.batch, queue)
        platform = info.aux.get("platform", "ec2")
        known = platform == "ec2" and self.instances.add_job(info.job.name, d, info.aux.get("vcpu", 1.0))
        self.usage.add(info.job.name, d, info.aux.get("vcpu", 1.0), info.aux.get("mem", 0),
                       platform, self._spot[queue], on_known_instance=known)
        c = d.get("container") or {}
        if c.get("logStreamName") and d.get("status") == "SUCCEEDED":
            group = (c.get("logConfiguration") or {}).get("options", {}).get("awslogs-group", self.settings.log_group)
            self.usage.add_log(info.job.name, group, c["logStreamName"])

    def shutdown(self):
        super().shutdown()
        per_rule, line = None, None
        if not self.settings.no_cost_summary:
            self.usage.fetch_peaks(self.cwlogs)
            if self.instances.vcpu_seconds:
                try:
                    # the price list is served from us-east-1, for every region
                    pricing = boto3.client("pricing", region_name="us-east-1")
                    per_rule, line = self.instances.costs(pricing)
                except Exception as e:
                    per_rule, line = None, f"{type(e).__name__}"
                if per_rule is None:
                    line = (f"EC2 cost estimated from the jobs' vCPUs, not priced by instance: {line}"
                            " (see IAM in the plugin's README).")
        overhead = DEFAULT_MEM_OVERHEAD_MIB if self.settings.mem_overhead_mib is None else self.settings.mem_overhead_mib
        table = None if self.settings.no_cost_summary else self.usage.table(overhead, per_rule, line)
        if table:
            self.logger.info(table)

    def additional_general_args(self):
        # A version-tagged wrapper ("v9.19.1/bio/...") is fetched with `git clone` when no prefix is
        # set, and stock images have no git. The plain HTTP prefix fetches the same files without git.
        # (Snakemake joins the result as a string, despite the interface docstring saying list.)
        if getattr(self.workflow.workflow_settings, "wrapper_prefix", None) is None:
            return f"--wrapper-prefix {WRAPPER_HTTP_PREFIX}"
        return ""

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        for info in active_jobs:
            try:
                self.batch.terminate_job(
                    jobId=info.external_jobid, reason="Cancelled by Snakemake"
                )
            except (ClientError, BotoCoreError) as e:
                self.logger.warning(f"Could not terminate {info.external_jobid}: {e}")

    def _warn_if_stuck(self, d: dict, info: SubmittedJobInfo):
        if d["status"] != "RUNNABLE" or info.aux.get("warned"):
            return
        waited = (time.time() * 1000 - d.get("createdAt", time.time() * 1000)) / 60000
        if waited >= self.settings.runnable_warn_minutes:
            info.aux["warned"] = True
            self.logger.warning(
                f"AWS Batch job {d['jobId']} has waited {waited:.0f} min in RUNNABLE "
                f"on queue {info.aux['queue']}. Usually the compute environment cannot "
                "provide the requested vCPU/memory (max vCPUs reached, instance types "
                "too small, or subnets without capacity). Check the Batch console."
            )

    def _failure_message(self, d: dict, info: SubmittedJobInfo) -> str:
        c = d.get("container", {})
        lines = [
            f"AWS Batch job {d['jobId']} FAILED (image {info.aux.get('image')}).",
        ]
        if d.get("statusReason"):
            lines.append(f"  status reason: {d['statusReason']}")
        if c.get("reason"):
            lines.append(f"  container reason: {c['reason']}")
        if c.get("exitCode") is not None:
            lines.append(f"  exit code: {c['exitCode']}")
        if _out_of_memory(d):
            mem = info.aux.get("mem")
            factor = self.settings.oom_memory_factor
            retries = getattr(info.job, "restart_times", 0) or 0
            if retries >= info.job.attempt and (factor is None or factor > 1):
                lines.append(f"  hint: the job ran out of memory ({mem} MiB); Snakemake retries it with more.")
            else:
                lines.append(
                    f"  hint: the job ran out of memory ({mem} MiB). Raise the rule's mem_mb, or run with "
                    "--retries 2: the plugin then retries jobs that ran out of memory with twice as much."
                )
        stream = c.get("logStreamName")
        if stream:
            group = (
                c.get("logConfiguration", {})
                .get("options", {})
                .get("awslogs-group", self.settings.log_group)
            )
            lines.append(f"  CloudWatch: group {group}, stream {stream}")
            try:
                events = self.cwlogs.get_log_events(
                    logGroupName=group,
                    logStreamName=stream,
                    limit=40,
                    startFromHead=False,
                )["events"]
                if events:
                    lines.append("  last log lines:")
                    lines += [f"    {e['message'].rstrip()}" for e in events]
                lines += _known_problem_hints([e["message"] for e in events])
            except (ClientError, BotoCoreError):
                lines.append("  (could not fetch log tail)")
        return "\n".join(lines) + "\n"
