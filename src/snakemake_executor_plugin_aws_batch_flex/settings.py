"""The plugin's settings: `aws-batch-flex-...` in a profile, `--aws-batch-flex-...` on the
command line. `from_profile` reads them from a profile the way Snakemake does, for smkflex."""

import dataclasses
import os
from dataclasses import dataclass, field
from typing import List, Optional

from snakemake_interface_executor_plugins.settings import (
    CommonSettings,
    ExecutorSettingsBase,
)

from .hostenv import DEFAULT_PATH as DEFAULT_HOST_ENV
from .jobdefs import DEFAULT_LOG_GROUP
from .sizing import DEFAULT_MEM_OVERHEAD_MIB

PLUGIN_NAME = "aws-batch-flex"
PROFILE_PREFIX = PLUGIN_NAME + "-"
# Seconds between status checks (and before the first): only the tests set it, for their fake
# Batch whose jobs end at once. Real Batch jobs take 30 s or more to start.
TEST_STATUS_SECONDS = os.environ.get("SMK_FLEX_TEST_STATUS_SECONDS")
MAX_SOURCES_MB = 100


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
            "is used (EC2 only, and only if the instances let containers reach it: "
            "jobs_use_instance_role in the Terraform setup).",
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
    no_usage_summary: bool = field(
        default=False,
        metadata={
            "help": "Don't print the per-rule summary of what the run used (vCPU-hours, memory, "
            "CPUs, disk, and the resources to paste into the profile) at the end of the run.",
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
    max_sources_mb: Optional[int] = field(
        default=MAX_SOURCES_MB,
        metadata={
            "help": "Every job gets the workflow folder (what git would track, or everything "
            ".gitignore doesn't exclude, and config/; single files over 10 MB are left out). "
            "Stop with a message when that is more than this many MB: data belongs in storage.",
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
    init_seconds_before_status_checks=int(TEST_STATUS_SECONDS or 10),
)


def host_mounts(s: ExecutorSettings) -> list:
    """[(host path, container path, read-only)] for the machine image's Snakemake and conda
    (EC2 only), mounted at the same path in the jobs."""
    mounts = []
    if s.bootstrap == "host":
        mounts.append((s.host_env_path, s.host_env_path, True))
    if s.conda_path:
        mounts.append((s.conda_path, s.conda_path, True))
    return mounts


def normalized(s: ExecutorSettings) -> ExecutorSettings:
    """The settings with every None that has a default replaced by the default, so the rest of
    the code needn't check (Snakemake can hand over None for a setting that wasn't given)."""
    changes = {}
    for f in dataclasses.fields(s):
        default = f.default if f.default is not dataclasses.MISSING else None
        if getattr(s, f.name) is None and default is not None:
            changes[f.name] = default
    return dataclasses.replace(s, **changes) if changes else s


def from_profile(profile: dict) -> ExecutorSettings:
    """The settings in a profile's config.yaml (keys aws-batch-flex-...), with the defaults for
    the rest, as Snakemake would read them."""
    kw = {}
    for f in dataclasses.fields(ExecutorSettings):
        value = profile.get(PROFILE_PREFIX + f.name.replace("_", "-"))
        if value is None:
            continue
        if f.type is bool and isinstance(value, str):
            value = value.lower() in ("true", "1", "yes")
        kw[f.name] = value
    return normalized(ExecutorSettings(**kw))
