"""AWS Batch executor for Snakemake that runs each rule in the image from its
`container:` directive. Images don't need Snakemake installed."""

import asyncio
import importlib.metadata as md
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

from .bootstrap import MODES, build_host_prelude, build_prelude, conda_snippet, image_python_snippet
from .hostenv import DEFAULT_PATH as DEFAULT_HOST_ENV
from .jobdefs import DEFAULT_LOG_GROUP, JobDefinitions, is_ecr
from .naming import batch_job_name
from .preflight import SUBMIT_HINT, check_queue, check_role
from .sizing import DEFAULT_MEM_OVERHEAD_MIB, job_requirements
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
            "reclaimed (Spot interruption). Independent of Snakemake's --retries.",
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
            "help": "Fargate only: scratch disk in GiB (21-200).",
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
    no_auto_tags: bool = field(
        default=False,
        metadata={
            "help": "Disable the automatic job tags snakemake-run-id, snakemake-rule "
            "and snakemake-job-id (on by default, for per-rule cost reporting).",
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

WRAPPER_HTTP_PREFIX = "https://github.com/snakemake/snakemake-wrappers/raw/"

_STATUS_RUNNING = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}


def _normalize_image(url: str) -> str:
    if url.startswith("docker://"):
        return url[len("docker://") :]
    if re.match(r"^[a-z]+://", url):
        raise WorkflowError(
            f"Unsupported container URL '{url}': AWS Batch can only run images from "
            "a Docker registry (e.g. 'docker://ubuntu:24.04' or 'ubuntu:24.04')."
        )
    return url


def _pinned(package: str) -> str:
    try:
        return f"{package}=={md.version(package)}"
    except md.PackageNotFoundError:
        return package


class Executor(RemoteExecutor):
    def __post_init__(self):
        s: ExecutorSettings = self.workflow.executor_settings
        if s.platform not in (None, "ec2", "fargate"):
            raise WorkflowError("--aws-batch-flex-platform must be 'ec2' or 'fargate'.")
        if s.script_python not in (None, "image", "host"):
            raise WorkflowError("--aws-batch-flex-script-python must be 'image' or 'host'.")
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
        tags = parse_pairs(s.tags or [], "--aws-batch-flex-tags")
        self.run_id = uuid.uuid4().hex[:12]

        cfg = BotoConfig(
            region_name=s.region, retries={"max_attempts": 10, "mode": "adaptive"}
        )
        self.batch = boto3.client("batch", config=cfg)
        self.cwlogs = boto3.client("logs", config=cfg)
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
        self._check_fargate_ok(main_platform, s.queue)
        self.logger.info(
            f"AWS Batch executor: queue={s.queue}, platform={main_platform}, "
            f"default image={self.default_image}, bootstrap={s.bootstrap}"
        )
        if not s.no_auto_tags:
            self.logger.info(f"Jobs are tagged snakemake-run-id={self.run_id}")

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

    def _bootstrap_for(self, platform: str) -> str:
        # Fargate cannot mount the host env: fall back to installing at job start.
        if platform == "fargate" and self.settings.bootstrap == "host":
            return "auto"
        return self.settings.bootstrap

    def _prelude_for(self, platform: str) -> str:
        if platform not in self._preludes:
            base = self._base_prelude(self._bootstrap_for(platform))
            if self.settings.conda_path and platform == "ec2":
                base += "\n" + conda_snippet(self.settings.conda_path)
            base += "\n" + image_python_snippet()  # SMK_FLEX_SCRIPT_PYTHON decides per job
            self._preludes[platform] = base
        return self._preludes[platform]

    def _base_prelude(self, mode: str) -> str:
        if mode == "host":
            return build_host_prelude(
                self.settings.host_env_path, self.settings.host_env_wait_seconds
            )
        smk_version = md.version("snakemake")
        reqs = [f"snakemake=={smk_version}"]
        packages = set()
        try:
            from snakemake_interface_storage_plugins.registry import (
                StoragePluginRegistry,
            )

            reg = StoragePluginRegistry()
            packages = {
                reg.get_plugin_package_name(n) for n in reg.get_registered_plugins()
            }
        except Exception:  # registry API drift: fall back to the default provider
            default = getattr(
                self.workflow.storage_settings, "default_storage_provider", None
            )
            if default:
                packages = {f"snakemake-storage-plugin-{default}"}
        reqs += [_pinned(p) for p in sorted(packages)]
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
        self._check_fargate_ok(platform, queue)
        overhead = self.settings.mem_overhead_mib
        if overhead is None:
            overhead = DEFAULT_MEM_OVERHEAD_MIB
        req = job_requirements(job.resources, job.threads, platform, overhead)
        script_python = str(
            job.resources.get("batch_script_python") or self.settings.script_python or "image"
        ).lower()
        if script_python not in ("image", "host"):
            raise WorkflowError(
                f"{job.name}: batch_script_python must be 'image' or 'host', got '{script_python}'."
            )
        jobdef = self.jobdefs.ensure(image, platform)

        script = f"{self._prelude_for(platform)}\n{self.format_job_exec(job)}"
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
                + [{"name": "SMK_FLEX_SCRIPT_PYTHON", "value": script_python}],
            },
        )
        if self.settings.spot_retries:
            kwargs["retryStrategy"] = {
                "attempts": self.settings.spot_retries + 1,
                "evaluateOnExit": [
                    {"onStatusReason": "Host EC2*", "action": "RETRY"},
                    {"onReason": "*", "action": "EXIT"},
                ],
            }
        if req.timeout_seconds:
            kwargs["timeout"] = {"attemptDurationSeconds": req.timeout_seconds}
        tags = job_tags(
            run_id=self.run_id,
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
                aux={"image": image, "queue": queue, "platform": platform, "warned": False},
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
            if status == "SUCCEEDED":
                self.report_job_success(info)
            elif status == "FAILED":
                msg = await asyncio.to_thread(self._failure_message, d, info)
                self.report_job_error(info, msg=msg)
            elif status in _STATUS_RUNNING:
                self._warn_if_stuck(d, info)
                yield info
            else:
                yield info

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
            except (ClientError, BotoCoreError):
                lines.append("  (could not fetch log tail)")
        return "\n".join(lines) + "\n"
