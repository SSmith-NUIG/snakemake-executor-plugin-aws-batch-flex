"""The executor: submits each Snakemake job as an AWS Batch job in its rule's image, follows
it, and prints what the run used at the end."""

import asyncio
import json
import importlib.metadata as md
import math
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import AsyncGenerator, List, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.jobs import JobExecutorInterface

from .bootstrap import conda_snippet, job_tools_source, launcher_snippet, snakemake_prelude
from .hints import known_problem_hints
from .imageinfo import Entrypoints
from .jobdefs import JobDefinitions, describe_existing, log_group_of, mutable_tag, normalize_image
from .naming import batch_job_name
from .orphans import ACTIVE, find_orphans, workflow_id
from .patches import NOT_APPLIED, keep_unneeded_temp, recheck_s3_directories
from .preflight import (
    SUBMIT_HINT,
    access_denied,
    ami_snakemake_version,
    check_queue,
    check_role,
    describe_queue,
    version_note,
)
from .settings import (
    ExecutorSettings,
    host_mounts,
    normalized,
    registry_secrets,
    validate,
    volume_mounts,
)
from .sizing import fargate_size, job_requirements
from .tags import job_tags, parse_pairs, parse_resource_string
from .usage import Usage

FARGATE_MAX_DISK_GIB = 200
CONDA_CACHE_HOST_DIR = "/var/cache/smkflex-conda"
CONDA_CACHE_JOB_DIR = "/tmp/smkflex-conda"  # condacache.ROOT
FARGATE_MAX_MEM_MIB = 122880
WRAPPER_HTTP_PREFIX = "https://github.com/snakemake/snakemake-wrappers/raw/"
ECS_OVERRIDES_MAX = 8192  # characters of a job's command and environment
CANCEL_THREADS = 16  # terminate_job calls at once when the run is cancelled
LOG_FILES, LOG_LINES = 3, 20  # of a failed job's log: files, shown with its error
LOG_READ_MAX = 2_000_000  # bytes of each read for that, from its start


def _out_of_memory(d: dict) -> bool:
    """Batch/ECS says why a container stopped; out of memory is OutOfMemoryError."""
    c = d.get("container") or {}
    return "OutOfMemory" in (c.get("reason") or "") or "OutOfMemory" in (d.get("statusReason") or "")


@dataclass
class Submitted:
    """What the plugin keeps about a submitted job (in SubmittedJobInfo.aux["smkflex"])."""
    image: str
    queue: str
    platform: str  # ec2 | fargate
    vcpu: float
    mem: int  # MiB the job got
    asked: int  # MiB its rule asked for (before a retry's extra)
    warned: bool = False  # about waiting long in RUNNABLE


def _sub(info: SubmittedJobInfo) -> Submitted:
    return info.aux["smkflex"]


def _traceback(lines, max_lines=30):
    """The last Python traceback in lines, up to its exception, or []."""
    starts = [i for i, line in enumerate(lines) if line.startswith("Traceback (most recent call last)")]
    if not starts:
        return []
    tb = []
    for line in lines[starts[-1]:starts[-1] + max_lines]:
        tb.append(line)
        if len(tb) > 1 and line and not line[0].isspace():  # the exception: "ImportError: ..."
            break
    return tb


def _log_tails(job, files=LOG_FILES, n=LOG_LINES):
    """[(name, lines to show)] of a failed job's log: files, which it uploaded before it ended:
    the last lines, and a Python traceback from elsewhere in the file. Scripts that open the log
    for stderr with "w" and for stdout with "a" (common in published pipelines) write their
    traceback over the start of it. Best effort: a file that isn't there or can't be read is
    left out."""
    out = []
    for f in list(getattr(job, "log", None) or [])[:files]:
        obj = getattr(f, "storage_object", None) if getattr(f, "is_storage", False) else None
        try:
            if obj is None:
                path, name = str(f), str(f)
            else:  # fetched to where the job had it, and removed again: only read here
                path, name = str(obj.local_path()), obj.print_query
                fetched = not os.path.exists(path)
                if fetched:
                    obj.local_path().parent.mkdir(parents=True, exist_ok=True)
                    obj.retrieve_object()
            with open(path, "rb") as fh:
                lines = fh.read(LOG_READ_MAX).decode(errors="replace").splitlines()
                if os.path.getsize(path) > LOG_READ_MAX:  # a big log: its end too
                    fh.seek(-16_000, os.SEEK_END)
                    end = fh.read().decode(errors="replace").splitlines()
                else:
                    end = lines
            if obj is not None and fetched:
                os.remove(path)
        except Exception:  # not there, not allowed, or a storage that can't do this
            continue
        tail = end[-n:]
        tb = _traceback(lines + end if end is not lines else lines)
        if tb and tb[-1] not in tail:
            tail = tb + ["..."] + tail
        if any(t.strip() for t in tail):
            out.append((name, tail))
    return out


def _not_allowed(e) -> bool:
    """Was a Batch call refused for want of a permission? Batch reports a missing iam:PassRole
    as a plain ClientException that says "not authorized"."""
    return isinstance(e, ClientError) and (
        access_denied(e) or "not authorized" in e.response.get("Error", {}).get("Message", ""))


class Executor(RemoteExecutor):
    def __post_init__(self):
        s: ExecutorSettings = validate(normalized(self.workflow.executor_settings))
        recheck_s3_directories()
        for what in NOT_APPLIED:
            self.logger.warning(
                f"This Snakemake ({md.version('snakemake')}) changed code the plugin works around, so "
                f"this workaround is off: {what}. Please report it to the plugin."
            )
        mounts = volume_mounts(s) + host_mounts(s)
        cache = s.conda_cache.strip()
        self.conda_cache = cache
        if cache != "none":
            # the instance's folder, shared by its jobs (EC2; Fargate jobs keep their own)
            mounts.append((CONDA_CACHE_HOST_DIR, CONDA_CACHE_JOB_DIR, False))
        self.mounts = mounts
        registry_credentials = registry_secrets(s)
        self.entrypoints = Entrypoints(registry_credentials, self.logger)
        self.settings = s
        # lock, default_storage_prefix, container_image and wrapper_prefix are Snakemake's but not
        # declared by the executor interface, hence getattr with a default
        lock = getattr(self.workflow.execution_settings, "lock", True)
        self.orphan_mode = s.orphan_jobs or ("cancel" if lock else "warn")
        self.workflow_id = workflow_id(
            os.getcwd(),
            self.workflow.main_snakefile or "",
            getattr(self.workflow.storage_settings, "default_storage_prefix", "") or "",
        )
        self._orphans_checked: set = set()
        tags = parse_pairs(s.tags or [], "--aws-batch-flex-tags")
        self.run_id = uuid.uuid4().hex[:12]
        # jobs that install Snakemake at start get the packages as they were now, all the same
        self.resolved_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        cfg = BotoConfig(
            region_name=s.region, retries={"max_attempts": 10, "mode": "adaptive"}
        )
        self.batch = boto3.client("batch", config=cfg)
        self.cwlogs = boto3.client("logs", config=cfg)
        self.region = self.batch.meta.region_name or s.region
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
        self._said_resolved = False
        self._warned_tags: set = set()
        # Batch jobs submitted and not seen to end. Cancelling stops these too: while Snakemake
        # checks on its running jobs, it takes them out of the list it would hand cancel_jobs.
        self._unfinished: set = set()
        self._warned_disk: set = set()
        self._existing_defs: dict = {}  # ref -> ExistingJobDefinition
        self._fair_share: dict = {}  # queue -> has a scheduling policy
        self._warned_existing: set = set()
        self.usage = Usage()
        self._oom: dict = {}  # job id -> (times it ran out of memory, MiB its rule asked for then)
        if not s.skip_preflight:
            errors, warnings, detected = check_queue(self.batch, s.queue, s.platform)
            self._platforms[s.queue] = s.platform or detected or "ec2"
            if s.job_role_arn:
                e2, w2 = check_role(boto3.client("iam", config=cfg), s.job_role_arn, "Job role")
                errors, warnings = errors + e2, warnings + w2
            self._report_checks(errors, warnings)
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
        # the jobs check the content against the sha256 in the name before running it
        url = f"{prefix}/.smkflex/code/jobtools-{hashlib.sha256(data).hexdigest()}.py"
        try:
            if headjob.get(url) != data:
                headjob.put(url, data)
        except (ClientError, BotoCoreError, OSError) as e:
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
                self._report_checks(errors, warnings)
            self._platforms[queue] = detected or self.settings.platform or "ec2"
        return self._platforms[queue]

    def _report_checks(self, errors, warnings):
        for w in warnings:
            self.logger.warning(f"preflight: {w}")
        if errors:
            raise WorkflowError("Startup check failed:\n  - " + "\n  - ".join(errors) +
                                "\n(skip with --aws-batch-flex-skip-preflight)")

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
            raise WorkflowError(f"{job.name}: {name} must be a whole number from {low} to {high}, got {value!r}.") from None
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
                q, _envs = describe_queue(self.batch, queue)
                self._fair_share[queue] = bool(q and q.get("schedulingPolicyArn"))
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
            raise WorkflowError(f"{job.name}: batch_shm_mb must be a positive number of MiB, got {value!r}.") from None
        if platform == "fargate":
            if not self._warned_fargate_shm:
                self._warned_fargate_shm = True
                self.logger.warning("Fargate doesn't support batch_shm_mb; /dev/shm stays at its default there.")
            return None
        return shm

    def _more_memory_after_oom(self, job: JobExecutorInterface, req, platform: str):
        """A retry of a job that ran out of memory asks for oom_memory_factor times more,
        for every time it ran out (Fargate: at most 120 GiB). Unless its rule already asks for
        more on this attempt (mem_mb=lambda wildcards, attempt: ...): that is used as it is."""
        times, asked = self._oom.get(job.jobid, (0, None))
        factor = self.settings.oom_memory_factor
        if not times or factor <= 1:
            return
        if asked is not None and req.memory_mib > asked:
            self.logger.info(
                f"{job.name} (attempt {job.attempt}) ran out of memory with {asked} MiB; its rule asks "
                f"for {req.memory_mib} MiB this time, so that is used."
            )
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
            raise WorkflowError(f"{job.name}: disk_mb must be a number of MB, got {value!r}.") from None
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
            default = getattr(self.workflow.storage_settings, "default_storage_provider", None)
            mode = self._bootstrap_for(platform, host_mount)
            base = snakemake_prelude(self.settings, mode, default, self.resolved_at)
            if mode in ("auto", "always") and not self._said_resolved:
                self._said_resolved = True
                self.logger.info(f"Jobs that install Snakemake at start get the packages as of {self.resolved_at}.")
            if self.settings.conda_path and platform == "ec2" and conda_mount:
                base += "\n" + conda_snippet(self.settings.conda_path)
            base += "\n" + launcher_snippet(bool(self.job_code))  # SMK_FLEX_SCRIPT_PYTHON decides per job
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

    def _image_for(self, job: JobExecutorInterface) -> str:
        subjobs = list(job.jobs) if job.is_group() else [job]
        images = set()
        for j in subjobs:
            # `container_img_url` is a Snakemake Job attribute, not (yet) part of the
            # stable executor-plugin interface, hence getattr.
            url = getattr(j, "container_img_url", None)
            images.add(normalize_image(url) if url else self.default_image)
        if len(images) > 1:
            raise WorkflowError(
                f"Group job {job.name} mixes container images {sorted(images)}; "
                "all rules in a group must use the same image."
            )
        image = images.pop()
        if mutable_tag(image) and image not in self._warned_tags:
            self._warned_tags.add(image)
            self.logger.warning(
                f"Image {image} has no version tag (or :latest), so what it contains can change "
                "between jobs and runs. Use a fixed tag (e.g. samtools:1.21--h50ea8bc_0) or a digest (@sha256:...)."
            )
        return image

    def _queue_for(self, job: JobExecutorInterface) -> str:
        return str(job.resources.get("batch_queue") or self.settings.queue)

    def run_job(self, job: JobExecutorInterface):
        image = self._image_for(job)
        queue = self._queue_for(job)
        platform = self._platform_for(job, queue)
        self._handle_orphans(queue)
        self._check_fargate_ok(platform, queue)
        req = job_requirements(job.resources, job.threads, platform, self.settings.mem_overhead_mib)
        asked = req.memory_mib  # by the rule, before the retry's extra
        self._more_memory_after_oom(job, req, platform)
        script_python = str(job.resources.get("batch_script_python") or self.settings.script_python).lower()
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

        script = f"{self._prelude_for(platform, host_mount, conda_mount)}\n{keep_unneeded_temp(self.format_job_exec(job))}"
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
                # what the image's ENTRYPOINT, which Batch can't change, runs as /bin/sh -c script
                "command": self.entrypoints.command(image, script, job.resources.get("batch_entrypoint"), job.name),
                "resourceRequirements": resource_reqs,
                "environment": [
                    {"name": k, "value": str(v)} for k, v in self.envvars().items()
                ]
                + [{"name": "SMK_FLEX_SCRIPT_PYTHON", "value": script_python}]
                # bioconda needs strict channel priority; an image's own conda often isn't set so,
                # and then mixes packages from different channels that don't load together
                + [{"name": "CONDA_CHANNEL_PRIORITY", "value": "strict"}]
                # the region, which jobs can't look up on the instance (IMDS) when it's closed to them
                + [{"name": n, "value": self.region} for n in ("AWS_REGION", "AWS_DEFAULT_REGION") if self.region]
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

        size = len(json.dumps(kwargs["containerOverrides"], separators=(",", ":")))
        if size > ECS_OVERRIDES_MAX:
            raise WorkflowError(
                f"{job.name}: its command and environment come to {size} characters, more than the "
                f"{ECS_OVERRIDES_MAX} ECS allows per job. What makes them long: many storage plugins or "
                "extra packages installed at job start (bootstrap host avoids that), a long storage "
                "prefix, or very many input/output files in one job."
            )
        try:
            resp = self.batch.submit_job(**kwargs)
        except (ClientError, BotoCoreError) as e:
            hint = f"\n{SUBMIT_HINT}" if _not_allowed(e) else ""
            raise WorkflowError(f"AWS Batch submit_job failed: {e}{hint}") from e

        self.logger.info(
            f"Submitted {job.name} as AWS Batch job {resp['jobId']} "
            f"({image}, {req.vcpu} vCPU, {req.memory_mib} MiB, queue {queue}, {platform})"
        )
        self._unfinished.add(resp["jobId"])
        self.report_job_submission(
            SubmittedJobInfo(
                job,
                external_jobid=resp["jobId"],
                aux={"smkflex": Submitted(image, queue, platform, float(req.vcpu), req.memory_mib, asked)},
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
            if d is None or d["status"] in ("SUCCEEDED", "FAILED"):
                self._unfinished.discard(jid)
            if d is None:
                self.report_job_error(
                    info, msg=f"AWS Batch no longer knows job {jid} (expired?)."
                )
                continue
            status = d["status"]
            if status in ("SUCCEEDED", "FAILED"):
                await asyncio.to_thread(self._record_usage, info, d)
            if status == "SUCCEEDED":
                self.report_job_success(info)
            elif status == "FAILED":
                if _out_of_memory(d):
                    times, _asked = self._oom.get(info.job.jobid, (0, None))
                    self._oom[info.job.jobid] = (times + 1, _sub(info).asked)
                msg = await asyncio.to_thread(self._failure_message, d, info)
                if info.job.is_group():  # Snakemake's report of a failed group leaves msg out
                    self.logger.error(f"Error in group job {info.job.name}:\n    message: {msg}")
                self.report_job_error(info, msg=msg)
            elif status in ACTIVE:
                self._warn_if_stuck(d, info)
                yield info
            else:
                yield info

    def _record_usage(self, info: SubmittedJobInfo, d: dict):
        sub = _sub(info)
        self.usage.add(info.job.name, d, sub.vcpu, sub.mem, sub.platform)
        c = d.get("container") or {}
        if c.get("logStreamName") and d.get("status") == "SUCCEEDED":
            group = log_group_of(c, self.settings.log_group)
            self.usage.add_log(info.job.name, group, c["logStreamName"])

    def shutdown(self):
        super().shutdown()
        if self.settings.no_usage_summary:
            return
        self.usage.fetch_peaks(self.cwlogs)
        table = self.usage.table(self.settings.mem_overhead_mib)
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
        # In parallel: a head job that is stopped has little time (ECS_CONTAINER_STOP_TIMEOUT)
        # to cancel every job and save its state.
        def cancel(jid):
            try:
                self.batch.terminate_job(jobId=jid, reason="Cancelled by Snakemake")
            except (ClientError, BotoCoreError) as e:
                self.logger.warning(f"Could not terminate {jid}: {e}")

        # set() copies at once, so the status checks can't change it meanwhile
        ids = {i.external_jobid for i in active_jobs} | set(self._unfinished)
        with ThreadPoolExecutor(CANCEL_THREADS) as pool:
            list(pool.map(cancel, sorted(ids)))

    def _warn_if_stuck(self, d: dict, info: SubmittedJobInfo):
        sub = _sub(info)
        if d["status"] != "RUNNABLE" or sub.warned:
            return
        waited = (time.time() * 1000 - d.get("createdAt", time.time() * 1000)) / 60000
        if waited >= self.settings.runnable_warn_minutes:
            sub.warned = True
            self.logger.warning(
                f"AWS Batch job {d['jobId']} has waited {waited:.0f} min in RUNNABLE "
                f"on queue {sub.queue}. Usually the compute environment cannot "
                "provide the requested vCPU/memory (max vCPUs reached, instance types "
                "too small, or subnets without capacity). Check the Batch console."
            )

    def _failure_message(self, d: dict, info: SubmittedJobInfo) -> str:
        c = d.get("container") or {}
        lines = [
            f"AWS Batch job {d['jobId']} FAILED (image {_sub(info).image}).",
        ]
        if d.get("statusReason"):
            lines.append(f"  status reason: {d['statusReason']}")
        if c.get("reason"):
            lines.append(f"  container reason: {c['reason']}")
        if c.get("exitCode") is not None:
            lines.append(f"  exit code: {c['exitCode']}")
        if _out_of_memory(d):
            mem = _sub(info).mem
            factor = self.settings.oom_memory_factor
            retries = getattr(info.job, "restart_times", 0) or 0
            if retries >= info.job.attempt and factor > 1:
                lines.append(f"  hint: the job ran out of memory ({mem} MiB); Snakemake retries it with more.")
            else:
                lines.append(
                    f"  hint: the job ran out of memory ({mem} MiB). Raise the rule's mem_mb, or run with "
                    "--retries 2: the plugin then retries jobs that ran out of memory with twice as much."
                )
        stream = c.get("logStreamName")
        if stream:
            group = log_group_of(c, self.settings.log_group)
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
                events = []
                lines.append("  (could not fetch log tail)")
        # Tools' own errors are usually in the rule's log: files (2> {log}), which are in storage
        logged = []
        for name, tail in _log_tails(info.job):
            lines.append(f"  end of log file {name}:")
            lines += [f"    {t}" for t in tail]
            logged += tail
        lines += known_problem_hints([e["message"] for e in events] + logged)
        return "\n".join(lines) + "\n"
