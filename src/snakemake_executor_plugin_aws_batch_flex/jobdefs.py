"""Job definitions, registered on demand: one per (image, platform), since Batch can't
override the image at submit time. The name includes a hash of the definition, so runs reuse
existing ones."""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Optional

from botocore.exceptions import ClientError
from snakemake_interface_common.exceptions import WorkflowError

from .tags import MANAGED_BY

DEFAULT_LOG_GROUP = "/aws/batch/job"
FARGATE_PLATFORM = "1.4.0"


def log_group_of(container: dict, default: str = DEFAULT_LOG_GROUP) -> str:
    """The CloudWatch log group of a described job's container: its job definition's, or default."""
    return ((container.get("logConfiguration") or {}).get("options") or {}).get("awslogs-group") or default


def normalize_image(url: str) -> str:
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


def mutable_tag(image: str) -> bool:
    """True for an image without a version: no tag, or :latest, and no digest."""
    if "@" in image:
        return False
    tag = image.rsplit("/", 1)[-1].partition(":")[2]
    return tag in ("", "latest")


def image_registry(image: str) -> str:
    """Registry host of an image reference; Docker Hub images (no host part) give docker.io."""
    first, _, rest = image.partition("/")
    if rest and ("." in first or ":" in first or first == "localhost"):
        return "docker.io" if first in ("index.docker.io", "registry-1.docker.io") else first
    return "docker.io"


def is_ecr(registry: str) -> bool:
    return bool(re.match(r"^\d{12}\.dkr\.ecr(-fips)?\.[a-z0-9-]+\.amazonaws\.com(\.cn)?$", registry))


class JobDefinitions:
    def __init__(
        self,
        batch_client,
        logger,
        *,
        prefix: str,
        platform: str,
        job_role_arn: Optional[str],
        execution_role_arn: Optional[str],
        log_group: str,
        assign_public_ip: str,
        ephemeral_storage_gib: Optional[int],
        tags: dict,
        mounts: Optional[list] = None,
        registry_credentials: Optional[dict] = None,
    ):
        self.batch = batch_client
        self.logger = logger
        self.prefix = re.sub(r"[^A-Za-z0-9_-]", "-", prefix)[:80]
        self.platform = platform
        self.job_role_arn = job_role_arn
        self.execution_role_arn = execution_role_arn
        self.log_group = log_group
        self.assign_public_ip = assign_public_ip
        self.ephemeral_storage_gib = ephemeral_storage_gib
        self.tags = tags
        # [(host_path, container_path, read_only)] bind mounts (EC2 only)
        self.mounts = list(mounts or [])
        # registry host -> Secrets Manager secret ARN (username/password) for private images
        self.registry_credentials = dict(registry_credentials or {})
        self._cache: dict[tuple, str] = {}

    def container_properties(
        self,
        image: str,
        platform: Optional[str] = None,
        shm_mb: Optional[int] = None,
        disk_gib: Optional[int] = None,
    ) -> dict:
        platform = platform or self.platform
        # vCPU/memory here are placeholders; they are overridden on every submit.
        props: dict = {
            "image": image,
            "resourceRequirements": [
                {"type": "VCPU", "value": "1"},
                {"type": "MEMORY", "value": "2048"},
            ],
        }
        if self.job_role_arn:
            props["jobRoleArn"] = self.job_role_arn
        secret = self.registry_credentials.get(image_registry(image))
        if secret:
            props["repositoryCredentials"] = {"credentialsParameter": secret}
        # ECS reads the registry secret with the execution role, on EC2 too
        if self.execution_role_arn and (platform == "fargate" or secret):
            props["executionRoleArn"] = self.execution_role_arn
        if self.mounts and platform == "ec2":  # Fargate has no host paths
            props["volumes"] = [
                {"name": f"vol{i}", "host": {"sourcePath": host}}
                for i, (host, _c, _ro) in enumerate(self.mounts)
            ]
            props["mountPoints"] = [
                {"sourceVolume": f"vol{i}", "containerPath": cont, "readOnly": ro}
                for i, (_h, cont, ro) in enumerate(self.mounts)
            ]
        if self.log_group != DEFAULT_LOG_GROUP:
            options = {"awslogs-group": self.log_group}
            region = getattr(getattr(self.batch, "meta", None), "region_name", None)
            if region:  # Fargate requires it
                options["awslogs-region"] = region
            props["logConfiguration"] = {"logDriver": "awslogs", "options": options}
        if shm_mb and platform == "ec2":  # /dev/shm size; Fargate doesn't support it
            props["linuxParameters"] = {"sharedMemorySize": int(shm_mb)}
        if platform == "fargate":
            props["networkConfiguration"] = {"assignPublicIp": self.assign_public_ip}
            # a fixed version, not LATEST, so jobs don't change platform from one run to the next
            props["fargatePlatformConfiguration"] = {"platformVersion": FARGATE_PLATFORM}
            disk = max(disk_gib or 0, self.ephemeral_storage_gib or 0)
            if disk:
                props["ephemeralStorage"] = {"sizeInGiB": disk}
        return props

    def ensure(
        self,
        image: str,
        platform: Optional[str] = None,
        shm_mb: Optional[int] = None,
        disk_gib: Optional[int] = None,
    ) -> str:
        """Return the job definition name for `image` on `platform` (and /dev/shm size,
        Fargate disk size), registering it if needed."""
        platform = platform or self.platform
        key = (image, platform, shm_mb, disk_gib)
        if key in self._cache:
            return self._cache[key]
        props = self.container_properties(image, platform, shm_mb, disk_gib)
        digest = hashlib.sha256(
            json.dumps([props, platform], sort_keys=True).encode()
        ).hexdigest()[:12]
        name = f"{self.prefix}-{digest}"
        try:
            existing = self.batch.describe_job_definitions(
                jobDefinitionName=name, status="ACTIVE"
            )["jobDefinitions"]
            if not existing:
                self.logger.info(f"Registering AWS Batch job definition {name} for {image} ({platform})")
                kwargs = dict(
                    jobDefinitionName=name,
                    type="container",
                    containerProperties=props,
                    platformCapabilities=[platform.upper()],
                )
                # ManagedBy lets scripts/cleanup-jobdefs.sh tell the plugin's definitions apart
                kwargs["tags"] = {**self.tags, **MANAGED_BY}
                self.batch.register_job_definition(**kwargs)
        except ClientError as e:
            raise WorkflowError(f"Could not register AWS Batch job definition: {e}") from e
        self._cache[key] = name
        return name


@dataclass
class ExistingJobDefinition:
    """What a job definition made outside the plugin fixes for its jobs."""
    arn: str
    image: str
    platform: str
    container_paths: set
    job_role: Optional[str]


def describe_existing(batch, ref: str) -> ExistingJobDefinition:
    """`ref` is a name (latest ACTIVE revision), name:revision, or an ARN."""
    try:
        if ref.startswith("arn:") or ":" in ref:
            found = batch.describe_job_definitions(jobDefinitions=[ref])["jobDefinitions"]
        else:
            found, token = [], None
            while True:
                kw = dict(jobDefinitionName=ref, status="ACTIVE")
                if token:
                    kw["nextToken"] = token
                resp = batch.describe_job_definitions(**kw)
                found += resp["jobDefinitions"]
                token = resp.get("nextToken")
                if not token:
                    break
    except ClientError as e:
        raise WorkflowError(f"Could not look up AWS Batch job definition '{ref}': {e}") from e
    found = [d for d in found if d.get("status", "ACTIVE") == "ACTIVE"]
    if not found:
        raise WorkflowError(f"AWS Batch job definition '{ref}' not found (or not ACTIVE).")
    d = max(found, key=lambda d: d.get("revision", 0))
    props = d.get("containerProperties")
    if d.get("type", "container") != "container" or not props:
        raise WorkflowError(f"Job definition '{ref}' is not a single-container job definition.")
    return ExistingJobDefinition(
        arn=d["jobDefinitionArn"],
        image=props.get("image", ""),
        platform="fargate" if "FARGATE" in (d.get("platformCapabilities") or ["EC2"]) else "ec2",
        container_paths={m.get("containerPath") for m in props.get("mountPoints") or []},
        job_role=props.get("jobRoleArn"),
    )
