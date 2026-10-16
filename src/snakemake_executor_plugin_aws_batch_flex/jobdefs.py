"""Job definitions, registered on demand: one per (image, platform), since Batch can't
override the image at submit time. The name includes a hash of the definition, so runs reuse
existing ones."""

import hashlib
import json
import re
from typing import Optional

from botocore.exceptions import ClientError
from snakemake_interface_common.exceptions import WorkflowError

DEFAULT_LOG_GROUP = "/aws/batch/job"


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
        self._cache: dict[tuple, str] = {}

    def container_properties(self, image: str, platform: Optional[str] = None) -> dict:
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
        if self.execution_role_arn and platform == "fargate":
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
            props["logConfiguration"] = {
                "logDriver": "awslogs",
                "options": {"awslogs-group": self.log_group},
            }
        if platform == "fargate":
            props["networkConfiguration"] = {"assignPublicIp": self.assign_public_ip}
            props["fargatePlatformConfiguration"] = {"platformVersion": "LATEST"}
            if self.ephemeral_storage_gib:
                props["ephemeralStorage"] = {"sizeInGiB": self.ephemeral_storage_gib}
        return props

    def ensure(self, image: str, platform: Optional[str] = None) -> str:
        """Return the job definition name for `image` on `platform`, registering it if needed."""
        platform = platform or self.platform
        if (image, platform) in self._cache:
            return self._cache[(image, platform)]
        props = self.container_properties(image, platform)
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
                if self.tags:
                    kwargs["tags"] = self.tags
                self.batch.register_job_definition(**kwargs)
        except ClientError as e:
            raise WorkflowError(f"Could not register AWS Batch job definition: {e}")
        self._cache[(image, platform)] = name
        return name
