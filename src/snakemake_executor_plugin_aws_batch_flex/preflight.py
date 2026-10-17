"""Startup checks on the queue, its compute environments and the job role.

A missing permission to look only gives a warning. Skip with --aws-batch-flex-skip-preflight."""

from typing import Optional

from botocore.exceptions import BotoCoreError, ClientError


def _denied(e: ClientError) -> bool:
    code = e.response.get("Error", {}).get("Code", "")
    return "AccessDenied" in code or "UnauthorizedOperation" in code or code == "AccessDeniedException"


def check_queue(batch, queue: str, platform: Optional[str] = None):
    """Returns (errors, warnings, detected platform or None) for the job queue and the compute
    environments behind it. A mismatch with `platform` is only an error when `platform` is given."""
    errors, warnings, detected = [], [], None
    try:
        queues = batch.describe_job_queues(jobQueues=[queue])["jobQueues"]
    except ClientError as e:
        if _denied(e):
            return [], [f"cannot verify queue '{queue}' (no batch:DescribeJobQueues permission); continuing"], None
        return [f"AWS Batch refused the queue lookup: {e}"], [], None
    except BotoCoreError as e:
        return [], [f"cannot verify queue '{queue}': {e}; continuing"], None
    if not queues:
        hint = ""
        try:
            names = sorted(q["jobQueueName"] for q in batch.describe_job_queues()["jobQueues"])
            hint = f" Queues in this region: {', '.join(names[:10]) or 'none'}."
        except (ClientError, BotoCoreError):
            pass
        return [f"Job queue '{queue}' not found in this region (check the queue name and the region)." + hint], [], None
    q = queues[0]
    if q.get("state") != "ENABLED":
        errors.append(f"Job queue '{queue}' is {q.get('state')}, not ENABLED.")
    if q.get("status") != "VALID":
        errors.append(f"Job queue '{queue}' has status {q.get('status')}: {q.get('statusReason', 'no reason given')}")
    ces = [o["computeEnvironment"] for o in q.get("computeEnvironmentOrder", [])]
    if not ces:
        return errors + [f"Job queue '{queue}' has no compute environment attached."], warnings, None
    try:
        envs = batch.describe_compute_environments(computeEnvironments=ces)["computeEnvironments"]
    except (ClientError, BotoCoreError):
        return errors, warnings + [f"cannot inspect the compute environments of '{queue}'; continuing"], None
    for ce in envs:
        name, kind = ce.get("computeEnvironmentName"), ce.get("computeResources", {}).get("type", "")
        if ce.get("status") not in (None, "VALID"):
            errors.append(f"Compute environment '{name}' has status {ce.get('status')}: {ce.get('statusReason', '')}")
        if kind:
            actual = "fargate" if kind.startswith("FARGATE") else "ec2"
            detected = detected or actual
            if platform and actual != platform:
                errors.append(
                    f"Queue '{queue}' runs on {actual.upper()} (compute environment '{name}') but "
                    f"--aws-batch-flex-platform is '{platform}'. Set --aws-batch-flex-platform {actual} "
                    "(or leave it unset: the platform is then detected per queue)."
                )
    return errors, warnings, detected


def check_role(iam, role_arn: str, label: str):
    """Best effort: does the role exist? Only a definite 'no such role' is an error."""
    name = role_arn.rsplit("/", 1)[-1]
    try:
        iam.get_role(RoleName=name)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchEntity":
            return [f"{label} '{role_arn}' does not exist."], []
        return [], []  # cannot look (no iam:GetRole): not our business
    except BotoCoreError:
        return [], []
    return [], []


SUBMIT_HINT = (
    "The identity running snakemake needs batch:SubmitJob (on the queue and the snakemake-* job definitions), "
    "batch:RegisterJobDefinition, batch:DescribeJobDefinitions, batch:DescribeJobs, batch:ListJobs, batch:TerminateJob, "
    "batch:TagResource (jobs are tagged), logs:GetLogEvents, and iam:PassRole on the job role."
)
