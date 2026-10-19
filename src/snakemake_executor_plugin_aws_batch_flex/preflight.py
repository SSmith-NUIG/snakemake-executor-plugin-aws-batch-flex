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


def ami_snakemake_version(batch, ec2, queue: str) -> Optional[str]:
    """The SnakemakeVersion tag (set by bake-ami.sh) of the AMI the queue's instances start from.
    None when it can't be found out: no tag (older AMIs), no permission, or several AMIs."""
    try:
        queues = batch.describe_job_queues(jobQueues=[queue])["jobQueues"]
        ces = [o["computeEnvironment"] for o in (queues[0].get("computeEnvironmentOrder", []) if queues else [])]
        if not ces:
            return None
        images = set()
        for ce in batch.describe_compute_environments(computeEnvironments=ces)["computeEnvironments"]:
            cr = ce.get("computeResources") or {}
            # Batch uses an image override first, then imageId, then the launch template's image
            found = {c.get("imageIdOverride") for c in cr.get("ec2Configuration") or []} - {None}
            if not found and cr.get("imageId"):
                found = {cr["imageId"]}
            lt = cr.get("launchTemplate") or {}
            if not found and (lt.get("launchTemplateId") or lt.get("launchTemplateName")):
                which = ({"LaunchTemplateId": lt["launchTemplateId"]} if lt.get("launchTemplateId")
                         else {"LaunchTemplateName": lt["launchTemplateName"]})
                for v in ec2.describe_launch_template_versions(
                        Versions=[lt.get("version") or "$Default"], **which)["LaunchTemplateVersions"]:
                    found |= {v.get("LaunchTemplateData", {}).get("ImageId")} - {None}
            images |= found
        if not images:
            return None
        versions = {t["Value"] for img in ec2.describe_images(ImageIds=sorted(images))["Images"]
                    for t in img.get("Tags", []) if t["Key"] == "SnakemakeVersion"}
        return versions.pop() if len(versions) == 1 else None
    except (ClientError, BotoCoreError, KeyError, IndexError):
        return None


def _version_tuple(v: str):
    try:
        return tuple(int(x) for x in v.split(".")[:3])
    except ValueError:
        return None


def version_note(ami: Optional[str], local: str):
    """(level, message) comparing the AMI's Snakemake with this machine's, or (None, None)."""
    if not ami or ami == "unknown" or ami == local:
        return None, None
    a, l = _version_tuple(ami), _version_tuple(local)
    msg = f"the AMI has Snakemake {ami}, this machine has {local}."
    if a and l and a[0] == l[0] and min(a, l) >= (9, 5):
        return "info", "Note: " + msg + " Versions from 9.5 on are known to work together."
    return "warning", ("Snakemake version gap: " + msg + " Jobs may fail with 'unrecognized "
                       "arguments'; install the AMI's version here or bake a new AMI.")
