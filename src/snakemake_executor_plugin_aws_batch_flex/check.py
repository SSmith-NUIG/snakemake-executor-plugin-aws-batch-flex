"""smkflex check: the setup, checked from this machine, one line per check."""

import os

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from . import headjob
from .preflight import ami_snakemake_version, check_queue, check_role, queue_uses_spot, version_note
from .profiles import Fail, _load_profile


def cmd_check(a):
    """Check the setup from this machine, one line per check, with what to do about failures."""
    results = []

    def report(level, what, fix=""):
        results.append(level)
        mark = {"ok": "  ok  ", "warn": " warn ", "fail": " FAIL "}[level]
        print(f"[{mark}] {what}" + (f"\n         -> {fix}" if fix else ""))

    try:
        profile_dir, profile = _load_profile(a.profile)
    except Fail as e:
        report("fail", f"profile: {e}", "run this in your workflow folder, or pass --profile DIR")
        return 1
    report("ok", f"profile {profile_dir}")
    if profile.get("executor") != "aws-batch-flex":
        report("warn", f"the profile's executor is {profile.get('executor')!r}, not aws-batch-flex",
               "add `executor: aws-batch-flex`, or pass --executor aws-batch-flex to snakemake")

    import importlib.metadata as md
    try:
        local = md.version("snakemake")
        report("ok", f"Snakemake {local} installed here")
    except md.PackageNotFoundError:
        local = None
        report("fail", "Snakemake isn't installed here", "pip install snakemake")
    provider = profile.get("default-storage-provider")
    prefix = str(profile.get("default-storage-prefix") or "").rstrip("/")
    if not provider or provider == "local" or not prefix:
        report("fail", "no remote storage in the profile",
               "set default-storage-provider: s3 and default-storage-prefix: s3://BUCKET/PREFIX")
    else:
        try:
            md.version(f"snakemake-storage-plugin-{provider}")
            report("ok", f"storage plugin {provider} installed here")
        except md.PackageNotFoundError:
            report("fail", f"snakemake-storage-plugin-{provider} isn't installed here",
                   f"pip install snakemake-storage-plugin-{provider}")

    region = profile.get("aws-batch-flex-region")
    cfg = BotoConfig(region_name=region, retries={"max_attempts": 3})
    try:
        who = boto3.client("sts", config=cfg).get_caller_identity()["Arn"]
        report("ok", f"AWS login: {who}")
    except (ClientError, BotoCoreError) as e:
        report("fail", f"AWS login: {e}", "run `aws configure` (or `aws sso login`), or set AWS_PROFILE")
        return 1
    batch = boto3.client("batch", config=cfg)
    region = batch.meta.region_name if hasattr(batch, "meta") else region
    if not region:
        report("fail", "no AWS region", "set aws-batch-flex-region in the profile")
        return 1

    queue = profile.get("aws-batch-flex-queue")
    platform, queue_ok = None, False
    if not queue:
        report("fail", "no queue in the profile", "set aws-batch-flex-queue (Terraform prints it as queue_name)")
    else:
        errors, warnings, detected = check_queue(batch, queue, profile.get("aws-batch-flex-platform"))
        platform = profile.get("aws-batch-flex-platform") or detected or "ec2"
        for e in errors:
            report("fail", f"queue: {e}")
        for w in warnings:
            report("warn", f"queue: {w}")
        queue_ok = not errors
        if not errors:
            spot = " (Spot)" if queue_uses_spot(batch, queue) else ""
            report("ok", f"queue {queue} in {region}: enabled, {platform.upper()}{spot}")

    iam = boto3.client("iam", config=cfg)
    for key, label in (("aws-batch-flex-job-role-arn", "job role"), ("aws-batch-flex-head-role-arn", "head role"),
                       ("aws-batch-flex-execution-role-arn", "execution role")):
        arn = profile.get(key)
        if arn:
            errors, _w = check_role(iam, arn, label.capitalize())
            if errors:
                report("fail", errors[0], f"check {key} in the profile (Terraform prints the right one)")
            else:
                report("ok", f"{label} {arn.rsplit('/', 1)[-1]}")
    if not profile.get("aws-batch-flex-job-role-arn") and platform == "fargate":
        report("fail", "Fargate jobs need a job role (there's no instance role to reach storage with)",
               "set aws-batch-flex-job-role-arn")
    if platform == "fargate" and not profile.get("aws-batch-flex-execution-role-arn"):
        report("fail", "Fargate needs an execution role", "set aws-batch-flex-execution-role-arn")
    if not profile.get("aws-batch-flex-head-role-arn"):
        report("warn", "no aws-batch-flex-head-role-arn: fine for snakemake, but smkflex run needs it",
               "add the head_role_arn that Terraform prints")

    if prefix and provider and provider != "local":
        probe = f"{prefix}/.smkflex/check-{os.getpid()}"
        try:
            headjob.put(probe, b"smkflex check")
            ok = headjob.get(probe) == b"smkflex check"
            if probe.startswith("s3://"):
                s3, bucket, key = headjob._s3(probe)
                s3.delete_object(Bucket=bucket, Key=key)
            else:
                os.remove(probe)
            report("ok" if ok else "fail", f"storage {prefix}: write, read, delete")
        except (ClientError, BotoCoreError, OSError) as e:
            report("fail", f"storage {prefix}: {e}",
                   "check the bucket name and that your AWS login may read and write under the prefix")

    if queue_ok and platform == "ec2" and profile.get("aws-batch-flex-bootstrap") == "host" and local:
        ami = ami_snakemake_version(batch, boto3.client("ec2", config=cfg), queue)
        level, note = version_note(ami, local)
        if not ami:
            report("warn", "couldn't find the AMI's Snakemake version (an older AMI, or no ec2:Describe* permission)")
        elif level == "warning":
            report("warn", note, f"pip install snakemake=={ami}, or bake a new AMI")
        else:
            report("ok", f"the AMI has Snakemake {ami}" + (f" (here: {local}; known to work together)" if level else ""))

    fails, warns = results.count("fail"), results.count("warn")
    print(f"\n{fails} problem(s), {warns} warning(s)." if fails or warns else "\nAll good.")
    return 1 if fails else 0
