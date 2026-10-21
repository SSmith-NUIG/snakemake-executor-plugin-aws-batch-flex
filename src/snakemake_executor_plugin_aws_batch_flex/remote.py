"""smkflex: run a whole workflow on AWS Batch, so nothing has to stay running on your machine.

`smkflex run` packs the workflow folder and the profile, uploads them next to the workflow's
files in storage, and submits one "head job" that runs `snakemake` with this plugin. Snakemake's
.snakemake state (what was made with which code, unfinished jobs) is saved to storage at the end
of each run and restored at the start of the next, so reruns behave as they do locally.

Optional: `snakemake --executor aws-batch-flex` from your own machine works as before."""

import argparse
import hashlib
import io
import os
import re
import shlex
import subprocess
import sys
import tarfile
import time
from datetime import datetime, timezone

import boto3
import yaml
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from . import headjob
from .bootstrap import build_host_prelude, build_prelude, conda_snippet
from .hostenv import DEFAULT_PATH as DEFAULT_HOST_ENV
from .jobdefs import DEFAULT_LOG_GROUP, JobDefinitions
from .naming import batch_job_name
from .preflight import check_queue
from .sizing import fargate_size

ACTIVE = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING")
SKIP_DIRS = {".snakemake", ".smkflex", ".git", "__pycache__"}

# Runs in the head job before anything else: fetch and unpack the bundle. Kept small, since
# everything sent with a job must fit in 8192 characters.
_FETCH = (
    "import io,sys,tarfile\n"
    "u,d=sys.argv[1],sys.argv[2]\n"
    "if u.startswith('s3://'):\n"
    " import boto3;b,_,k=u[5:].partition('/')\n"
    " data=boto3.client('s3').get_object(Bucket=b,Key=k)['Body'].read()\n"
    "else:\n"
    " data=open(u,'rb').read()\n"
    "t=tarfile.open(fileobj=io.BytesIO(data))\n"
    "t.extractall(d,**({'filter':'data'} if hasattr(tarfile,'data_filter') else {}))\n"
)


class Fail(Exception):
    pass


class _Log:
    def info(self, msg):
        print(msg, file=sys.stderr)

    warning = info


def _load_profile(path):
    if path is None:
        path = "profile" if os.path.isfile(os.path.join("profile", "config.yaml")) else os.environ.get("SNAKEMAKE_PROFILE")
    if not path:
        raise Fail("no profile: pass --profile DIR (the folder with config.yaml)")
    cfg = os.path.join(path, "config.yaml")
    if not os.path.isfile(cfg):
        raise Fail(f"{cfg} not found")
    with open(cfg) as f:
        return path, yaml.safe_load(f) or {}


def _flag(value):
    return str(value).lower() in ("true", "1", "yes")


def _clients(profile):
    cfg = BotoConfig(region_name=profile.get("aws-batch-flex-region"),
                     retries={"max_attempts": 10, "mode": "adaptive"})
    return boto3.client("batch", config=cfg), boto3.client("logs", config=cfg), cfg.region_name


def workflow_files(root):
    """Files to send: what git would track (tracked plus untracked, not ignored), or
    everything outside .snakemake/.git when the folder isn't a git checkout."""
    try:
        out = subprocess.run(["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=root,
                             capture_output=True, check=True).stdout.decode()
        files = [f for f in out.split("\0") if f]
    except (OSError, subprocess.CalledProcessError):
        files = []
        for d, dirs, names in os.walk(root):
            dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
            files += [os.path.relpath(os.path.join(d, n), root) for n in names]
    return sorted(f for f in files
                  if not SKIP_DIRS.intersection(f.split("/")) and os.path.isfile(os.path.join(root, f)))


def make_bundle(root, profile_dir, max_mb):
    buf = io.BytesIO()
    total = 0
    files = workflow_files(root)
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
            if total > max_mb * 1024 * 1024:
                raise Fail(f"the workflow folder has more than {max_mb} MB of files (at {f}); data "
                           "belongs in storage. Add big files to .gitignore, or raise --max-bundle-mb.")
            t.add(os.path.join(root, f), arcname=f)
        # what `git ls-files` would say, for the head job's stand-in git
        listing = "".join(f + "\n" for f in files).encode()
        info = tarfile.TarInfo(".smkflex/files.txt")
        info.size = len(listing)
        t.addfile(info, io.BytesIO(listing))
        for d, dirs, names in os.walk(profile_dir):
            dirs[:] = [x for x in dirs if x != "__pycache__"]
            for n in names:
                p = os.path.join(d, n)
                t.add(p, arcname=os.path.join(".smkflex", "profile", os.path.relpath(p, profile_dir)))
        # this plugin as installed here, so the head job runs the same version
        pkg = os.path.dirname(os.path.abspath(__file__))
        for n in sorted(os.listdir(pkg)):
            if n.endswith(".py"):
                t.add(os.path.join(pkg, n), arcname=f".smkflex/site/{os.path.basename(pkg)}/{n}")
        t.add(headjob.__file__, arcname=".smkflex/head.py")
    return buf.getvalue()


def _active_heads(batch, queue, state_key):
    ids = []
    for status in ACTIVE:
        token = None
        while True:
            kw = dict(jobQueue=queue, jobStatus=status, maxResults=1000)
            if token:
                kw["nextToken"] = token
            resp = batch.list_jobs(**kw)
            ids += [j["jobId"] for j in resp.get("jobSummaryList", [])]
            token = resp.get("nextToken")
            if not token:
                break
    found = []
    for i in range(0, len(ids), 100):
        for d in batch.describe_jobs(jobs=ids[i:i + 100])["jobs"]:
            if (d.get("tags") or {}).get("smkflex-state") == state_key:
                found.append(d["jobId"])
    return found


def head_command(profile, platform, bundle_url, state_url, snakemake_args):
    mode = profile.get("aws-batch-flex-bootstrap", "auto")
    if mode == "host" and platform == "fargate":
        mode = "auto"
    if mode == "host":
        prelude = build_host_prelude(profile.get("aws-batch-flex-host-env-path", DEFAULT_HOST_ENV),
                                     int(profile.get("aws-batch-flex-host-env-wait-seconds", 120)))
    else:
        import importlib.metadata as md
        from . import storage_requirements

        default = profile.get("default-storage-provider")
        reqs = [f"snakemake=={md.version('snakemake')}"] + storage_requirements(default)
        reqs += list(profile.get("aws-batch-flex-extra-packages") or [])
        pyver = str(profile.get("aws-batch-flex-python-version") or f"{sys.version_info[0]}.{sys.version_info[1]}")
        prelude = build_prelude(mode, md.version("snakemake"), reqs, pyver,
                                _flag(profile.get("aws-batch-flex-install-prereqs", False)))
    conda = profile.get("aws-batch-flex-conda-path")
    if conda and platform == "ec2":
        prelude += "\n" + conda_snippet(conda)
    args = " ".join(shlex.quote(a) for a in snakemake_args)
    return (
        f"{prelude}\n"
        'SMKFLEX_DIR="${TMPDIR:-/tmp}/smkflex-workflow"; rm -rf "$SMKFLEX_DIR"; mkdir -p "$SMKFLEX_DIR"\n'
        'SMKFLEX_PY="${SMK_PY:-python}"\n'
        f'"$SMKFLEX_PY" -c {shlex.quote(_FETCH)} {shlex.quote(bundle_url)} "$SMKFLEX_DIR"\n'
        'cd "$SMKFLEX_DIR"\n'
        f'exec "$SMKFLEX_PY" .smkflex/head.py {shlex.quote(state_url)} {args}\n'
    )


def cmd_run(a, snakemake_args):
    profile_dir, profile = _load_profile(a.profile)
    queue = a.queue or profile.get("aws-batch-flex-queue")
    prefix = str(profile.get("default-storage-prefix") or "").rstrip("/")
    role = a.role or profile.get("aws-batch-flex-head-role-arn")
    if not queue:
        raise Fail("no queue: set aws-batch-flex-queue in the profile or pass --queue")
    if not prefix or profile.get("default-storage-provider") in (None, "local"):
        raise Fail("smkflex needs remote storage (default-storage-provider and default-storage-prefix "
                   "in the profile): the head job's disk is gone when it ends")
    if not role:
        raise Fail("no role for the head job: set aws-batch-flex-head-role-arn in the profile (Terraform "
                   "prints it as head_role_arn) or pass --role")
    name = re.sub(r"[^A-Za-z0-9_-]", "-", a.name or os.path.basename(os.path.abspath(".")))
    state_url = f"{prefix}/.smkflex/{name}/state.tar.gz"
    state_key = hashlib.sha256(state_url.encode()).hexdigest()[:16]
    batch, _logs, region = _clients(profile)

    errors, warnings, detected = check_queue(batch, queue, profile.get("aws-batch-flex-platform"))
    for w in warnings:
        print(f"warning: {w}", file=sys.stderr)
    if errors:
        raise Fail("; ".join(errors))
    platform = profile.get("aws-batch-flex-platform") or detected or "ec2"
    if not a.force:
        running = _active_heads(batch, queue, state_key)
        if running:
            raise Fail(f"workflow '{name}' is already running as head job {running[0]}; wait for it, "
                       f"`smkflex cancel {running[0]}`, or pass --force")

    bundle = make_bundle(".", profile_dir, a.max_bundle_mb)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bundle_url = f"{prefix}/.smkflex/{name}/runs/{stamp}/workflow.tar.gz"
    headjob.put(bundle_url, bundle)
    print(f"Uploaded the workflow ({len(bundle) // 1024} KiB) to {bundle_url}", file=sys.stderr)

    mounts = []
    if profile.get("aws-batch-flex-bootstrap") == "host" and platform == "ec2":
        env = profile.get("aws-batch-flex-host-env-path", DEFAULT_HOST_ENV)
        mounts.append((env, env, True))
    if profile.get("aws-batch-flex-conda-path") and platform == "ec2":
        conda = profile["aws-batch-flex-conda-path"]
        mounts.append((conda, conda, True))
    jobdefs = JobDefinitions(
        batch, _Log(),
        prefix=profile.get("aws-batch-flex-job-definition-prefix", "snakemake"),
        platform=platform,
        job_role_arn=role,
        execution_role_arn=profile.get("aws-batch-flex-execution-role-arn"),
        log_group=profile.get("aws-batch-flex-log-group", DEFAULT_LOG_GROUP),
        assign_public_ip=profile.get("aws-batch-flex-assign-public-ip", "ENABLED"),
        ephemeral_storage_gib=None,
        tags={},
        mounts=mounts,
    )
    image = a.image or profile.get("aws-batch-flex-default-image") or "debian:12"
    jobdef = jobdefs.ensure(image, platform)
    vcpu, mem = str(a.vcpus), a.mem_mb
    if platform == "fargate":
        vcpu, mem = fargate_size(a.vcpus, a.mem_mb)
    env = [{"name": "AWS_REGION", "value": region}, {"name": "AWS_DEFAULT_REGION", "value": region}] if region else []
    resp = batch.submit_job(
        jobName=batch_job_name(f"smkflex-{name}"),
        jobQueue=queue,
        jobDefinition=jobdef,
        containerOverrides={
            "command": ["/bin/sh", "-c", head_command(profile, platform, bundle_url, state_url, snakemake_args)],
            "resourceRequirements": [{"type": "VCPU", "value": vcpu}, {"type": "MEMORY", "value": str(mem)}],
            "environment": env,
        },
        tags={"smkflex-workflow": name[:256], "smkflex-state": state_key},
        propagateTags=True,
    )
    jid = resp["jobId"]
    print(f"Submitted workflow '{name}' as head job {jid} (queue {queue}, {platform}).")
    if a.wait:
        return follow(batch, _logs, jid, follow_logs=True)
    print(f"  follow it:  smkflex logs {jid} --follow\n  cancel it:  smkflex cancel {jid}")
    return 0


def follow(batch, logs, jid, follow_logs):
    """Print the head job's log (until it ends, with follow_logs). Returns 0 if it succeeded."""
    token, last_status, group, stream = None, None, None, None
    while True:
        jobs = batch.describe_jobs(jobs=[jid])["jobs"]
        if not jobs:
            raise Fail(f"no job {jid}")
        d = jobs[0]
        status = d["status"]
        if status != last_status and (follow_logs or status in ("SUCCEEDED", "FAILED")):
            print(f"[smkflex] head job {jid}: {status}", file=sys.stderr)
            last_status = status
        c = d.get("container", {})
        stream = c.get("logStreamName") or stream
        group = (c.get("logConfiguration", {}).get("options", {}).get("awslogs-group") or DEFAULT_LOG_GROUP)
        got = 0
        if stream:
            while True:
                kw = dict(logGroupName=group, logStreamName=stream, startFromHead=True)
                if token:
                    kw["nextToken"] = token
                try:
                    resp = logs.get_log_events(**kw)
                except ClientError as e:
                    if "ResourceNotFound" in str(e):
                        break
                    raise
                for e in resp.get("events", []):
                    print(e["message"].rstrip())
                got += len(resp.get("events", []))
                # CloudWatch can return empty pages before the end; the end is when the
                # token stops changing
                new = resp.get("nextForwardToken")
                if not new or new == token:
                    break
                token = new
        if status in ("SUCCEEDED", "FAILED"):
            if status == "FAILED" and d.get("statusReason"):
                print(f"[smkflex] {d['statusReason']}", file=sys.stderr)
            return 0 if status == "SUCCEEDED" else 1
        if not follow_logs:
            if not stream:
                print(f"[smkflex] head job {jid} is {status}; no log yet", file=sys.stderr)
            return 0
        if not got:
            time.sleep(5)


def cmd_logs(a):
    _dir, profile = _load_profile(a.profile) if (a.profile or os.path.isdir("profile")) else (None, {})
    if a.region:
        profile["aws-batch-flex-region"] = a.region
    batch, logs, _r = _clients(profile)
    return follow(batch, logs, a.job_id, a.follow)


def cmd_cancel(a):
    _dir, profile = _load_profile(a.profile) if (a.profile or os.path.isdir("profile")) else (None, {})
    if a.region:
        profile["aws-batch-flex-region"] = a.region
    batch, _logs, _r = _clients(profile)
    batch.terminate_job(jobId=a.job_id, reason="smkflex cancel")
    print(f"Asked head job {a.job_id} to stop; Snakemake cancels the workflow's jobs and saves its state.")
    return 0


def cmd_status(a):
    _dir, profile = _load_profile(a.profile)
    batch, _logs, _r = _clients(profile)
    queue = a.queue or profile.get("aws-batch-flex-queue")
    resp = batch.list_jobs(jobQueue=queue, filters=[{"name": "JOB_NAME", "values": ["smkflex-*"]}])
    rows = sorted(resp.get("jobSummaryList", []), key=lambda j: j.get("createdAt", 0), reverse=True)[:a.limit]
    if not rows:
        print(f"no smkflex runs in queue {queue}")
    for j in rows:
        when = datetime.fromtimestamp(j.get("createdAt", 0) / 1000).strftime("%Y-%m-%d %H:%M")
        print(f"{j['jobId']}  {j.get('status', ''):<9}  {when}  {j.get('jobName', '')[len('smkflex-'):]}")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="smkflex", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run the workflow in this folder on AWS Batch; other arguments "
                       "(e.g. targets, --forceall) go to snakemake")
    r.add_argument("--profile", help="profile folder (default: ./profile, or $SNAKEMAKE_PROFILE)")
    r.add_argument("--name", help="workflow name; runs with the same name share state (default: folder name)")
    r.add_argument("--queue", help="queue for the head job (default: the profile's)")
    r.add_argument("--role", help="IAM role for the head job (default: aws-batch-flex-head-role-arn)")
    r.add_argument("--image", help="image for the head job (default: the profile's default image)")
    r.add_argument("--vcpus", type=int, default=1)
    r.add_argument("--mem-mb", type=int, default=2048)
    r.add_argument("--max-bundle-mb", type=int, default=100)
    r.add_argument("--wait", action="store_true", help="stay attached and print the log until the run ends")
    r.add_argument("--force", action="store_true", help="submit even if this workflow is already running")
    for name, text in (("logs", "print a run's Snakemake log"), ("cancel", "stop a run and its jobs")):
        s = sub.add_parser(name, help=text)
        s.add_argument("job_id")
        s.add_argument("--profile")
        s.add_argument("--region")
        if name == "logs":
            s.add_argument("--follow", "-f", action="store_true")
    s = sub.add_parser("status", help="list recent runs")
    s.add_argument("--profile")
    s.add_argument("--queue")
    s.add_argument("--limit", type=int, default=20)

    a, extra = p.parse_known_args(argv)
    if extra and a.cmd != "run":
        p.error(f"unrecognized arguments: {' '.join(extra)}")
    if extra[:1] == ["--"]:
        extra = extra[1:]
    try:
        if a.cmd == "run":
            return cmd_run(a, extra)
        return {"logs": cmd_logs, "cancel": cmd_cancel, "status": cmd_status}[a.cmd](a)
    except Fail as e:
        print(f"smkflex: {e}", file=sys.stderr)
        return 2
    except (ClientError, BotoCoreError) as e:
        print(f"smkflex: AWS error: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
