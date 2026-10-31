"""smkflex: run a whole workflow on AWS Batch, so nothing has to stay running on your machine.

`smkflex run` packs the workflow folder and the profile, uploads them next to the workflow's
files in storage, and submits one "head job" that runs `snakemake` with this plugin. Snakemake's
.snakemake state (what was made with which code, unfinished jobs) is saved to storage at the end
of each run and restored at the start of the next, so reruns behave as they do locally.

Optional: `snakemake --executor aws-batch-flex` from your own machine works as before."""

import argparse
import hashlib
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone

import yaml
from botocore.exceptions import BotoCoreError, ClientError
from snakemake_interface_common.exceptions import WorkflowError

from . import headjob
from .bundle import _tag, added_files, clone, config_named_files, head_command, local_commit, make_bundle
from .check import cmd_check
from .imageinfo import Entrypoints
from .jobdefs import JobDefinitions, log_group_of
from .naming import batch_job_name
from .orphans import active_jobs, describe_jobs, list_jobs
from .preflight import check_queue, queue_uses_spot
from .profiles import Fail, _clients, _load_profile, _Log, workflow_profile_file
from .settings import from_profile, host_mounts, registry_secrets, secret_refs
from .sizing import fargate_size
from .tags import MANAGED_BY
from .transfer import cmd_pull, cmd_push

HEAD_IMAGE = "buildpack-deps:bookworm-scm"


def _active_heads(batch, queue, state_key):
    return [d["jobId"] for d in active_jobs(batch, queue) if (d.get("tags") or {}).get("smkflex-state") == state_key]

def _check_snakemake_args(args):
    """Fail on options Snakemake doesn't know (a typo in an smkflex option such as --wiat
    ends up here too), before anything is uploaded or submitted."""
    try:
        from snakemake.cli import get_argument_parser
    except ImportError:
        return
    try:
        _ns, unknown = get_argument_parser().parse_known_args(args)
    except SystemExit:
        raise Fail("snakemake can't use these arguments (see above)") from None
    unknown = [u for u in unknown if u.startswith("-")]
    if unknown:
        raise Fail(f"neither smkflex run nor snakemake has the option(s) {' '.join(unknown)} "
                   "(see smkflex run --help and snakemake --help)")

def cmd_run(a, snakemake_args):
    _check_snakemake_args(snakemake_args)
    tmp = None
    try:
        if a.git:
            tmp = tempfile.mkdtemp(prefix="smkflex-git-")
            commit = clone(a.git, a.rev, os.path.join(tmp, "repo"))
            root = os.path.join(tmp, "repo", a.path or "")
            if not os.path.isdir(root):
                raise Fail(f"{a.path} isn't a folder in {a.git}")
            print(f"Cloned {a.git} at {commit[:12]}" + (f" ({a.rev})" if a.rev else ""), file=sys.stderr)
            extra = added_files(a.add or [])
            source = {"smkflex-source": _tag(a.git + (f" {a.rev}" if a.rev else "")),
                      "smkflex-commit": commit[:12] + ("+added" if extra else "")}
            default_name = re.sub(r"\.git$", "", a.git.rstrip("/").rsplit("/", 1)[-1])
        else:
            if a.rev or a.path:
                raise Fail("--rev and --path go with --git")
            root = "."
            extra = added_files(a.add or [])
            commit = local_commit(root)
            source = {"smkflex-commit": commit + ("+added" if extra else "")} if commit else {}
            default_name = os.path.basename(os.path.abspath("."))
        if extra:
            print(f"Adding {len(extra)} local file(s): " + ", ".join(sorted(extra)[:5])
                  + (" ..." if len(extra) > 5 else ""), file=sys.stderr)
        return _run(a, snakemake_args, root, source, default_name, extra)
    finally:
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)

def _run(a, snakemake_args, root, source, default_name, extra=None):
    profile_dir, profile = _load_profile(a.profile)
    s = from_profile(profile)
    queue = a.queue or s.queue
    prefix = str(profile.get("default-storage-prefix") or "").rstrip("/")
    role = a.role or s.head_role_arn
    if not queue:
        raise Fail("no queue: set aws-batch-flex-queue in the profile or pass --queue")
    if not prefix or profile.get("default-storage-provider") in (None, "local"):
        raise Fail("smkflex needs remote storage (default-storage-provider and default-storage-prefix "
                   "in the profile): the head job's disk is gone when it ends")
    if not role:
        raise Fail("no role for the head job: set aws-batch-flex-head-role-arn in the profile (Terraform "
                   "prints it as head_role_arn) or pass --role")
    name = re.sub(r"[^A-Za-z0-9_-]", "-", a.name or default_name)
    # One project (storage prefix) has one state and one run at a time, as Snakemake has one
    # working directory with one lock: two runs would make and overwrite the same outputs.
    state_url = f"{prefix}/.smkflex/state.tar.gz"
    state_key = hashlib.sha256(state_url.encode()).hexdigest()[:16]
    batch, _logs, region = _clients(profile)

    errors, warnings, detected = check_queue(batch, queue, s.platform)
    for w in warnings:
        print(f"warning: {w}", file=sys.stderr)
    if errors:
        raise Fail("; ".join(errors))
    platform = s.platform or detected or "ec2"
    if queue_uses_spot(batch, queue):
        print(f"warning: queue {queue} uses Spot instances; if AWS takes the head job's instance back, "
              "the whole run stops. Use an on-demand queue for it (--queue).", file=sys.stderr)
    running = _active_heads(batch, queue, state_key)
    if running:
        raise Fail(f"a run of this project ({prefix}) is still going: head job {running[0]}. Wait for "
                   f"it or `smkflex cancel {running[0]}` (finished outputs are kept), then run again. "
                   "Another set of samples is another project: `smkflex init` in its own folder.")

    bundle = make_bundle(root, profile_dir, a.max_bundle_mb,
                         {**config_named_files(root, snakemake_args), **(extra or {})})
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bundle_url = f"{prefix}/.smkflex/runs/{stamp}/workflow.tar.gz"
    headjob.put(bundle_url, bundle)
    print(f"Uploaded the workflow ({len(bundle) // 1024} KiB) to {bundle_url}", file=sys.stderr)

    jobdefs = JobDefinitions(
        batch, _Log(),
        prefix=s.job_definition_prefix,
        platform=platform,
        job_role_arn=role,
        execution_role_arn=s.execution_role_arn,
        log_group=s.log_group,
        assign_public_ip=s.assign_public_ip,
        ephemeral_storage_gib=None,
        tags={},
        mounts=host_mounts(s) if platform == "ec2" else [],
        # Snakemake in the head job sees them too (a Snakefile's envvars: checks for them there)
        secrets=secret_refs(s),
    )
    # Debian 12 with git: Snakemake needs git for pipelines loaded from GitHub (snakedeploy's)
    image = a.image or HEAD_IMAGE
    jobdef = jobdefs.ensure(image, platform)
    vcpu, mem = str(a.vcpus), a.mem_mb
    if platform == "fargate":
        vcpu, mem = fargate_size(a.vcpus, a.mem_mb)
    env = [{"name": "AWS_REGION", "value": region}, {"name": "AWS_DEFAULT_REGION", "value": region}] if region else []
    env.append({"name": "SMKFLEX_BUNDLE_URL", "value": bundle_url})
    resp = batch.submit_job(
        jobName=batch_job_name(f"smkflex-{name}"),
        jobQueue=queue,
        jobDefinition=jobdef,
        containerOverrides={
            "command": Entrypoints(registry_secrets(s), _Log()).command(
                image, head_command(profile, platform, bundle_url, state_url, snakemake_args,
                                    workdir=None if a.git else os.path.abspath(root))),
            "resourceRequirements": [{"type": "VCPU", "value": vcpu}, {"type": "MEMORY", "value": str(mem)}],
            "environment": env,
        },
        tags={"smkflex-workflow": name[:256], "smkflex-state": state_key, **source, **MANAGED_BY},
        propagateTags=True,
    )
    jid = resp["jobId"]
    at = f" at {source['smkflex-commit']}" if source.get("smkflex-commit") else ""
    print(f"Submitted workflow '{name}'{at} as head job {jid} (queue {queue}, {platform}).")
    if a.wait:
        return follow(batch, _logs, jid, follow_logs=True)
    print(f"  follow it:  smkflex logs {jid} --follow\n  cancel it:  smkflex cancel {jid}")
    return 0

def follow(batch, logs, jid, follow_logs):
    """Print the head job's log (until it ends, with follow_logs). Returns 0 if it succeeded."""
    token, last_status, group, stream = None, None, None, None
    ended = False
    while True:
        jobs = batch.describe_jobs(jobs=[jid])["jobs"]
        if not jobs:
            raise Fail(f"no job {jid}")
        d = jobs[0]
        status = d["status"]
        done = status in ("SUCCEEDED", "FAILED")
        if status != last_status and follow_logs and not done:
            print(f"[smkflex] head job {jid}: {status}", file=sys.stderr)
            last_status = status
        c = d.get("container", {})
        stream = c.get("logStreamName") or stream
        group = log_group_of(c)
        got = 0
        if stream:
            while True:
                kw = dict(logGroupName=group, logStreamName=stream, startFromHead=True)
                if token:
                    kw["nextToken"] = token
                try:
                    resp = logs.get_log_events(**kw)
                except ClientError as e:
                    if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                        break  # no log stream yet
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
        if done and not ended and stream:
            # the last lines can reach CloudWatch a few seconds after the job ends
            ended = True
            time.sleep(3)
            continue
        if done:
            print(f"[smkflex] head job {jid}: {status}", file=sys.stderr)
            if status == "FAILED" and d.get("statusReason"):
                print(f"[smkflex] {d['statusReason']}", file=sys.stderr)
            return 0 if status == "SUCCEEDED" else 1
        if not follow_logs:
            if not stream:
                print(f"[smkflex] head job {jid} is {status}; no log yet", file=sys.stderr)
            return 0
        if not got:
            time.sleep(5)

def _job_clients(a):
    """Clients for commands that only need a job ID: the profile (as the other commands find
    it) for the region, else the AWS config's; --region overrides both."""
    try:
        _dir, profile = _load_profile(a.profile)
    except Fail:
        if a.profile:
            raise
        profile = {}
    if a.region:
        profile["aws-batch-flex-region"] = a.region
    return _clients(profile)

def cmd_logs(a):
    batch, logs, _r = _job_clients(a)
    return follow(batch, logs, a.job_id, a.follow)

def cmd_cancel(a):
    batch, _logs, _r = _job_clients(a)
    jobs = batch.describe_jobs(jobs=[a.job_id])["jobs"]
    if not jobs:
        raise Fail(f"no job {a.job_id}")
    status = jobs[0]["status"]
    if status in ("SUCCEEDED", "FAILED"):
        print(f"Head job {a.job_id} has already ended ({status}).")
        return 0
    batch.terminate_job(jobId=a.job_id, reason="smkflex cancel")
    if status == "RUNNING":
        print(f"Asked head job {a.job_id} to stop; Snakemake cancels the workflow's jobs and saves "
              "its state (about a minute).")
    else:
        print(f"Cancelled head job {a.job_id} before it started ({status}); no jobs were submitted.")
    return 0

def cmd_status(a):
    _dir, profile = _load_profile(a.profile)
    batch, _logs, _r = _clients(profile)
    queue = a.queue or profile.get("aws-batch-flex-queue")
    if not queue:
        raise Fail("no queue: set aws-batch-flex-queue in the profile or pass --queue")
    rows = list_jobs(batch, jobQueue=queue, filters=[{"name": "JOB_NAME", "values": ["smkflex-*"]}])
    rows = sorted(rows, key=lambda j: j.get("createdAt", 0), reverse=True)[:a.limit]
    if not rows:
        print(f"no smkflex runs in queue {queue}")
        return 0
    # the summaries have no tags; the commit is on the job itself
    tags = {d["jobId"]: d.get("tags") or {} for d in describe_jobs(batch, [j["jobId"] for j in rows])}
    for j in rows:
        when = datetime.fromtimestamp(j.get("createdAt", 0) / 1000).strftime("%Y-%m-%d %H:%M")
        t = tags.get(j["jobId"], {})
        what = j.get("jobName", "")[len("smkflex-"):]
        if t.get("smkflex-commit"):
            what += f"  {t['smkflex-commit']}" + (f"  ({t['smkflex-source']})" if t.get("smkflex-source") else "")
        print(f"{j['jobId']}  {j.get('status', ''):<9}  {when}  {what}")
    return 0

# Not resources/: in Snakemake's layout it has small files the workflow reads (gene lists,
# templates), which the jobs need; what rules download into it goes to storage anyway
IGNORE_LINES = ("data/", "results/", "logs/", ".snakemake/")

def cmd_init(a):
    """Set up this folder as a project: its own folder in the bucket (in the workflow profile
    Snakemake loads by itself, the existing one if the pipeline has one) and a .gitignore that
    keeps data out of smkflex run."""
    _dir, profile = _load_profile(a.profile)
    wp = workflow_profile_file(create=True)
    wp_dir = os.path.dirname(wp)
    current = {}
    if os.path.isfile(wp):
        with open(wp) as f:
            current = yaml.safe_load(f) or {}
    base = str(profile.get("default-storage-prefix") or "").rstrip("/")
    name = re.sub(r"[^A-Za-z0-9_.-]", "-", a.name or os.path.basename(os.path.abspath(".")))
    if current.get("default-storage-prefix"):
        prefix = current["default-storage-prefix"]
        print(f"This project already has its folder in storage: {prefix} (in {wp}); left as it is.")
    else:
        if not base:
            raise Fail("the profile has no default-storage-prefix to put the project's folder under")
        if not os.path.isdir("workflow") and not os.path.isfile("Snakefile"):
            print("(no workflow/ or Snakefile here; is this the project's top folder?)", file=sys.stderr)
        prefix = f"{base}/{name}"
        os.makedirs(wp_dir, exist_ok=True)
        ends_open = os.path.isfile(wp) and open(wp).read()[-1:] not in ("", "\n")
        with open(wp, "a") as f:
            f.write(("\n" if ends_open else "") + f"default-storage-prefix: {prefix}\n")
        print(f"This project's files go to {prefix} (written to {wp}).")
    lines = []
    if os.path.isfile(".gitignore"):
        with open(".gitignore") as f:
            lines = [l.rstrip("\n") for l in f]
    new = [l for l in IGNORE_LINES if l not in lines and l.rstrip("/") not in lines]
    if new:
        with open(".gitignore", "a") as f:
            if lines and lines[-1].strip():
                f.write("\n")
            f.write("# kept out of smkflex run (data reaches the jobs through storage)\n")
            f.write("".join(l + "\n" for l in new))
        print(f"Added {', '.join(new)} to .gitignore.")
    print("\nNext:\n"
          "  edit config/ (the pipeline's docs say what goes there)\n"
          "  smkflex push data          upload input files (data/ here, as the sample sheets name them)\n"
          "  snakemake --profile " + os.path.basename(_dir.rstrip("/")) + "     or: smkflex run")
    return 0

def main(argv=None):
    p = argparse.ArgumentParser(prog="smkflex", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run the workflow in this folder on AWS Batch; other arguments "
                       "(e.g. targets, --forceall) go to snakemake")
    r.add_argument("--profile", help="profile folder (default: ./profile, or $SNAKEMAKE_PROFILE)")
    r.add_argument("--git", metavar="URL", help="run the workflow from this git repository instead of this folder "
                   "(cloned here with your git login, so private repositories work)")
    r.add_argument("--rev", help="with --git: branch, tag or commit (default: the default branch)")
    r.add_argument("--path", help="with --git: folder in the repository that has the workflow")
    r.add_argument("--add", action="append", metavar="PATH",
                   help="send this local file or folder too, at the same path (with --git: over the "
                   "repository's, e.g. --add config/ for your own config); repeatable")
    r.add_argument("--name", help="the run's name in smkflex status and AWS Batch "
                   "(default: the folder's or repository's name)")
    r.add_argument("--queue", help="queue for the head job (default: the profile's)")
    r.add_argument("--role", help="IAM role for the head job (default: aws-batch-flex-head-role-arn)")
    r.add_argument("--image", help=f"image for the head job (default: {HEAD_IMAGE}, Debian 12 with git)")
    r.add_argument("--vcpus", type=int, default=1)
    r.add_argument("--mem-mb", type=int, default=2048)
    r.add_argument("--max-bundle-mb", type=int, default=100, help="stop when the workflow folder to send is "
                   "bigger than this many MB (default: 100)")
    r.add_argument("--wait", action="store_true", help="stay attached and print the log until the run ends")
    for name, text in (("logs", "print a run's Snakemake log"), ("cancel", "stop a run and its jobs")):
        s = sub.add_parser(name, help=text)
        s.add_argument("job_id")
        s.add_argument("--profile")
        s.add_argument("--region")
        if name == "logs":
            s.add_argument("--follow", "-f", action="store_true")
    s = sub.add_parser("pull", help="download files or folders from the workflow's storage, e.g. results")
    s.add_argument("paths", nargs="*", help="paths as the workflow names them, e.g. results or results/summary.tsv")
    s.add_argument("--profile")
    s.add_argument("--to", default=".", help="folder to download into (default: here)")
    s.add_argument("--prefix", help="storage prefix to download from (default: the profile's default-storage-prefix)")
    s.add_argument("--dry-run", action="store_true", help="only list what would be downloaded")
    s.add_argument("--force", action="store_true", help="download files that are already here too")
    s = sub.add_parser("init", help="set up this folder as a project: its own folder in the bucket, and a "
                       ".gitignore for data")
    s.add_argument("--profile")
    s.add_argument("--name", help="the project's folder name in the bucket (default: this folder's name)")
    s = sub.add_parser("push", help="upload input files or folders to the workflow's storage, at the same paths")
    s.add_argument("paths", nargs="+", help="local paths as the workflow names them: relative (data, "
                   "data/reads.fq) or absolute (/shared/refs), as in the config")
    s.add_argument("--profile")
    s.add_argument("--prefix", help="storage prefix to upload to (default: the profile's default-storage-prefix)")
    s.add_argument("--dry-run", action="store_true", help="only list what would be uploaded")
    s.add_argument("--force", action="store_true", help="upload files that are already there too")
    s = sub.add_parser("check", help="check the setup (AWS login, queue, roles, storage, versions)")
    s.add_argument("--profile")
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
        return {"logs": cmd_logs, "cancel": cmd_cancel, "status": cmd_status, "check": cmd_check,
                "pull": cmd_pull, "push": cmd_push, "init": cmd_init}[a.cmd](a)
    except Fail as e:
        print(f"smkflex: {e}", file=sys.stderr)
        return 2
    except (ClientError, BotoCoreError) as e:
        print(f"smkflex: AWS error: {e}", file=sys.stderr)
        return 2
    except WorkflowError as e:  # a setting in the profile
        print(f"smkflex: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
