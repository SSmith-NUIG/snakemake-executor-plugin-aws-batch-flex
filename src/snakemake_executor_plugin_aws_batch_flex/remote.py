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
import shutil
import tarfile
import tempfile
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
from .preflight import ami_snakemake_version, check_queue, check_role, queue_uses_spot, version_note
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


def workflow_profile_file(create=False):
    """The project's workflow profile, as Snakemake finds it when started here: the first of
    default/, profiles/default/ and (next to workflow/Snakefile) workflow/profiles/default/ that
    exists. With create, profiles/default/config.yaml when there's none (Snakemake reads that
    for a Snakefile at the top or in workflow/)."""
    candidates = ["default", os.path.join("profiles", "default")]
    if os.path.isfile(os.path.join("workflow", "Snakefile")):
        candidates.append(os.path.join("workflow", "profiles", "default"))
    for d in candidates:
        if os.path.isdir(d):
            for name in ("profile.yaml", "config.yaml"):
                if os.path.isfile(os.path.join(d, name)):
                    return os.path.join(d, name)
            return os.path.join(d, "config.yaml")
    return os.path.join("profiles", "default", "config.yaml") if create else None


def _only_installed_profile():
    """The name of the one profile in ~/.config/snakemake that uses this plugin, if there is
    exactly one (setup.sh installs it there)."""
    base = os.path.join(os.environ.get("XDG_CONFIG_HOME") or os.path.expanduser("~/.config"), "snakemake")
    names = []
    try:
        entries = sorted(os.listdir(base))
    except OSError:
        return None
    for n in entries:
        for f in ("profile.yaml", "config.yaml"):
            cfg = os.path.join(base, n, f)
            if os.path.isfile(cfg):
                try:
                    with open(cfg) as fh:
                        if (yaml.safe_load(fh) or {}).get("executor") == "aws-batch-flex":
                            names.append(n)
                except (OSError, yaml.YAMLError):
                    pass
                break
    if len(names) == 1:
        print(f"(using profile {names[0]} from {base})", file=sys.stderr)
        return names[0]
    return None


def _load_profile(path):
    """(profile folder, settings) for --profile: a folder, or a name that Snakemake looks up in
    ~/.config/snakemake/ (as `snakemake --profile NAME` does). Default: ./profile, then
    $SNAKEMAKE_PROFILE."""
    if path is None:
        path = "profile" if os.path.isdir("profile") else os.environ.get("SNAKEMAKE_PROFILE")
    if not path:
        path = _only_installed_profile()
    if not path:
        raise Fail("no profile: pass --profile (a folder with config.yaml, or a profile name)")
    found = None
    try:
        from snakemake.cli import get_profile_dir

        found = get_profile_dir(path)
    except ImportError:
        cfg = os.path.join(path, "config.yaml")
        found = (path, cfg) if os.path.isfile(cfg) else None
    if not found:
        raise Fail(f"profile {path} not found (looked for a folder with config.yaml or profile.yaml "
                   "here and in ~/.config/snakemake/)")
    folder, cfg = found
    with open(cfg) as f:
        settings = yaml.safe_load(f) or {}
    # The project's own workflow profile, which Snakemake loads on top (e.g. its storage prefix)
    wp = workflow_profile_file()
    if wp and os.path.isfile(wp):
        with open(wp) as f:
            settings.update(yaml.safe_load(f) or {})
    return str(folder), settings


def _flag(value):
    return str(value).lower() in ("true", "1", "yes")


def _clients(profile):
    cfg = BotoConfig(region_name=profile.get("aws-batch-flex-region"),
                     retries={"max_attempts": 10, "mode": "adaptive"})
    return boto3.client("batch", config=cfg), boto3.client("logs", config=cfg), cfg.region_name


def _ignore_patterns(root):
    """Patterns from a .gitignore at the top of a folder that isn't a git checkout (simple
    globs: `*.bam`, `data/`, `results`; no negation)."""
    try:
        with open(os.path.join(root, ".gitignore")) as f:
            lines = [l.strip() for l in f]
    except OSError:
        return []
    return [l for l in lines if l and not l.startswith(("#", "!"))]


def _ignored(rel, patterns):
    import fnmatch

    parts = rel.split("/")
    for p in patterns:
        anchored, p = p.startswith("/"), p.strip("/")
        if "/" in p or anchored:
            if fnmatch.fnmatch(rel, p) or any(fnmatch.fnmatch("/".join(parts[:i]), p) for i in range(1, len(parts))):
                return True
        elif any(fnmatch.fnmatch(part, p) for part in parts):
            return True
    return False


def workflow_files(root):
    """Files to send: what git would track (tracked plus untracked, not ignored), or
    everything outside .snakemake/.git when the folder isn't a git checkout, minus what a
    .gitignore there lists."""
    try:
        out = subprocess.run(["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=root,
                             capture_output=True, check=True).stdout.decode()
        files = [f for f in out.split("\0") if f]
    except (OSError, subprocess.CalledProcessError):
        patterns = _ignore_patterns(root)
        files = []
        for d, dirs, names in os.walk(root):
            dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
            files += [os.path.relpath(os.path.join(d, n), root) for n in names]
        files = [f for f in files if not _ignored(f, patterns)]
    return sorted(f for f in files
                  if not SKIP_DIRS.intersection(f.split("/")) and os.path.isfile(os.path.join(root, f)))


def added_files(paths, base="."):
    """{path in the bundle: local file} for --add: files, or every file in a folder, given
    relative to `base` (the current folder) and sent to the same place."""
    out = {}
    for p in paths:
        norm = os.path.normpath(p)
        if os.path.isabs(p) or norm == ".." or norm.startswith(".." + os.sep):
            raise Fail(f"--add {p}: give a path inside this folder (it's sent to the same place)")
        full = os.path.join(base, norm)
        if os.path.isfile(full):
            out[norm] = full
        elif os.path.isdir(full):
            for d, dirs, names in os.walk(full):
                dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
                for n in names:
                    f = os.path.join(d, n)
                    out[os.path.relpath(f, base)] = f
        else:
            raise Fail(f"--add {p}: no such file or folder here")
    return out


def make_bundle(root, profile_dir, max_mb, extra=None):
    """The workflow folder `root` as a tar.gz, with `extra` ({path in the bundle: local file})
    laid over it, plus the profile, this plugin and the head job's script."""
    buf = io.BytesIO()
    total = 0
    sources = {f: os.path.join(root, f) for f in workflow_files(root)}
    sources.update(extra or {})
    files = sorted(sources)
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for f in files:
            total += os.path.getsize(sources[f])
            if total > max_mb * 1024 * 1024:
                raise Fail(f"the workflow folder has more than {max_mb} MB of files (at {f}); data "
                           "belongs in storage. Add big files to .gitignore, or raise --max-bundle-mb.")
            t.add(sources[f], arcname=f)
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


def _git(*args, cwd=None):
    try:
        return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout.strip()
    except FileNotFoundError:
        raise Fail("git isn't installed here; it's needed for --git")
    except subprocess.CalledProcessError as e:
        raise Fail(f"git {' '.join(args[:2])} failed: {(e.stderr or e.stdout).strip()}")


def clone(url, rev, dest):
    """Shallow clone of `rev` (branch, tag; default: the default branch), or a full clone
    when it's a commit. Returns the commit."""
    opts = ["--quiet", "--recurse-submodules", "--shallow-submodules"]
    try:
        _git("clone", *opts, "--depth", "1", *(["--branch", rev] if rev else []), url, dest)
    except Fail:
        if not rev:
            raise
        shutil.rmtree(dest, ignore_errors=True)
        _git("clone", "--quiet", "--recurse-submodules", url, dest)
        try:
            _git("checkout", "--quiet", rev, cwd=dest)
        except Fail:
            raise Fail(f"{url} has no branch, tag or commit '{rev}'")
        _git("submodule", "update", "--quiet", "--init", "--recursive", cwd=dest)
    return _git("rev-parse", "HEAD", cwd=dest)


def local_commit(root):
    """The commit of a local git checkout, with -modified if it has uncommitted changes."""
    try:
        sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no", "."], cwd=root,
                               capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return sha[:12] + ("-modified" if dirty else "")


def _tag(value):
    return re.sub(r"[^A-Za-z0-9 _.:/=+\-@]", "_", str(value))[:256]


def cmd_run(a, snakemake_args):
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
            source = {"smkflex-commit": commit} if commit else {}
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
    name = re.sub(r"[^A-Za-z0-9_-]", "-", a.name or default_name)
    state_url = f"{prefix}/.smkflex/{name}/state.tar.gz"
    state_key = hashlib.sha256(state_url.encode()).hexdigest()[:16]
    batch, _logs, region = _clients(profile)

    errors, warnings, detected = check_queue(batch, queue, profile.get("aws-batch-flex-platform"))
    for w in warnings:
        print(f"warning: {w}", file=sys.stderr)
    if errors:
        raise Fail("; ".join(errors))
    platform = profile.get("aws-batch-flex-platform") or detected or "ec2"
    if queue_uses_spot(batch, queue):
        print(f"warning: queue {queue} uses Spot instances; if AWS takes the head job's instance back, "
              "the whole run stops. Use an on-demand queue for it (--queue).", file=sys.stderr)
    if not a.force:
        running = _active_heads(batch, queue, state_key)
        if running:
            raise Fail(f"workflow '{name}' is already running as head job {running[0]}; wait for it, "
                       f"`smkflex cancel {running[0]}`, or pass --force")

    bundle = make_bundle(root, profile_dir, a.max_bundle_mb, extra)
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
    env.append({"name": "SMKFLEX_BUNDLE_URL", "value": bundle_url})
    resp = batch.submit_job(
        jobName=batch_job_name(f"smkflex-{name}"),
        jobQueue=queue,
        jobDefinition=jobdef,
        containerOverrides={
            "command": ["/bin/sh", "-c", head_command(profile, platform, bundle_url, state_url, snakemake_args)],
            "resourceRequirements": [{"type": "VCPU", "value": vcpu}, {"type": "MEMORY", "value": str(mem)}],
            "environment": env,
        },
        tags={"smkflex-workflow": name[:256], "smkflex-state": state_key, **source},
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
    resp = batch.list_jobs(jobQueue=queue, filters=[{"name": "JOB_NAME", "values": ["smkflex-*"]}])
    rows = sorted(resp.get("jobSummaryList", []), key=lambda j: j.get("createdAt", 0), reverse=True)[:a.limit]
    if not rows:
        print(f"no smkflex runs in queue {queue}")
        return 0
    # the summaries have no tags; the commit is on the job itself
    tags = {d["jobId"]: d.get("tags") or {} for d in batch.describe_jobs(jobs=[j["jobId"] for j in rows])["jobs"]}
    for j in rows:
        when = datetime.fromtimestamp(j.get("createdAt", 0) / 1000).strftime("%Y-%m-%d %H:%M")
        t = tags.get(j["jobId"], {})
        what = j.get("jobName", "")[len("smkflex-"):]
        if t.get("smkflex-commit"):
            what += f"  {t['smkflex-commit']}" + (f"  ({t['smkflex-source']})" if t.get("smkflex-source") else "")
        print(f"{j['jobId']}  {j.get('status', ''):<9}  {when}  {what}")
    return 0


def _remote_files(prefix, path, s3):
    """(relative path, size) of the files under prefix/path, in storage."""
    path = path.strip("/")
    if prefix.startswith("s3://"):
        bucket, _, base = prefix[len("s3://"):].partition("/")
        base = base.strip("/")
        key = f"{base}/{path}" if base else path
        out = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key):
            for o in page.get("Contents", []):
                rel = o["Key"][len(base) + 1:] if base else o["Key"]
                # "results" must not also match "results2/..."
                if rel == path or rel.startswith(path + "/") or not path:
                    out.append((rel, o["Size"]))
        return out
    root = os.path.join(prefix, path)
    if os.path.isfile(root):
        return [(path, os.path.getsize(root))]
    out = []
    for d, _dirs, names in os.walk(root):
        for n in names:
            full = os.path.join(d, n)
            out.append((os.path.relpath(full, prefix), os.path.getsize(full)))
    return sorted(out)


def cmd_pull(a):
    _dir, profile = _load_profile(a.profile)
    prefix = str(profile.get("default-storage-prefix") or "").rstrip("/")
    if not prefix:
        raise Fail("no default-storage-prefix in the profile")
    region = profile.get("aws-batch-flex-region")
    s3 = boto3.client("s3", config=BotoConfig(region_name=region)) if prefix.startswith("s3://") else None
    if not a.paths:
        tops = sorted({rel.split("/")[0] for rel, _ in _remote_files(prefix, "", s3)
                       if not rel.startswith((".smkflex/", "snakemake-workflow-sources."))})
        raise Fail("say what to download, e.g. `smkflex pull results`. In storage: " + (", ".join(tops) or "nothing"))
    got = skipped = 0
    for path in a.paths:
        files = _remote_files(prefix, path, s3)
        if not files:
            print(f"nothing in storage under {path}", file=sys.stderr)
        for rel, size in files:
            dest = os.path.join(a.to, rel)
            if not a.force and os.path.isfile(dest) and os.path.getsize(dest) == size:
                skipped += 1
                continue
            if a.dry_run:
                print(f"would download {rel} ({size} bytes)")
                continue
            os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
            if s3:
                bucket, _, base = prefix[len("s3://"):].partition("/")
                key = f"{base.strip('/')}/{rel}" if base.strip("/") else rel
                s3.download_file(bucket, key, dest)
            else:
                with open(os.path.join(prefix, rel), "rb") as src, open(dest, "wb") as out:
                    out.write(src.read())
            print(f"downloaded {rel}")
            got += 1
    if not a.dry_run:
        print(f"{got} downloaded, {skipped} already here (same size; --force downloads them again)"
              if skipped else f"{got} downloaded")
    return 0


IGNORE_LINES = ("data/", "results/", "resources/", "logs/", ".snakemake/")


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
        with open(wp, "a") as f:
            f.write(f"default-storage-prefix: {prefix}\n")
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


def cmd_push(a):
    """Upload local input files to the same paths under the storage prefix, where the
    workflow's rules look for them."""
    _dir, profile = _load_profile(a.profile)
    prefix = str(profile.get("default-storage-prefix") or "").rstrip("/")
    if a.prefix:
        prefix = a.prefix.rstrip("/")
    if not prefix:
        raise Fail("no default-storage-prefix in the profile")
    files = added_files(a.paths)
    s3 = boto3.client("s3", config=BotoConfig(region_name=profile.get("aws-batch-flex-region"))) \
        if prefix.startswith("s3://") else None
    there = {}
    for top in {f.split("/")[0] for f in files}:
        there.update(dict(_remote_files(prefix, top, s3)))
    sent = skipped = 0
    for rel, local in sorted(files.items()):
        if not a.force and there.get(rel) == os.path.getsize(local):
            skipped += 1
            continue
        if a.dry_run:
            print(f"would upload {rel} ({os.path.getsize(local)} bytes)")
            continue
        if s3:
            bucket, _, base = prefix[len("s3://"):].partition("/")
            s3.upload_file(local, bucket, f"{base}/{rel}" if base else rel)
        else:
            os.makedirs(os.path.dirname(os.path.join(prefix, rel)), exist_ok=True)
            shutil.copyfile(local, os.path.join(prefix, rel))
        print(f"uploaded {rel}")
        sent += 1
    if not a.dry_run:
        print(f"{sent} uploaded to {prefix}" + (f", {skipped} already there (same size; --force sends "
                                                 "them again)" if skipped else ""))
    return 0


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
    r.add_argument("--name", help="workflow name; runs with the same name share state "
                   "(default: the folder's or repository's name)")
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
    s = sub.add_parser("pull", help="download files or folders from the workflow's storage, e.g. results")
    s.add_argument("paths", nargs="*", help="paths as the workflow names them, e.g. results or results/summary.tsv")
    s.add_argument("--profile")
    s.add_argument("--to", default=".", help="folder to download into (default: here)")
    s.add_argument("--dry-run", action="store_true", help="only list what would be downloaded")
    s.add_argument("--force", action="store_true", help="download files that are already here too")
    s = sub.add_parser("init", help="set up this folder as a project: its own folder in the bucket, and a "
                       ".gitignore for data")
    s.add_argument("--profile")
    s.add_argument("--name", help="the project's folder name in the bucket (default: this folder's name)")
    s = sub.add_parser("push", help="upload input files or folders to the workflow's storage, at the same paths")
    s.add_argument("paths", nargs="+", help="local paths as the workflow names them, e.g. data or data/reads.fq")
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
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
