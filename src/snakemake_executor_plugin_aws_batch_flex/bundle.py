"""smkflex run: the workflow folder packed for the head job (with --add files, or a --git clone),
the commit it records, and the head job's command."""

import io
import os
import re
import shlex
import shutil
import subprocess
import tarfile
from datetime import datetime, timezone

from . import headjob
from .bootstrap import conda_snippet, same_path, snakemake_prelude
from .profiles import Fail
from .settings import from_profile
from .sources import SKIP_DIRS, named_files, workflow_files

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

def config_named_files(root, snakemake_args=()):
    """{path: local file} of the small files named in the config files the run reads: the YAML
    in config/ and --configfile's (sources.named_files; the Snakefile's own config isn't read
    before Snakemake starts, so files it names alone are left out)."""
    import glob

    import yaml

    paths = glob.glob(os.path.join(root, "config", "**", "*.y*ml"), recursive=True)
    args = list(snakemake_args)
    for i, arg in enumerate(args):
        if arg.startswith(("--configfile=", "--configfiles=")):
            paths.append(os.path.join(root, arg.split("=", 1)[1]))
        elif arg in ("--configfile", "--configfiles"):
            for value in args[i + 1:]:
                if value.startswith("-"):
                    break
                paths.append(os.path.join(root, value))
    configs = []
    for p in paths:
        try:
            with open(p) as f:
                configs.append(yaml.safe_load(f))
        except (OSError, yaml.YAMLError):
            continue
    return {f: os.path.join(root, f) for f in named_files(configs, root)}


def make_bundle(root, profile_dir, max_mb, extra=None):
    """The workflow folder `root` as a tar.gz, with `extra` ({path in the bundle: local file})
    laid over it, plus the profile, this plugin and the head job's script."""
    buf = io.BytesIO()
    total = 0
    sources = {f: os.path.join(root, f) for f in workflow_files(root)}
    sources.update(extra or {})
    files = sorted(sources)
    # dereference: a linked file (config/samples.tsv -> ~/sheets/...) goes as its content; the
    # head job's unpacking refuses links that point outside the folder
    with tarfile.open(fileobj=buf, mode="w:gz", dereference=True) as t:
        for f in files:
            total += os.path.getsize(sources[f])
            if total > max_mb * 1e6:  # MB, as max-sources-mb
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

def head_command(profile, platform, bundle_url, state_url, snakemake_args, workdir=None):
    """The head job's command. With workdir (the project folder smkflex run was started in), it
    unpacks the workflow at that same path where it can, so that paths a Snakefile makes absolute
    (os.path.abspath) are where smkflex push put them, in the head job and its jobs alike."""
    s = from_profile(profile)
    mode = "auto" if s.bootstrap == "host" and platform == "fargate" else s.bootstrap
    resolved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    prelude = snakemake_prelude(s, mode, profile.get("default-storage-provider"), resolved_at)
    if s.conda_path and platform == "ec2":
        prelude += "\n" + conda_snippet(s.conda_path)
    args = " ".join(shlex.quote(a) for a in snakemake_args)
    return (
        f"{prelude}\n"
        'SMKFLEX_DIR="${TMPDIR:-/tmp}/smkflex-workflow"\n'
        + (f"SMKFLEX_HERE={shlex.quote(workdir)}\n" + same_path("SMKFLEX_HERE", "SMKFLEX_DIR") if workdir else "")
        + 'rm -rf "$SMKFLEX_DIR"; mkdir -p "$SMKFLEX_DIR"\n'
        'SMKFLEX_PY="${SMK_PY:-python}"\n'
        f'"$SMKFLEX_PY" -c {shlex.quote(_FETCH)} {shlex.quote(bundle_url)} "$SMKFLEX_DIR"\n'
        'cd "$SMKFLEX_DIR"\n'
        f'exec "$SMKFLEX_PY" .smkflex/head.py {shlex.quote(state_url)} {args}\n'
    )

def _git(*args, cwd=None):
    try:
        return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout.strip()
    except FileNotFoundError:
        raise Fail("git isn't installed here; it's needed for --git") from None
    except subprocess.CalledProcessError as e:
        raise Fail(f"git {' '.join(args[:2])} failed: {(e.stderr or e.stdout).strip()}") from None

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
            raise Fail(f"{url} has no branch, tag or commit '{rev}'") from None
        _git("submodule", "update", "--quiet", "--init", "--recursive", cwd=dest)
    return _git("rev-parse", "HEAD", cwd=dest)

def local_commit(root):
    """The commit of a local git checkout, with -modified if it has uncommitted changes,
    including new files that aren't git-ignored (the bundle sends those too)."""
    try:
        sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "."], cwd=root,
                               capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return sha[:12] + ("-modified" if dirty else "")

def _tag(value):
    return re.sub(r"[^A-Za-z0-9 _.:/=+\-@]", "_", str(value))[:256]
