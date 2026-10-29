"""Reuse conda environments between jobs instead of building them in every job.

Runs inside the job (in Snakemake's Python, from the job's launcher). Environments are built
under one fixed folder, so Snakemake gives the same environment the same name in every job
(its name is a hash of the definition and the folder). Before building one, a job looks:

1. in that folder: on EC2 it's the instance's /var/cache/smkflex-conda, shared by the jobs
   on that machine (a lock makes the others wait while one builds);
2. in S3, if a cache URL is set: <url>/<platform>/<name>.tar.gz, uploaded by an earlier job.

The folder (<platform>) is the CPU type and the image's glibc version, e.g. x86_64-glibc-2.36-strict,
so environments are only shared between machines and images that conda would have built the
same packages for: an x86 build doesn't run on Graviton (arm64).

Only the standard library and boto3 (which comes with the S3 storage plugin) are used."""

import fcntl
import os
import platform
import shutil
import sys
import tarfile
import tempfile

ROOT = os.environ.get("SMK_FLEX_CONDA_ROOT", "/tmp/smkflex-conda")


def platform_key():
    try:
        version = os.confstr("CS_GNU_LIBC_VERSION") or ""
    except (ValueError, OSError, AttributeError):
        version = ""
    # -strict: built with strict channel priority (CONDA_CHANNEL_PRIORITY, set for every job),
    # as bioconda needs; environments built before, without it, are left behind
    return f"{platform.machine() or 'unknown'}-{version.replace(' ', '-') or 'unknown'}-strict"


def env_prefix(root=None):
    return os.path.join(root or ROOT, platform_key())


def _say(msg):
    print(f"[snakemake-batch] {msg}", file=sys.stderr, flush=True)


def _s3(url):
    import boto3

    bucket, _, key = url[len("s3://"):].partition("/")
    return boto3.client("s3"), bucket, key


def _fetch(url, dest):
    """Copy the cached archive at url (s3:// or a folder path) to dest; False if there's none."""
    if url.startswith("s3://"):
        s3, bucket, key = _s3(url)
        from botocore.exceptions import ClientError

        try:
            s3.download_file(bucket, key, dest)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                return False
            raise
        return True
    if not os.path.isfile(url):
        return False
    shutil.copyfile(url, dest)
    return True


def _store(src, url):
    if url.startswith("s3://"):
        s3, bucket, key = _s3(url)
        s3.upload_file(src, bucket, key)
    else:
        os.makedirs(os.path.dirname(url), exist_ok=True)
        tmp = url + ".part"
        shutil.copyfile(src, tmp)
        os.replace(tmp, url)


def restore(url, prefix, name):
    """Unpack the cached environment `name` into prefix. True if it's there afterwards."""
    fd, archive = tempfile.mkstemp(dir=prefix, suffix=".tar.gz")
    os.close(fd)
    try:
        if not _fetch(url, archive):
            return False
        with tarfile.open(archive) as t:
            members = t.getmembers()
            if not members or any(not (m.name == name or m.name.startswith(name + "/")
                                       or m.name == name + ".env_setup_done") for m in members):
                raise ValueError(f"unexpected contents in {url}")
            # the flag file comes last in the archive, so a half-unpacked env has none and
            # Snakemake removes and rebuilds it
            if hasattr(tarfile, "data_filter"):
                # "tar" keeps the env's own permissions and links, unlike the "data" default
                t.extractall(prefix, members=members, filter="tar")
            else:
                t.extractall(prefix, members=members)
        return os.path.exists(os.path.join(prefix, name + ".env_setup_done"))
    finally:
        os.remove(archive)


def save(url, prefix, name):
    fd, archive = tempfile.mkstemp(dir=prefix, suffix=".tar.gz")
    os.close(fd)
    try:
        with tarfile.open(archive, "w:gz", compresslevel=1) as t:
            t.add(os.path.join(prefix, name), arcname=name)
            t.add(os.path.join(prefix, name + ".env_setup_done"), arcname=name + ".env_setup_done")
        _store(archive, url)
    finally:
        os.remove(archive)


def wrap_create(orig, cache_url=None):
    """Wrap snakemake.deployment.conda.Env.create with the instance folder and the S3 cache."""

    def create(self, dryrun=False):
        if dryrun or getattr(self, "name", None) or getattr(self, "dir", None) or self.is_containerized:
            return orig(self, dryrun)
        path = self.address
        prefix, name = os.path.dirname(path), os.path.basename(path)
        done = path + ".env_setup_done"
        os.makedirs(prefix, exist_ok=True)
        with open(path + ".smkflex-lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)  # another job on this machine may be building it
            if os.path.isdir(path) and os.path.exists(done):
                _say(f"conda env {name} was built by an earlier job on this machine")
                return path
            key = f"{cache_url.rstrip('/')}/{platform_key()}/{name}.tar.gz" if cache_url else None
            if key:
                try:
                    if restore(key, prefix, name):
                        _say(f"conda env {name} restored from {key}")
                        return path
                except Exception as e:
                    _say(f"could not restore conda env {name} from the cache ({e!r}); building it")
                    shutil.rmtree(path, ignore_errors=True)
            result = orig(self, dryrun)
            if key and os.path.exists(done):
                try:
                    save(key, prefix, name)
                    _say(f"conda env {name} saved to {key}")
                except Exception as e:
                    _say(f"could not save conda env {name} to the cache: {e!r}")
            return result

    create._smkflex = True
    return create


def install(argv, setting):
    """Turn the cache on for this job: set Snakemake's conda prefix (unless the job has one)
    and wrap Env.create. setting is 'instance' or an S3 URL; anything going wrong leaves
    the job as it was."""
    # --deploy-sources only unpacks the workflow before the job's real run: no envs there
    if "--conda-prefix" in argv or "--deploy-sources" in argv:
        return argv
    try:
        prefix = env_prefix()
        os.makedirs(prefix, exist_ok=True)
        # can this job write there? (a unique name: jobs in other containers on the same
        # machine can have the same process ID)
        fd, probe = tempfile.mkstemp(dir=prefix, prefix=".write-test-")
        os.close(fd)
        os.remove(probe)
        import inspect

        from snakemake.deployment import conda as smk_conda

        env = smk_conda.Env
        # what wrap_create was written for
        if (list(inspect.signature(env.create).parameters)[:2] != ["self", "dryrun"]
                or not all(hasattr(env, a) for a in ("address", "is_containerized"))):
            raise RuntimeError("this Snakemake's conda Env changed")
        if not getattr(smk_conda.Env.create, "_smkflex", False):
            url = setting if setting.startswith("s3://") or setting.startswith("/") else None
            smk_conda.Env.create = wrap_create(smk_conda.Env.create, url)
    except Exception as e:
        _say(f"conda env cache off for this job: {e!r}")
        return argv
    return argv + ["--conda-prefix", prefix]
