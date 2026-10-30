"""Fixes the job's launcher applies before Snakemake starts in the job (it loads this file with
condacache.py and jobmonitor.py; job_tools_source). Only the standard library, and boto3 and the
S3 storage plugin when they're there."""

import os
import re
import sys

MAX_LIBRARIES = 10  # missing system libraries to load before giving up


def _conda_root():
    """The AMI's conda mounted into the job, found by its condabin on PATH (bootstrap.conda_snippet)."""
    for d in os.environ.get("PATH", "").split(os.pathsep):
        if d.endswith(os.sep + "condabin"):
            return os.path.dirname(d)
    return None


def _load(root, lib):
    """Load the AMI conda's copy of a library into this process (for every later lookup)."""
    import ctypes

    path = os.path.join(root, "lib", lib)
    if not os.path.exists(path):
        return False
    try:
        ctypes.CDLL(path, mode=ctypes.RTLD_GLOBAL)
        return True
    except OSError:
        return False


def make_numpy_loadable():
    """numpy (and pandas) in Snakemake's env need libraries of the system, such as libstdc++.so.6
    and libz.so.1, which many images don't have where Linux looks (bioconda's keep theirs inside
    their conda env). A Snakefile that imports pandas then fails in every job, and Snakemake
    itself tries pandas in every script: job.

    The AMI's conda, mounted into the job, has conda-forge's copies (made to work on old and new
    systems): load the missing ones into this process, so numpy finds them already loaded. Only
    Snakemake's own process gets them, not the rule's commands. If numpy still can't load
    (no conda mounted), numpy and pandas look not installed, which Snakemake shrugs off, and the
    log says why."""
    root = _conda_root()
    if root:
        _load(root, "libgcc_s.so.1")  # what libstdc++ itself needs
        _load(root, "libstdc++.so.6")
    for _ in range(MAX_LIBRARIES):
        try:
            import numpy  # noqa: F401
            return
        except ModuleNotFoundError:
            return
        except ImportError as e:
            error = e
            missing = re.search(r"(\S+\.so[.\d]*): cannot open shared object file", str(e))
            if not (root and missing and _load(root, os.path.basename(missing.group(1)))):
                break
            # what's left of the failed import goes, so the next try starts afresh
            for name in [m for m in sys.modules if m == "numpy" or m.startswith("numpy.")]:
                del sys.modules[name]
    sys.modules["numpy"] = sys.modules["pandas"] = None
    why = (str(error).strip().splitlines() or [""])[-1][:200]
    print(f"[snakemake-batch] numpy and pandas of Snakemake's env can't load in this image, so this job "
          f"runs without them: {why}", file=sys.stderr, flush=True)


S3_THREADS = 32  # files of a directory() output moved at once


def parallel_s3_directories():
    """Work around snakemake-storage-plugin-s3 (0.3.6): it uploads and downloads a directory()
    output one file at a time, and deletes it one file at a time, which takes minutes for one
    with thousands of small files (a datavzrd report: 30 MB in 14,000 files). Here they go
    S3_THREADS at a time, and deletes 1,000 per request, with a client of the plugin's settings
    (its own allows 10 connections). Single files are left to the plugin. Unlike the plugin's
    remove(), an emptied bucket isn't deleted. Returns why it isn't applied, or None."""
    try:
        import inspect

        from snakemake_storage_plugin_s3 import StorageObject
    except ImportError:
        return None  # not installed: nothing to work around
    store, retrieve, remove = StorageObject.store_object, StorageObject.retrieve_object, StorageObject.remove
    if getattr(store, "_parallel", False):
        return None
    try:
        source = inspect.getsource(StorageObject)
        if any(list(inspect.signature(f).parameters) != ["self"] for f in (store, retrieve, remove)) \
                or not all(name in source for name in ("def get_subkeys", "self.provider.s3c", "_is_dir")):
            return "this S3 storage plugin's StorageObject changed"
    except (OSError, TypeError):
        return "this S3 storage plugin's StorageObject can't be checked"

    from concurrent.futures import ThreadPoolExecutor
    from weakref import WeakKeyDictionary

    clients = WeakKeyDictionary()  # provider -> its client for this

    def client(obj):
        if obj.provider not in clients:
            import boto3

            s = obj.provider.settings
            clients[obj.provider] = boto3.client(
                "s3", endpoint_url=s.endpoint_url, region_name=s.region, aws_access_key_id=s.access_key,
                aws_secret_access_key=s.secret_key, aws_session_token=s.token,
                config=boto3.session.Config(signature_version=s.signature_version, max_pool_connections=S3_THREADS,
                                            retries={"max_attempts": s.retries, "mode": "standard"}))
        return clients[obj.provider]

    def store_object(self):
        local = self.local_path()
        if not local.is_dir() or not self.bucket_exists():  # the plugin makes a missing bucket
            return store(self)
        self._is_dir = True
        c = client(self)
        files = [p for p in local.rglob("*") if p.is_file()]
        with ThreadPoolExecutor(S3_THREADS) as pool:
            list(pool.map(lambda p: c.upload_file(str(p), self.bucket, f"{self.key}/{p.relative_to(local).as_posix()}"),
                          files))

    def retrieve_object(self):
        if not self.is_dir():
            return retrieve(self)
        local, prefix, c = self.local_path(), self.key + "/", client(self)
        local.mkdir(parents=True, exist_ok=True)

        def get(key):
            path = local / key[len(prefix):]
            path.parent.mkdir(parents=True, exist_ok=True)
            c.download_file(self.bucket, key, str(path))

        with ThreadPoolExecutor(S3_THREADS) as pool:
            list(pool.map(get, [item.key for item in self.get_subkeys()]))

    def remove_(self):
        if not self.is_dir():
            return remove(self)
        keys, c = [item.key for item in self.get_subkeys()], client(self)
        for i in range(0, len(keys), 1000):
            c.delete_objects(Bucket=self.bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]],
                                                         "Quiet": True})

    store_object._parallel = True
    StorageObject.store_object, StorageObject.retrieve_object, StorageObject.remove = store_object, retrieve_object, remove_
    return None


SYSTEM_DIRS = ("/proc", "/sys", "/dev", "/etc", "/usr", "/bin", "/sbin", "/lib", "/lib64", "/boot", "/opt",
               "/run", "/var/lib")  # never local copies of storage files there


def plain_local_paths(argv):
    """Keep the job's local copies of the run's storage files at their plain paths: results/x at
    <working folder>/results/x and an absolute /data/x at /data/x, instead of under
    .snakemake/storage/<bucket>/<prefix>/. Tools then find inputs however the Snakefile names
    them, {input}, a param or os.path.abspath(), as on a cluster's shared filesystem (zarp passes
    STAR its genome index as a param). Only for a job that works at the same path as Snakemake
    (bootstrap.WORKDIR), so that a plain path means the same as where Snakemake runs. Returns why
    it isn't applied, or None."""
    args = list(argv)
    prefix = next((a.split("=", 1)[1] for a in args if a.startswith("--default-storage-prefix=")), None)
    if prefix is None and "--default-storage-prefix" in args[:-1]:
        prefix = args[args.index("--default-storage-prefix") + 1]
    if not prefix:
        return None  # no default storage: nothing to keep elsewhere
    prefix = prefix.rstrip("/")
    try:
        import inspect
        import posixpath
        from pathlib import Path

        from snakemake_interface_storage_plugins.storage_object import StorageObjectBase
    except ImportError:
        return "no storage plugin interface"
    orig = StorageObjectBase.local_path
    if getattr(orig, "_plain", False):
        return None
    try:
        if list(inspect.signature(orig).parameters) != ["self"] or "_overwrite_local_path" not in inspect.getsource(orig):
            return "the storage interface's StorageObjectBase.local_path changed"
    except (OSError, TypeError):
        return "the storage interface's StorageObjectBase.local_path can't be checked"

    def local_path(self):
        query = str(self.query)
        if self._overwrite_local_path or not query.startswith(prefix + "/"):
            return orig(self)
        rest = posixpath.normpath(query[len(prefix) + 1:])
        if rest.startswith("/"):  # Snakemake's prefix//abs for an absolute path
            if rest == "/" or any(rest == d or rest.startswith(d + "/") for d in SYSTEM_DIRS):
                return orig(self)
            return Path(rest)
        if rest == ".." or rest.startswith("../"):
            return orig(self)
        # absolute: Snakemake's job takes a relative local path equal to the file's own name for
        # the file itself, and then doesn't see the outputs it made
        return Path(os.getcwd()) / rest

    local_path._plain = True
    StorageObjectBase.local_path = local_path
    return None
