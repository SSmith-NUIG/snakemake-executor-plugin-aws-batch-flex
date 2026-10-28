"""Fixes the job's launcher applies before Snakemake starts in the job (it loads this file with
condacache.py and jobmonitor.py; job_tools_source). Only the standard library."""

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
