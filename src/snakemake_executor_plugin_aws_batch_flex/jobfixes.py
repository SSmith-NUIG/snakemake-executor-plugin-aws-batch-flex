"""Fixes the job's launcher applies before Snakemake starts in the job (it loads this file with
condacache.py and jobmonitor.py; job_tools_source). Only the standard library."""

import os
import sys


def _conda_root():
    """The AMI's conda mounted into the job, found by its condabin on PATH (bootstrap.conda_snippet)."""
    for d in os.environ.get("PATH", "").split(os.pathsep):
        if d.endswith(os.sep + "condabin"):
            return os.path.dirname(d)
    return None


def make_numpy_loadable():
    """numpy (and pandas) in Snakemake's env need the system's libstdc++.so.6, which many images
    don't have where Linux looks (bioconda's keep theirs inside their conda env). A Snakefile that
    imports pandas then fails in every job, and Snakemake itself tries pandas in every script: job.

    The AMI's conda, mounted into the job, has conda-forge's (made to work on old and new
    systems): load it into this process first, so numpy finds it already loaded. Only Snakemake's
    own process gets it, not the rule's commands. Without one, and if numpy still can't load,
    numpy and pandas look not installed (which Snakemake shrugs off), and the log says why."""
    root = _conda_root()
    if root:
        import ctypes

        for lib in ("libgcc_s.so.1", "libstdc++.so.6"):
            path = os.path.join(root, "lib", lib)
            if os.path.exists(path):
                try:
                    ctypes.CDLL(path, mode=ctypes.RTLD_GLOBAL)
                except OSError:
                    pass
    try:
        import numpy  # noqa: F401
    except ModuleNotFoundError:
        pass
    except ImportError as e:
        sys.modules["numpy"] = sys.modules["pandas"] = None
        why = (str(e).strip().splitlines() or [""])[-1][:200]
        print(f"[snakemake-batch] numpy and pandas of Snakemake's env can't load in this image, so this job "
              f"runs without them: {why}", file=sys.stderr, flush=True)
