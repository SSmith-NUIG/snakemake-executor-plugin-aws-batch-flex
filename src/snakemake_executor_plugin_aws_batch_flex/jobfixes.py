"""Fixes the job's launcher applies before Snakemake starts in the job (it loads this file with
condacache.py and jobmonitor.py; job_tools_source). Only the standard library."""

import sys


def hide_unloadable_numpy():
    """numpy (and pandas) in Snakemake's env, built against the system's libstdc++.so.6, can't load
    in an image without one where Linux looks (bioconda's images keep theirs in their conda env).
    Snakemake tries pandas in every script: job and only shrugs off "not installed": make it that,
    and say so."""
    try:
        import numpy  # noqa: F401
    except ModuleNotFoundError:
        pass
    except ImportError as e:
        sys.modules["numpy"] = sys.modules["pandas"] = None
        why = (str(e).strip().splitlines() or [""])[-1][:200]
        print(f"[snakemake-batch] numpy and pandas of Snakemake's env can't load in this image, so this job "
              f"runs without them: {why}", file=sys.stderr, flush=True)
