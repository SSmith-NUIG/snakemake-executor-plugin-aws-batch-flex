"""Workflows from examples/conformance, run against the fake Batch."""
import json, os, shutil, subprocess, sys
import pytest
from pathlib import Path

FAKE = str(Path(__file__).parent / "fakeaws")
CONF = Path(__file__).parent.parent / "examples" / "conformance"


def run_conf(tmp_path, smk, *extra):
    for item in ("scripts", "envs", smk):
        src = CONF / item
        shutil.copytree(src, tmp_path / item) if src.is_dir() else shutil.copy(src, tmp_path / item)
    log = tmp_path / "calls.jsonl"
    env = dict(os.environ, PYTHONPATH=FAKE, FAKEAWS_LOG=str(log))
    cmd = [sys.executable, "-m", "snakemake", "-s", smk, "--executor", "aws-batch-flex",
           "--aws-batch-flex-queue", "q", "--aws-batch-flex-default-image", "debian:12",
           "--default-storage-provider", "fs", "--default-storage-prefix", str(tmp_path / "store"),
           "--default-resources", "mem_mb=500", "--latency-wait", "10", *extra]
    r = subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=900)
    calls = [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []
    return r, calls


def test_script_rules_python_and_bash(tmp_path):
    r, calls = run_conf(tmp_path, "script.smk", "-j", "2")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    # The job runs with `--cores all`, so Snakemake caps threads at the CPUs of the machine that
    # runs it (a Batch host in real life, this test machine here). The plugin must still ask
    # Batch for the rule's 2 vCPUs.
    expected = min(2, os.cpu_count() or 1)
    assert (tmp_path / "store/results/py_script.txt").read_text().strip() == f"pyscript threads={expected} rule=py_script"
    assert (tmp_path / "store/results/sh_script.txt").read_text().strip() == "shscript threads=1"
    vcpu = {c["jobName"].split("_")[1]: next(r["value"] for r in c["res"] if r["type"] == "VCPU")
            for c in calls if c["call"] == "submit_job"}
    assert vcpu["py"] == "2" and vcpu["sh"] == "1"


def test_fanout_many_jobs(tmp_path):
    n = 12
    r, calls = run_conf(tmp_path, "fanout.smk", "--config", f"n_units={n}", "-j", "12")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    assert (tmp_path / "store/results/total.txt").read_text().strip() == str(n * (n - 1) // 2)
    subs = [c for c in calls if c["call"] == "submit_job"]
    assert len(subs) == n + 1                              # n units + gather
    assert len({c["jobdef"] for c in subs}) == 1           # one image => one job definition, reused


@pytest.mark.skipif(not shutil.which("Rscript"), reason="needs Rscript on this machine")
def test_r_script_rule(tmp_path):
    r, calls = run_conf(tmp_path, "rscript.smk", "-j", "1")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    expected = min(2, os.cpu_count() or 1)
    out = (tmp_path / "store/results/r/out.txt").read_text().strip()
    assert out.startswith(f"rscript threads={expected} rule=r_script R=")
    assert {c["image"] for c in calls if c["call"] == "register_job_definition"} == {"rocker/r-ver:4.4.2"}


def _conda_ok():
    try:
        v = subprocess.run(["conda", "--version"], capture_output=True, text=True).stdout.split()[-1]
        return tuple(int(x) for x in v.split(".")[:3]) >= (24, 7, 1)
    except Exception:
        return False


@pytest.mark.skipif(not (os.environ.get("RUN_NETWORK_TESTS") and _conda_ok()),
                    reason="needs RUN_NETWORK_TESTS=1 and conda >= 24.7.1 (downloads the wrapper and samtools)")
def test_wrapper_rule_with_conda(tmp_path):
    r, _ = run_conf(tmp_path, "wrapper.smk", "--sdm", "conda", "-j", "2")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    fai = (tmp_path / "store/results/wr/ref.fa.fai").read_text()
    assert fai.startswith("chr1\t12\t") and "chr2\t8\t" in fai
