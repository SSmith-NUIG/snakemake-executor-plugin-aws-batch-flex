"""conda_path mount. The env test needs a conda install at CONDA_TEST_ROOT (default /opt/mf)."""
import os, subprocess, textwrap
from pathlib import Path
import pytest
from snakemake_executor_plugin_aws_batch_flex import hostenv
from snakemake_executor_plugin_aws_batch_flex.bootstrap import conda_snippet
from test_integration import run

ROOT = os.environ.get("CONDA_TEST_ROOT", "/opt/mf")


def test_conda_script_creates_named_envs_and_is_valid_bash(tmp_path):
    s = hostenv.conda_script("/opt/miniforge3", [("rnaseq", "channels: [conda-forge]\ndependencies: [python=3.11]")])
    f = tmp_path / "c.sh"; f.write_text(s)
    assert subprocess.run(["bash", "-n", str(f)]).returncode == 0
    assert "env create -n rnaseq" in s and "env update -n rnaseq" in s
    with pytest.raises(ValueError):
        hostenv.conda_script("/x", [("bad name", "x")])


def test_snippet_puts_condabin_last_and_redirects_cache():
    s = conda_snippet("/opt/miniforge3")
    assert 'PATH="$PATH:/opt/miniforge3/condabin"' in s and "CONDA_PKGS_DIRS" in s


@pytest.mark.skipif(not Path(ROOT, "bin/conda").exists(), reason="no conda install for the test")
def test_named_existing_env_runs_via_conda_mount(tmp_path, monkeypatch):
    subprocess.run([f"{ROOT}/bin/conda", "create", "-y", "-q", "--offline", "-n", "smk_mount_test"], check=True, capture_output=True)
    smk = textwrap.dedent('''
        rule all:
            input: "who.txt"
        rule who:
            output: "who.txt"
            container: "docker://anything:1"
            conda: "smk_mount_test"
            shell: "echo $CONDA_PREFIX > {output}"
    ''')
    clean = os.environ["PATH"]
    monkeypatch.setenv("FAKEAWS_JOB_PATH", clean)                   # the job's own PATH: no conda
    monkeypatch.setenv("PATH", f"{clean}:{ROOT}/condabin")          # the launcher: Snakemake needs conda there too
    r, calls = run(tmp_path, smk, "--sdm", "conda", "--aws-batch-flex-conda-path", ROOT)
    assert r.returncode == 0, r.stdout[-2500:] + r.stderr[-2500:]
    assert (tmp_path / "store" / "who.txt").read_text().strip() == f"{ROOT}/envs/smk_mount_test"
    reg = next(c for c in calls if c["call"] == "register_job_definition")
    assert {"sourceVolume": "vol0", "containerPath": ROOT, "readOnly": True} in reg["mounts"]
