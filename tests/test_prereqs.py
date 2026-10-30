"""The fallbacks for images without curl, wget or pip, run in a stripped PATH with stub tools."""
import os, stat, subprocess
from snakemake_executor_plugin_aws_batch_flex.bootstrap import build_prelude


def _env(tmp_path, tools, uid="0"):
    b = tmp_path / "bin"; b.mkdir()
    log = tmp_path / "calls.log"
    for t in ["mkdir", "cat"]:
        os.symlink(f"/bin/{t}" if os.path.exists(f"/bin/{t}") else f"/usr/bin/{t}", b / t)
    def stub(name, body):
        f = b / name; f.write_text("#!/bin/sh\n" + body); f.chmod(f.stat().st_mode | stat.S_IEXEC)
    stub("id", f"echo {uid}")
    for t in tools:
        stub(t, f'echo "{t} $*" >> {log}\nexit 0')
    return {"PATH": str(b), "TMPDIR": str(tmp_path)}, log


def _run(tmp_path, prereqs, tools, uid="0"):
    env, log = _env(tmp_path, tools, uid)
    script = tmp_path / "p.sh"
    script.write_text(build_prelude("always", "9.27.0", ["snakemake==9.27.0"], "3.12", prereqs))
    r = subprocess.run(["/bin/sh", str(script)], env=env, capture_output=True, text=True)
    return r, (log.read_text() if log.exists() else "")


def test_apt_installs_curl_when_no_fetcher(tmp_path):
    r, calls = _run(tmp_path, True, ["apt-get"])
    assert "apt-get update" in calls and "install" in calls and "curl" in calls and "ca-certificates" in calls
    assert calls.index("update") < calls.index("install")


def test_apk_used_when_no_apt(tmp_path):
    r, calls = _run(tmp_path, True, ["apk"])
    assert "apk add" in calls and "curl" in calls


def test_off_by_default_installs_nothing_and_fails_clearly(tmp_path):
    r, calls = _run(tmp_path, False, ["apt-get"])
    assert calls == "" and r.returncode == 127 and "cannot bootstrap" in r.stderr


def test_non_root_is_reported_not_attempted(tmp_path):
    r, calls = _run(tmp_path, True, ["apt-get"], uid="1000")
    assert calls == "" and "non-root" in r.stderr and "batch_queue" in r.stderr


def test_uv_from_the_images_conda_as_any_user(tmp_path):
    # mambaorg/micromamba: non-root, no curl; its micromamba fetches uv into the job's folder
    r, calls = _run(tmp_path, True, ["apt-get", "micromamba"], uid="1000")
    assert "micromamba create -y -q -p" in calls and "-c conda-forge uv=" in calls and "apt-get" not in calls


def test_skipped_when_curl_already_present(tmp_path):
    r, calls = _run(tmp_path, True, ["apt-get", "curl"])
    assert "apt-get" not in calls


def test_job_works_at_snakemakes_path_where_it_safely_can(tmp_path):
    # so that os.path.abspath() in a Snakefile gives the same paths in the job (zarp)
    from snakemake_executor_plugin_aws_batch_flex.bootstrap import WORKDIR

    def where(workdir):
        env = {"PATH": os.environ["PATH"], "TMPDIR": str(tmp_path)}
        if workdir:
            env["SMK_FLEX_WORKDIR"] = workdir
        r = subprocess.run(["sh", "-c", f"set -eu\n{WORKDIR}\npwd"], env=env, capture_output=True, text=True)
        return r.stdout.strip()

    full = tmp_path / "has-files"
    full.mkdir()
    (full / "x").write_text("")
    assert where(str(tmp_path / "home" / "me" / "proj")) == str(tmp_path / "home" / "me" / "proj")
    for unsafe in (None, "/", "/opt/x", "/usr/lib/x", str(full)):  # else a folder of its own
        assert where(unsafe).startswith(str(tmp_path / "snakemake-job-")), unsafe
