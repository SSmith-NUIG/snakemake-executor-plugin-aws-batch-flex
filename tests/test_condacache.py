"""The conda env cache (condacache.py), with a stand-in for Snakemake's Env."""
import os, re, threading, time

from snakemake_executor_plugin_aws_batch_flex import condacache


class FakeEnv:
    builds = 0
    lock = threading.Lock()

    def __init__(self, prefix, name="abc123_"):
        self.address = os.path.join(prefix, name)
        self.name = self.dir = None
        self.is_containerized = False


def fake_create(self, dryrun=False):
    # what Snakemake's Env.create leaves behind: the env folder and the "done" flag
    with FakeEnv.lock:
        FakeEnv.builds += 1
    time.sleep(0.3)
    os.makedirs(os.path.join(self.address, "bin"))
    with open(os.path.join(self.address, "bin", "tool"), "w") as f:
        f.write("#!/bin/sh\n")
    open(self.address + ".env_setup_done", "w").close()
    return self.address


def test_jobs_on_one_machine_build_once(tmp_path):
    FakeEnv.builds = 0
    create = condacache.wrap_create(fake_create)
    threads = [threading.Thread(target=create, args=(FakeEnv(str(tmp_path)),)) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert FakeEnv.builds == 1  # the others waited for the lock and found it built


def test_cache_in_storage_reaches_other_machines(tmp_path):
    FakeEnv.builds = 0
    cache = str(tmp_path / "bucket" / "conda-cache")  # a folder stands in for s3://...
    create = condacache.wrap_create(fake_create, cache)
    first, second = tmp_path / "machine1", tmp_path / "machine2"
    assert create(FakeEnv(str(first))) == str(first / "abc123_")
    assert FakeEnv.builds == 1
    archive = tmp_path / "bucket" / "conda-cache" / condacache.platform_key() / "abc123_.tar.gz"
    assert archive.exists()
    # another machine: restored, not built
    path = create(FakeEnv(str(second)))
    assert FakeEnv.builds == 1
    assert os.path.exists(os.path.join(path, "bin", "tool")) and os.path.exists(path + ".env_setup_done")


def test_a_broken_archive_means_building(tmp_path):
    FakeEnv.builds = 0
    cache = tmp_path / "cache"
    (cache / condacache.platform_key()).mkdir(parents=True)
    (cache / condacache.platform_key() / "abc123_.tar.gz").write_bytes(b"not a tar file")
    path = condacache.wrap_create(fake_create, str(cache))(FakeEnv(str(tmp_path / "m")))
    assert FakeEnv.builds == 1 and os.path.exists(path + ".env_setup_done")


def test_install_sets_the_prefix_and_respects_one_given(tmp_path, monkeypatch):
    monkeypatch.setattr(condacache, "ROOT", str(tmp_path / "root"))
    from snakemake.deployment import conda as smk_conda
    orig = smk_conda.Env.create
    try:
        argv = condacache.install(["--cores", "1"], "instance")
        assert argv == ["--cores", "1", "--conda-prefix", str(tmp_path / "root" / condacache.platform_key())]
        assert getattr(smk_conda.Env.create, "_smkflex", False)
        assert condacache.install(["--conda-prefix", "/x"], "instance") == ["--conda-prefix", "/x"]
        assert condacache.install(["--deploy-sources", "s3://b/src.tar.xz", "abc"], "instance") == \
            ["--deploy-sources", "s3://b/src.tar.xz", "abc"]
    finally:
        smk_conda.Env.create = orig


def test_install_gives_up_quietly_when_the_folder_is_not_writable(tmp_path, monkeypatch):
    ro = tmp_path / "ro"
    ro.mkdir()
    ro.chmod(0o500)
    monkeypatch.setattr(condacache, "ROOT", str(ro))
    try:
        if os.access(ro, os.W_OK):  # running as root: permissions don't apply
            return
        assert condacache.install(["--cores", "1"], "instance") == ["--cores", "1"]
    finally:
        ro.chmod(0o700)


def _job(tmp_path, code):
    """A job's shell script with the launcher, as the plugin sends it (cf. test_script_python)."""
    import subprocess, sys, textwrap
    from snakemake_executor_plugin_aws_batch_flex.bootstrap import image_python_snippet
    (tmp_path / "Snakefile").write_text(textwrap.dedent('''
        rule all:
            run:
                print("PREFIX", workflow.deployment_settings.conda_prefix)
    '''))
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    script = (f'set -eu\nSMK_WORK={work}\nSMK_PY={sys.executable}\npython() {{ "$SMK_PY" "$@"; }}\n'
              f'{image_python_snippet()}\ncd {tmp_path}\npython -m snakemake -c1 -q\n')
    env = dict(os.environ, SMK_FLEX_CONDA_CACHE="instance", SMK_FLEX_JOB_CODE=str(code),
               SMK_FLEX_CONDA_ROOT=str(tmp_path / "root"))
    return subprocess.run(["sh", "-c", script], env=env, capture_output=True, text=True, timeout=120)


def _tools(tmp_path, text, named_for=None):
    import hashlib
    data = (named_for or text).encode()
    code = tmp_path / f"jobtools-{hashlib.sha256(data).hexdigest()}.py"
    code.write_text(text)
    return code


def test_launcher_turns_the_job_tools_on(tmp_path):
    from snakemake_executor_plugin_aws_batch_flex.bootstrap import job_tools_source
    code = _tools(tmp_path, job_tools_source())
    r = _job(tmp_path, code)
    assert r.returncode == 0, r.stdout + r.stderr
    assert f"PREFIX {tmp_path / 'root' / condacache.platform_key()}" in r.stdout + r.stderr
    # and the usage figures, at the end
    assert re.search(r"\[snakemake-batch\] peak-memory-bytes=\d+ peak-cpus=[\d.]+ avg-cpus=[\d.]+ "
                     r"peak-disk-bytes=\d+", r.stderr), r.stderr


def test_launcher_runs_only_job_tools_that_match_their_checksum(tmp_path):
    from snakemake_executor_plugin_aws_batch_flex.bootstrap import job_tools_source
    code = _tools(tmp_path, 'print("SHOULD NOT RUN")\n' + job_tools_source(), named_for=job_tools_source())
    r = _job(tmp_path, code)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "doesn't match its checksum" in r.stderr and "SHOULD NOT RUN" not in r.stdout + r.stderr


def test_launcher_runs_the_job_without_the_cache_code(tmp_path):
    r = _job(tmp_path, tmp_path / "missing.py")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "job tools off for this job" in r.stderr and "PREFIX None" in r.stdout + r.stderr


def test_install_in_many_jobs_at_once(tmp_path, monkeypatch):
    # jobs starting together on one machine: none may turn the cache off
    monkeypatch.setattr(condacache, "ROOT", str(tmp_path / "root"))
    from snakemake.deployment import conda as smk_conda
    orig, results = smk_conda.Env.create, []
    try:
        threads = [threading.Thread(target=lambda: results.append(condacache.install([], "instance")))
                   for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(r and r[0] == "--conda-prefix" for r in results) and len(results) == 20
    finally:
        smk_conda.Env.create = orig


def test_environments_are_kept_apart_by_cpu_type_and_glibc(monkeypatch):
    import platform
    monkeypatch.setattr(platform, "machine", lambda: "aarch64")
    assert condacache.platform_key().startswith("aarch64-")
    assert condacache.env_prefix("/r") == "/r/" + condacache.platform_key()
