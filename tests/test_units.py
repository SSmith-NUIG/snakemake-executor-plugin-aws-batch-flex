import os
import re
import subprocess
import sys
import types
import pytest
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_executor_plugin_aws_batch_flex.sizing import fargate_size, job_requirements
from snakemake_executor_plugin_aws_batch_flex.bootstrap import build_prelude
from snakemake_executor_plugin_aws_batch_flex.jobdefs import normalize_image as _normalize_image


def test_fargate_rounding():
    assert fargate_size(1, 3000) == ("1", 3072)
    assert fargate_size(3, 1000) == ("4", 8192)
    with pytest.raises(WorkflowError):
        fargate_size(32, 1000)


def test_requirements():
    r = job_requirements({"mem_mb": 4000, "runtime": 30, "gpu": 2}, 8, "ec2")
    assert (r.vcpu, r.memory_mib, r.gpu, r.timeout_seconds) == ("8", 4256, 2, 1800)
    assert job_requirements({}, 1, "ec2").memory_mib == 2048
    assert job_requirements({"mem_mb": 4000}, 1, "ec2", 0).memory_mib == 4000
    assert job_requirements({"mem_mib": 1000}, 1, "fargate").memory_mib == 2048
    assert job_requirements({"mem_mib": 1900}, 1, "fargate").memory_mib == 3072
    with pytest.raises(WorkflowError):
        job_requirements({"gpu": 1}, 1, "fargate")


def test_image_normalisation():
    assert _normalize_image("docker://ubuntu:24.04") == "ubuntu:24.04"
    assert _normalize_image("quay.io/x/y:1") == "quay.io/x/y:1"
    with pytest.raises(WorkflowError):
        _normalize_image("shub://x/y")


@pytest.mark.parametrize("mode", ["auto", "always", "never"])
def test_prelude_is_valid_posix_sh(mode, tmp_path):
    f = tmp_path / "p.sh"
    f.write_text(build_prelude(mode, "9.27.0", ["snakemake==9.27.0", "pkg==1"], "3.12"))
    assert subprocess.run(["sh", "-n", str(f)]).returncode == 0



def test_image_registry():
    from snakemake_executor_plugin_aws_batch_flex.jobdefs import image_registry, is_ecr
    assert image_registry("ubuntu:24.04") == "docker.io"
    assert image_registry("staphb/bwa:0.7.19") == "docker.io"
    assert image_registry("quay.io/biocontainers/bwa:0.7.17") == "quay.io"
    assert image_registry("localhost:5000/x") == "localhost:5000"
    assert is_ecr(image_registry("123456789012.dkr.ecr.eu-west-1.amazonaws.com/tool:1"))
    assert not is_ecr("quay.io")


def test_known_problem_hints():
    from snakemake_executor_plugin_aws_batch_flex.hints import known_problem_hints as hints
    two = hints(["snakemake: error: unrecognized arguments: --storage-gcs-project x --storage-http-retries 5"])
    assert len(two) == 2 and "snakemake-storage-plugin-gcs" in two[0] and "plugin-http" in two[1]
    assert "versions are probably too far apart" in hints(["error: unrecognized arguments: --new-flag"])[0]
    h = hints(['ModuleNotFoundError in file "x/common.smk", line 3:', "ModuleNotFoundError: No module named 'pandas'"])
    assert len(h) == 1 and "update-ami.sh --extra-package pandas" in h[0]
    h = hints(["botocore.exceptions.ClientError: An error occurred (403) when calling the HeadObject operation: Forbidden"])
    assert len(h) == 1 and "job role" in h[0] and "s3_prefix" in h[0]
    assert "ran out of disk" in hints(["cp: error writing 'x': No space left on device"])[0]
    h = hints(["SyntaxError: Non-UTF-8 code starting with '\\xbc' in file /bin/sh on line 2, but no encoding declared"])
    assert len(h) == 1 and "ENTRYPOINT []" in h[0]
    assert hints(["all fine"]) == []


def test_version_note():
    from snakemake_executor_plugin_aws_batch_flex.preflight import version_note
    assert version_note(None, "9.27.0") == (None, None)
    assert version_note("9.27.0", "9.27.0") == (None, None)
    assert version_note("9.20.0", "9.27.0")[0] == "info"
    assert version_note("9.0.1", "9.27.0")[0] == "warning"
    assert version_note("10.0.0", "9.27.0")[0] == "warning"


class _Stub:
    def __init__(self, **calls): self.calls = calls
    def __getattr__(self, name):
        r = self.calls[name]
        if isinstance(r, Exception):
            def fail(**kw): raise r
            return fail
        return lambda **kw: r


def test_ami_snakemake_version_paths():
    from botocore.exceptions import ClientError
    from snakemake_executor_plugin_aws_batch_flex.preflight import ami_snakemake_version
    q = {"jobQueues": [{"computeEnvironmentOrder": [{"computeEnvironment": "ce"}]}]}
    tagged = {"Images": [{"Tags": [{"Key": "SnakemakeVersion", "Value": "9.27.0"}]}]}
    override = _Stub(describe_job_queues=q, describe_compute_environments={"computeEnvironments": [
        {"computeResources": {"ec2Configuration": [{"imageIdOverride": "ami-1"}]}}]})
    assert ami_snakemake_version(override, _Stub(describe_images=tagged), "q") == "9.27.0"
    via_lt = _Stub(describe_job_queues=q, describe_compute_environments={"computeEnvironments": [
        {"computeResources": {"launchTemplate": {"launchTemplateId": "lt-1", "version": "$Latest"}}}]})
    ec2 = _Stub(describe_images=tagged, describe_launch_template_versions={
        "LaunchTemplateVersions": [{"LaunchTemplateData": {"ImageId": "ami-2"}}]})
    assert ami_snakemake_version(via_lt, ec2, "q") == "9.27.0"
    assert ami_snakemake_version(override, _Stub(describe_images={"Images": [{"Tags": []}]}), "q") is None
    denied = ClientError({"Error": {"Code": "UnauthorizedOperation"}}, "DescribeImages")
    assert ami_snakemake_version(override, _Stub(describe_images=denied), "q") is None


def test_usage_summary():
    from snakemake_executor_plugin_aws_batch_flex.usage import Usage
    u = Usage()
    hour = 3600 * 1000
    # a Spot job retried once: both attempts count
    u.add("align", {"attempts": [{"startedAt": 0, "stoppedAt": hour}, {"startedAt": hour, "stoppedAt": 2 * hour}]},
          4, 8192, "ec2")
    u.add("qc", {"startedAt": 0, "stoppedAt": hour}, 1, 2048, "fargate")
    u.add("qc", {"status": "FAILED"}, 1, 2048, "fargate")  # never started
    t = u.table()
    align = next(l for l in t.splitlines() if l.strip().startswith("align")).split()
    assert align[1:5] == ["1", "8.00", "16.00", "4"]  # jobs, vCPU-h, GiB-h, vCPUs: no prices
    assert "USD" not in t and "costs: AWS Billing, by the jobs' tags snakemake-run-id" in t
    qc = next(l for l in t.splitlines() if l.strip().startswith("qc")).split()
    assert qc[1:4] == ["2", "1.00", "2.00"]
    assert "total" in t
    assert Usage().table() is None


def test_usage_hints_at_grouping_many_short_jobs():
    from snakemake_executor_plugin_aws_batch_flex.usage import Usage
    u = Usage()
    for _ in range(25):
        u.add("tiny", {"startedAt": 0, "stoppedAt": 5000}, 1, 1024, "ec2")
    for _ in range(25):
        u.add("long", {"startedAt": 0, "stoppedAt": 600000}, 1, 1024, "ec2")
    for _ in range(5):
        u.add("few", {"startedAt": 0, "stoppedAt": 5000}, 1, 1024, "ec2")
    t = u.table()
    assert "Hint: tiny ran 25 jobs of 5 s each" in t
    assert "Hint: long" not in t and "Hint: few" not in t


def test_keep_unneeded_temp():
    from snakemake_executor_plugin_aws_batch_flex.patches import keep_unneeded_temp as k
    cmd = "python -m snakemake --target-jobs a: --unneeded-temp-files x.db 'y z.db' --force --cores all"
    assert k(cmd) == "python -m snakemake --target-jobs a: --force --cores all"
    assert k("python -m snakemake --unneeded-temp-files a.db") == "python -m snakemake"
    assert k("python -m snakemake --force") == "python -m snakemake --force"


def test_s3_directory_is_looked_up_again():
    import pytest
    s3 = pytest.importorskip("snakemake_storage_plugin_s3")
    from botocore.exceptions import ClientError
    from snakemake_executor_plugin_aws_batch_flex.patches import recheck_s3_directories as _recheck_s3_directories
    _recheck_s3_directories()
    _recheck_s3_directories()  # patching twice is harmless

    class Obj:
        exists, is_dir = s3.StorageObject.exists, s3.StorageObject.is_dir
        def __init__(self):
            self._is_dir, self.keys, self.listed = None, [], 0
        def get_subkeys(self):
            self.listed += 1
            return iter(self.keys)
        def bucket_exists(self):
            return True
        def s3obj(self):  # no plain object of this name: a directory, or nothing yet
            def load():
                raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
            return types.SimpleNamespace(load=load)

    o = Obj()
    assert o.exists() is False  # before the job: no files under the prefix
    o.keys = ["star_genome/SA"]  # the job uploaded the directory
    assert o.exists() is True
    # what mtime() and size() ask for every file: answered from memory, no extra S3 request
    f = Obj()
    assert f.is_dir() is False and f.is_dir() is False and f.listed == 1


def test_memory_suggestions():
    from snakemake_executor_plugin_aws_batch_flex.usage import Usage
    u = Usage()
    d = {"startedAt": 0, "stoppedAt": 1000}
    u.add("big", d, 1, 8448, "ec2")     # asked 8 GiB + overhead, used 1 GiB: wasted
    u.add("tight", d, 1, 4352, "ec2")   # asked 4 GiB + overhead, used 3.9 GiB: at risk
    u.add("fine", d, 1, 4352, "ec2")    # used 2.5 GiB: leave it
    u.peaks.update({"big": [1024.0, 900.0], "tight": [3900.0], "fine": [2500.0]})
    tips = u.suggestions(256)
    # big: 1024 * 1.3 -> 1400 MiB container, minus 256 for Snakemake = 1144 MiB = 1200 MB
    assert tips == {"big": 1200, "fine": 3200, "tight": 5100}
    t = u.table(256)
    assert "set-resources:\n  big:\n    mem_mb: 1200    # job gets 1400 MiB; had 8448, peak 1024\n  fine:" in t
    import yaml
    block = t[t.index("set-resources:"):]
    assert yaml.safe_load(block)["set-resources"]["tight"] == {"mem_mb": 5100}  # pastes as YAML
    assert "busy" in t and "disk GiB" in t


def test_sources_patch_leaves_other_executors_alone(tmp_path, monkeypatch):
    """Snakemake imports every executor plugin; the get_sources patch only acts for this one."""
    import types

    import snakemake.api  # noqa: F401  (loads Snakemake in the order its CLI does)
    from snakemake.dag import DAG

    from snakemake_executor_plugin_aws_batch_flex import patches

    patches.skip_remote_module_scripts()  # in case the plugin was imported before Snakemake

    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "samples.tsv").write_text("s\n")
    monkeypatch.setattr(DAG.get_sources, "__wrapped__", lambda self: {"Snakefile"})

    def dag(executor):
        wf = types.SimpleNamespace(executor_plugin=types.SimpleNamespace(name=executor), rules=[])
        return types.SimpleNamespace(workflow=wf)

    assert DAG.get_sources(dag("slurm")) == {"Snakefile"}
    assert DAG.get_sources(dag("aws-batch-flex")) == {"Snakefile", "config/samples.tsv"}


def test_cancel_terminates_jobs_in_parallel_and_goes_on_after_errors():
    import threading, time, types
    from botocore.exceptions import ClientError
    from snakemake_executor_plugin_aws_batch_flex.executor import Executor

    done, warnings, lock = [], [], threading.Lock()

    def terminate_job(jobId, reason):
        time.sleep(0.2)
        if jobId == "j3":
            raise ClientError({"Error": {"Code": "ClientException", "Message": "gone"}}, "TerminateJob")
        with lock:
            done.append(jobId)

    me = types.SimpleNamespace(batch=types.SimpleNamespace(terminate_job=terminate_job),
                               logger=types.SimpleNamespace(warning=warnings.append))
    jobs = [types.SimpleNamespace(external_jobid=f"j{i}") for i in range(20)]
    start = time.time()
    Executor.cancel_jobs(me, jobs)
    assert time.time() - start < 2  # one at a time would take 4 s
    assert sorted(done) == sorted(f"j{i}" for i in range(20) if i != 3)
    assert len(warnings) == 1 and "j3" in warnings[0]


def test_reproducible_installs():
    from snakemake_executor_plugin_aws_batch_flex import hostenv
    from snakemake_executor_plugin_aws_batch_flex.bootstrap import UV_VERSION, locked
    from snakemake_executor_plugin_aws_batch_flex.jobdefs import mutable_tag

    # jobs that install Snakemake at start: a pinned uv, and packages as of the run's start
    p = build_prelude("always", "9.27.0", ["snakemake==9.27.0"], "3.12", resolved_at="2026-10-28T09:00:00Z")
    assert f"astral.sh/uv/{UV_VERSION}/install.sh" in p and f"uv=={UV_VERSION}" in p
    assert "--exclude-newer 2026-10-28T09:00:00Z snakemake==9.27.0" in p
    assert "--exclude-newer" not in build_prelude("always", "9.27.0", ["snakemake==9.27.0"], "3.12")
    # the AMI's env: exactly this machine's versions, all the way down
    pins, _skipped = locked(["snakemake"])
    assert any(p.startswith("snakemake==") for p in pins) and any(p.startswith("pyyaml==") for p in pins)
    script = hostenv.install_script("/opt/env", "3.12", ["snakemake==9.27.0"], pins)
    assert '-c "$ENV/constraints.txt"' in script and "\npyyaml==" in script
    assert f"astral.sh/uv/{UV_VERSION}/install.sh" in script and "astral.sh/uv/install.sh" not in script
    conda = hostenv.conda_script("/opt/miniforge3", [])
    assert hostenv.MINIFORGE in conda and "sha256sum -c" in conda and "latest" not in conda
    # images whose contents can change under the same name
    assert mutable_tag("debian") and mutable_tag("quay.io/x/y:latest") and mutable_tag("localhost:5000/img")
    assert not mutable_tag("debian:12") and not mutable_tag("localhost:5000/img:1.0")
    assert not mutable_tag("debian@sha256:" + "0" * 64)


def test_jobs_log_their_image_id(tmp_path, monkeypatch, capsys):
    import json
    from snakemake_executor_plugin_aws_batch_flex import jobmonitor
    meta = tmp_path / "meta.json"
    meta.write_text(json.dumps({"Image": "debian:12", "ImageID": "sha256:abc"}))
    monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", meta.as_uri())
    jobmonitor.log_image()
    assert "[snakemake-batch] image debian:12 id=sha256:abc" in capsys.readouterr().err


def test_versions_that_dont_fit_are_not_pinned(monkeypatch):
    import types
    from snakemake_executor_plugin_aws_batch_flex import bootstrap
    dists = {"top": ("1.0", ["dep>=2,<3", "other; extra == 'x'"]), "dep": ("3.1", []),
             "other": ("1.0", []), "local": ("1.0+cu12", [])}

    def distribution(name):
        if name not in dists:
            raise bootstrap.md.PackageNotFoundError(name)
        return types.SimpleNamespace(version=dists[name][0], requires=dists[name][1])

    monkeypatch.setattr(bootstrap.md, "distribution", distribution)
    # dep 3.1 breaks top's dep<3 (an environment mixed by hand); other is only for an extra
    assert bootstrap.locked(["top", "local"]) == (["top==1.0"], ["dep==3.1", "local==1.0+cu12"])


def test_rules_whose_jobs_ran_out_get_no_memory_suggestion(tmp_path, monkeypatch):
    from snakemake_executor_plugin_aws_batch_flex import jobmonitor
    from snakemake_executor_plugin_aws_batch_flex.usage import Usage
    (tmp_path / "memory.events").write_text("low 0\nhigh 0\nmax 3\noom 1\noom_kill 2\n")
    monkeypatch.setattr(jobmonitor, "CGROUP", str(tmp_path))
    assert jobmonitor._oom_kills() == 2
    monkeypatch.setattr(jobmonitor, "CGROUP", str(tmp_path / "none"))
    assert jobmonitor._oom_kills() is None

    u = Usage()
    for rule in ("ok", "killed"):
        u.add(rule, {"startedAt": 0, "stoppedAt": 1000}, 1, 4352, "ec2")
        u.add_log(rule, "g", rule)
    logs = {"ok": "peak-memory-bytes=1073741824 peak-cpus=1.00 oom-kills=0",
            "killed": "peak-memory-bytes=4294967296 peak-cpus=1.00 oom-kills=2"}
    client = types.SimpleNamespace(get_log_events=lambda logGroupName, logStreamName, **kw: {
        "events": [{"message": "[snakemake-batch] " + logs[logStreamName]}]})
    u.fetch_peaks(client)
    assert set(u.suggestions(256)) == {"ok"}
    assert "Hint: in killed's jobs, Linux killed 2 process(es) for lack of memory" in u.table(256)


def test_hint_for_a_script_that_wasnt_sent():
    from snakemake_executor_plugin_aws_batch_flex.hints import known_problem_hints as hints
    py = ["python: can't open file '/tmp/snakemake-job-7/scripts/synthetic/gen.py': [Errno 2] No such file or directory"]
    r = ["Fatal error: cannot open file 'scripts/plot.R': No such file or directory"]
    sh = ["bash: tools/run.sh: No such file or directory"]
    dash = ["sh: 0: cannot open scripts/a.sh: No such file"]
    old_dash = ["sh: 0: Can't open bin/b.sh"]
    for lines, name in ((py, "scripts/synthetic/gen.py"), (r, "scripts/plot.R"), (sh, "tools/run.sh"),
                        (dash, "scripts/a.sh"), (old_dash, "bin/b.sh")):
        h = hints(lines)
        assert len(h) == 1 and f"{name} wasn't sent to the job" in h[0] and "git-ignored" in h[0], h
    # a data file R can't read is something else
    assert not hints(["Error in file(file, \"rt\") : cannot open file 'data/x.csv': No such file or directory"])


def test_settings_are_checked_before_anything_reaches_aws():
    from snakemake_executor_plugin_aws_batch_flex.settings import (ExecutorSettings, normalized, registry_secrets,
                                                                   validate, volume_mounts)

    def check(**kw):
        return validate(normalized(ExecutorSettings(queue="q", **kw)))

    check()  # the defaults are fine
    secret, ex = "arn:aws:secretsmanager:eu-west-1:1:secret:hub", "arn:aws:iam::1:role/exec"
    bad = [({"platform": "lambda"}, "platform must be 'ec2' or 'fargate'"),
           ({"bootstrap": "sometimes"}, "bootstrap must be"),
           ({"script_python": "mine"}, "script-python must be"),
           ({"orphan_jobs": "kill"}, "orphan-jobs must be"),
           ({"assign_public_ip": "yes"}, "assign-public-ip must be"),
           ({"conda_cache": "somewhere"}, "must be none, instance or an s3:// URL"),
           ({"oom_memory_factor": 0.5}, "oom-memory-factor must be a number from 1 or more"),
           ({"spot_retries": 10}, "spot-retries must be a number from 0 to 9"),
           ({"ephemeral_storage_gib": 500}, "ephemeral-storage-gib must be a number from 21 to 200"),
           ({"volumes": ["/scratch"]}, "expected HOST:CONTAINER[:ro]"),
           ({"registry_credentials": ["docker.io"]}, "expected REGISTRY=SECRET_ARN"),
           ({"registry_credentials": [f"docker.io={secret}"]}, "needs --aws-batch-flex-execution-role-arn"),
           ({"registry_credentials": [f"123456789012.dkr.ecr.eu-west-1.amazonaws.com={secret}"],
             "execution_role_arn": ex}, "is ECR")]
    for kw, msg in bad:
        with pytest.raises(WorkflowError, match=re.escape(msg)):
            check(**kw)
    s = check(volumes=["/scratch:/scratch", "/ref:/ref:ro"], registry_credentials=[f"Index.Docker.io={secret}"],
              execution_role_arn=ex)
    assert volume_mounts(s) == [("/scratch", "/scratch", False), ("/ref", "/ref", True)]
    assert registry_secrets(s) == {"docker.io": secret}


def test_aws_errors_are_told_apart_by_their_codes(monkeypatch, tmp_path):
    from botocore.exceptions import ClientError
    from snakemake_executor_plugin_aws_batch_flex import condacache
    from snakemake_executor_plugin_aws_batch_flex.executor import _not_allowed

    def err(code, msg=""):
        return ClientError({"Error": {"Code": code, "Message": msg}}, "Op")

    raised = []
    s3 = types.SimpleNamespace(download_file=lambda b, k, d: (_ for _ in ()).throw(raised[-1]))
    monkeypatch.setattr(condacache, "_s3", lambda url: (s3, "b", "k"))
    raised.append(err("404"))
    assert condacache._fetch("s3://b/k", str(tmp_path / "x")) is False  # not cached yet: build
    raised.append(err("403", "Forbidden"))
    with pytest.raises(ClientError):  # not allowed: say so, don't pretend it isn't cached
        condacache._fetch("s3://b/k", str(tmp_path / "x"))

    assert _not_allowed(err("AccessDeniedException"))
    assert _not_allowed(err("ClientException", "User: x is not authorized to perform: iam:PassRole"))
    assert not _not_allowed(err("ClientException", "Job queue q does not exist"))


def test_a_workaround_for_changed_code_is_not_applied_but_noted():
    from snakemake_executor_plugin_aws_batch_flex import patches

    def changed(self, job, wildcards, files):  # say Snakemake added a parameter
        pass

    assert patches.fits(lambda self, job, files, include=None: 0, ["self", "job", "files"], "W1")
    assert not patches.fits(changed, ["self", "job", "files"], "W2")
    try:
        assert "W2" in patches.NOT_APPLIED and "W1" not in patches.NOT_APPLIED
    finally:
        patches.NOT_APPLIED.remove("W2")


def test_extra_packages_are_pinned_too():
    import importlib.metadata as md
    from snakemake_executor_plugin_aws_batch_flex import hostenv
    here = md.version("pytest")  # any package installed here with dependencies of its own
    pins = hostenv.default_constraints([], ["pytest"])
    assert f"pytest=={here}" in pins and any(p.startswith("pluggy==") for p in pins)
    # asked for a version this machine doesn't have: not pinned, so the install doesn't fail
    assert not any(p.startswith("pytest==") for p in hostenv.default_constraints([], ["pytest<1"]))


def test_numpy_that_cant_load_in_the_image_looks_not_installed(tmp_path):
    from snakemake_executor_plugin_aws_batch_flex.bootstrap import job_tools_source
    (tmp_path / "numpy").mkdir()  # as the AMI's numpy behaves in an image without libstdc++
    (tmp_path / "numpy" / "__init__.py").write_text(
        'raise ImportError("Original error was: libstdc++.so.6: cannot open shared object file")')
    (tmp_path / "tools.py").write_text(job_tools_source())
    code = ("ns = {}; exec(open('tools.py').read(), ns); ns['make_numpy_loadable']()\n"
            "for m in ('numpy', 'pandas'):\n"
            "    try: __import__(m)\n"
            "    except ModuleNotFoundError: print(m, 'not installed')\n")
    r = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(tmp_path)),
                       capture_output=True, text=True)
    assert r.stdout.split("\n")[:2] == ["numpy not installed", "pandas not installed"], r.stdout + r.stderr
    assert "can't load in this image" in r.stderr and "libstdc++.so.6" in r.stderr


def test_numpy_gets_the_amis_conda_libstdcxx_first(tmp_path, monkeypatch):
    import ctypes
    from snakemake_executor_plugin_aws_batch_flex import jobfixes
    root = tmp_path / "miniforge3"
    (root / "condabin").mkdir(parents=True)
    (root / "lib").mkdir()
    for lib in ("libgcc_s.so.1", "libstdc++.so.6"):
        (root / "lib" / lib).write_text("")
    monkeypatch.setenv("PATH", f"/usr/bin:{root / 'condabin'}")  # as bootstrap.conda_snippet puts it
    loaded = []
    monkeypatch.setattr(ctypes, "CDLL", lambda path, mode=0: loaded.append((path, mode)))
    jobfixes.make_numpy_loadable()
    # libgcc_s first (libstdc++ needs it), both for the whole process, before numpy is imported
    assert loaded == [(str(root / "lib" / "libgcc_s.so.1"), ctypes.RTLD_GLOBAL),
                      (str(root / "lib" / "libstdc++.so.6"), ctypes.RTLD_GLOBAL)]
    monkeypatch.setenv("PATH", "/usr/bin")
    loaded.clear()
    jobfixes.make_numpy_loadable()  # no conda mounted: nothing to load
    assert loaded == []


def test_each_library_the_image_lacks_comes_from_the_amis_conda(tmp_path):
    from snakemake_executor_plugin_aws_batch_flex.bootstrap import job_tools_source
    root = tmp_path / "miniforge3"
    (root / "condabin").mkdir(parents=True)
    (root / "lib").mkdir()
    for lib in ("libgcc_s.so.1", "libstdc++.so.6", "libz.so.1"):
        (root / "lib" / lib).write_text("")
    # as the AMI's numpy behaves in samtools' bioconda image: libz missing, until it's loaded
    (tmp_path / "pkgs" / "numpy").mkdir(parents=True)
    (tmp_path / "pkgs" / "numpy" / "__init__.py").write_text(
        "import builtins\n"
        "if 'libz.so.1' not in getattr(builtins, 'loaded', []):\n"
        "    raise ImportError('Original error was: libz.so.1: cannot open shared object file')\n")
    (tmp_path / "tools.py").write_text(job_tools_source())
    code = ("import builtins, ctypes, os\n"
            "builtins.loaded = []\n"
            "ctypes.CDLL = lambda path, mode=0: builtins.loaded.append(os.path.basename(path))\n"
            "ns = {}; exec(open('tools.py').read(), ns); ns['make_numpy_loadable']()\n"
            "import numpy\n"
            "print('loaded', builtins.loaded)\n")
    env = dict(os.environ, PYTHONPATH=str(tmp_path / "pkgs"), PATH=f"/usr/bin:/bin:{root / 'condabin'}")
    r = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "loaded ['libgcc_s.so.1', 'libstdc++.so.6', 'libz.so.1']" in r.stdout
    assert "can't load in this image" not in r.stderr
