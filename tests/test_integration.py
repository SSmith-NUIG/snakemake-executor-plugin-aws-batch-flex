"""Runs a real Snakemake workflow through the plugin against a fake AWS Batch."""
import json, os, subprocess, sys, textwrap
from pathlib import Path

FAKE = str(Path(__file__).parent / "fakeaws")

SNAKEFILE = textwrap.dedent('''
    container: "docker://ubuntu:24.04"

    rule all:
        input: "a.txt", "b.txt"

    rule make_a:
        output: "a.txt"
        threads: 4
        resources: mem_mb=3000, batch_queue="special-queue"
        shell: "echo A > {output}"

    rule make_b:
        input: "a.txt"
        output: "b.txt"
        container: "docker://quay.io/biocontainers/bwa:0.7.17"
        shell: "cat {input} > {output} && echo B >> {output}"
''')

FAILING = textwrap.dedent('''
    rule all:
        input: "x.txt"
    rule boom:
        output: "x.txt"
        shell: "echo about-to-fail-marker >&2; exit 3"
''')


def run(tmp_path, snakefile, *extra):
    (tmp_path / "Snakefile").write_text(snakefile)
    log = tmp_path / "calls.jsonl"
    env = dict(os.environ, PYTHONPATH=FAKE, FAKEAWS_LOG=str(log))
    cmd = [sys.executable, "-m", "snakemake", "--executor", "aws-batch-flex",
           "--aws-batch-flex-queue", "default-queue", "--aws-batch-flex-default-image", "debian:12",
           "--default-storage-provider", "fs", "--default-storage-prefix", str(tmp_path / "store"),
           "--default-resources", "mem_mb=500", "-j", "4", "--latency-wait", "5", *extra]
    r = subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=300)
    calls = [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []
    return r, calls


def test_per_rule_images_queues_and_resources(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE)
    assert r.returncode == 0, r.stdout + r.stderr
    assert (tmp_path / "store" / "b.txt").read_text() == "A\nB\n"
    images = {c["image"] for c in calls if c["call"] == "register_job_definition"}
    assert images == {"ubuntu:24.04", "quay.io/biocontainers/bwa:0.7.17"}
    a = next(c for c in calls if c["call"] == "submit_job" and c["queue"] == "special-queue")
    assert {"type": "VCPU", "value": "4"} in a["res"]
    # Snakemake derives mem_mib 2862 from mem_mb 3000; plus the default 256 MiB overhead
    assert {"type": "MEMORY", "value": "3118"} in a["res"]
    assert sum(c["queue"] == "default-queue" for c in calls if c["call"] == "submit_job") >= 1


def test_failure_surfaces_log_tail(tmp_path):
    r, _ = run(tmp_path, FAILING)
    assert r.returncode != 0
    out = r.stdout + r.stderr
    assert "FAILED" in out and "exit code: 1" in out and "exit status 3" in out
    assert "about-to-fail-marker" in out  # log tail was fetched and shown


def test_startup_check_wrong_queue(tmp_path):
    r, calls = run(tmp_path, FAILING, "--aws-batch-flex-queue", "no-such-queue")
    assert r.returncode != 0
    assert "Job queue 'no-such-queue' not found" in r.stdout + r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)


def test_startup_check_wrong_platform(tmp_path):
    r, calls = run(tmp_path, FAILING, "--aws-batch-flex-queue", "my-fargate-queue",
                   "--aws-batch-flex-platform", "ec2")
    assert r.returncode != 0
    assert "Set --aws-batch-flex-platform fargate" in r.stdout + r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)


MIXED = textwrap.dedent('''
    rule all:
        input: "e.txt", "f.txt"
    rule on_ec2:
        output: "e.txt"
        resources: mem_mb=1000
        shell: "echo E > {output}"
    rule on_fargate:
        output: "f.txt"
        resources: mem_mb=1000, batch_queue="my-fargate-queue"
        shell: "echo F > {output}"
''')


def test_mixed_ec2_and_fargate_queues(tmp_path):
    r, calls = run(tmp_path, MIXED, "--aws-batch-flex-execution-role-arn", "arn:aws:iam::1:role/exec",
                   "--aws-batch-flex-volumes", "/tmp:/scratch")
    assert r.returncode == 0, r.stdout + r.stderr
    assert (tmp_path / "store" / "f.txt").read_text() == "F\n"
    defs = {c["platform"][0]: c for c in calls if c["call"] == "register_job_definition"}
    assert set(defs) == {"EC2", "FARGATE"}
    assert defs["EC2"]["mounts"] and not defs["FARGATE"]["mounts"]  # no host mounts on Fargate
    subs = {c["queue"]: c for c in calls if c["call"] == "submit_job"}
    assert {"type": "MEMORY", "value": "1210"} in subs["default-queue"]["res"]  # 954 + 256
    assert {"type": "MEMORY", "value": "2048"} in subs["my-fargate-queue"]["res"]  # Fargate minimum for 1 vCPU
    assert "Fargate jobs get no host mounts" in r.stdout + r.stderr


def test_fargate_rule_without_execution_role_fails_clearly(tmp_path):
    r, calls = run(tmp_path, MIXED)
    assert r.returncode != 0
    assert "requires --aws-batch-flex-execution-role-arn" in r.stdout + r.stderr
    assert not any(c["call"] == "submit_job" and c["queue"] == "my-fargate-queue" for c in calls)


def test_jobs_fetch_wrappers_over_http_not_git(tmp_path, monkeypatch):
    cmd = tmp_path / "cmd.txt"
    monkeypatch.setenv("FAKEAWS_CMD", str(cmd))
    r, _ = run(tmp_path, MIXED.replace(', batch_queue="my-fargate-queue"', ""))
    assert r.returncode == 0, r.stdout + r.stderr
    assert "--wrapper-prefix https://github.com/snakemake/snakemake-wrappers/raw/" in cmd.read_text()
    # ECS allows 8192 characters for all overrides of a job; leave room for env vars and JSON.
    assert len(cmd.read_text()) < 6500, len(cmd.read_text())


PER_RULE = textwrap.dedent('''
    rule all:
        input: "a.txt", "b.txt"
    rule a:
        output: "a.txt"
        shell: "echo a > {output}"
    rule b:
        output: "b.txt"
        resources: batch_script_python="host"
        shell: "echo b > {output}"
''')


def _script_python(calls):
    return {c["jobName"].split("_")[1]: c["env"]["SMK_FLEX_SCRIPT_PYTHON"]
            for c in calls if c["call"] == "submit_job"}


def test_script_python_per_rule_overrides_setting(tmp_path):
    r, calls = run(tmp_path, PER_RULE)
    assert r.returncode == 0, r.stdout + r.stderr
    assert _script_python(calls) == {"a": "image", "b": "host"}
    r, calls = run(tmp_path, PER_RULE.replace('"host"', '"image"'), "--forceall",
                   "--aws-batch-flex-script-python", "host")
    assert r.returncode == 0, r.stdout + r.stderr
    assert _script_python(calls) == {"a": "host", "b": "image"}



SECRET = "arn:aws:secretsmanager:eu-west-1:1:secret:dockerhub"
EXEC = "arn:aws:iam::1:role/exec"


def test_registry_credentials_only_for_matching_registry(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE, "--aws-batch-flex-registry-credentials", f"docker.io={SECRET}",
                   "--aws-batch-flex-execution-role-arn", EXEC)
    assert r.returncode == 0, r.stdout + r.stderr
    defs = {c["image"]: c for c in calls if c["call"] == "register_job_definition"}
    hub, quay = defs["ubuntu:24.04"], defs["quay.io/biocontainers/bwa:0.7.17"]
    assert hub["creds"] == {"credentialsParameter": SECRET} and hub["exec_role"] == EXEC
    assert quay["creds"] is None and quay["exec_role"] is None  # EC2, no secret: no execution role


def test_registry_credentials_need_execution_role(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE, "--aws-batch-flex-registry-credentials", f"docker.io={SECRET}")
    assert r.returncode != 0 and "needs --aws-batch-flex-execution-role-arn" in r.stdout + r.stderr
    assert not calls


def test_registry_credentials_reject_ecr(tmp_path):
    ecr = "123456789012.dkr.ecr.eu-west-1.amazonaws.com"
    r, _ = run(tmp_path, SNAKEFILE, "--aws-batch-flex-registry-credentials", f"{ecr}={SECRET}",
               "--aws-batch-flex-execution-role-arn", EXEC)
    assert r.returncode != 0 and "is ECR" in r.stdout + r.stderr


def _orphan(jid, tmp_path, wf=None, jobdef="snakemake-0123abcd", queue="default-queue"):
    from snakemake_executor_plugin_aws_batch_flex.orphans import workflow_id
    wf = wf or workflow_id(str(tmp_path), str(tmp_path / "Snakefile"), str(tmp_path / "store"))
    return {"jobId": jid, "queue": queue, "jobDefinition": jobdef,
            "tags": {"snakemake-workflow": wf, "snakemake-run-id": "oldrun"}}


def test_leftover_jobs_are_cancelled(tmp_path, monkeypatch):
    orphans = [_orphan("old-1", tmp_path),
               _orphan("other-wf", tmp_path, wf="someoneelse"),
               _orphan("not-ours", tmp_path, jobdef="nextflow-thing")]
    monkeypatch.setenv("FAKEAWS_ORPHANS", json.dumps(orphans))
    r, calls = run(tmp_path, MIXED.replace(', batch_queue="my-fargate-queue"', ""))
    assert r.returncode == 0, r.stdout + r.stderr
    assert [c["jobId"] for c in calls if c["call"] == "terminate_job"] == ["old-1"]
    first_submit = next(i for i, c in enumerate(calls) if c["call"] == "submit_job")
    assert next(i for i, c in enumerate(calls) if c["call"] == "terminate_job") < first_submit
    wf = orphans[0]["tags"]["snakemake-workflow"]
    assert all(c["tags"]["snakemake-workflow"] == wf for c in calls if c["call"] == "submit_job")


def test_leftover_jobs_only_warned_with_warn_or_nolock(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKEAWS_ORPHANS", json.dumps([_orphan("old-1", tmp_path)]))
    for extra in (["--aws-batch-flex-orphan-jobs", "warn"], ["--nolock"]):
        r, calls = run(tmp_path, MIXED.replace(', batch_queue="my-fargate-queue"', ""), "--forceall", *extra)
        assert r.returncode == 0, r.stdout + r.stderr
        assert not any(c["call"] == "terminate_job" for c in calls)
        assert "from an earlier run of this workflow are still active" in r.stdout + r.stderr


def test_set_resources_from_the_command_line(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE, "--set-resources", "make_b:batch_queue=other-queue",
                   "make_b:mem_mb=1000")
    assert r.returncode == 0, r.stdout + r.stderr
    b = next(c for c in calls if c["call"] == "submit_job" and "make_b" in c["jobName"])
    assert b["queue"] == "other-queue"
    assert {"type": "MEMORY", "value": "1210"} in b["res"]  # 954 + 256


SHM = textwrap.dedent('''
    container: "docker://debian:12"
    rule all:
        input: "a.txt", "b.txt", "c.txt"
    rule a:
        output: "a.txt"
        resources: batch_shm_mb=2048
        shell: "echo a > {output}"
    rule b:
        output: "b.txt"
        resources: shared_memory_size_mb=2048
        shell: "echo b > {output}"
    rule c:
        output: "c.txt"
        shell: "echo c > {output}"
''')


def test_shared_memory_per_rule(tmp_path):
    r, calls = run(tmp_path, SHM)
    assert r.returncode == 0, r.stdout + r.stderr
    defs = {c["name"]: c["linux"] for c in calls if c["call"] == "register_job_definition"}
    assert sorted(map(str, defs.values())) == ["None", "{'sharedMemorySize': 2048}"]  # a and b share one
    jobdef = {c["jobName"].split("_")[1]: c["jobdef"] for c in calls if c["call"] == "submit_job"}
    assert jobdef["a"] == jobdef["b"] != jobdef["c"]


def test_shared_memory_must_be_positive(tmp_path):
    r, calls = run(tmp_path, SHM.replace("batch_shm_mb=2048", "batch_shm_mb=-1"))
    assert r.returncode != 0 and "batch_shm_mb must be a positive" in r.stdout + r.stderr


def test_failure_explains_missing_storage_plugin(tmp_path):
    smk = FAILING.replace('echo about-to-fail-marker >&2; exit 3',
                          'echo snakemake: error: unrecognized arguments: --storage-gcs-project x >&2; exit 2')
    r, _ = run(tmp_path, smk)
    assert r.returncode != 0
    assert "hint: this machine has snakemake-storage-plugin-gcs installed" in r.stdout + r.stderr


def test_startup_compares_ami_snakemake_version(tmp_path, monkeypatch):
    host = ["--aws-batch-flex-bootstrap", "host", "--aws-batch-flex-host-env-wait-seconds", "0"]
    monkeypatch.setenv("FAKEAWS_AMI_VERSION", "9.0.1")
    r, _ = run(tmp_path, FAILING, *host)
    assert "Snakemake version gap: the AMI has Snakemake 9.0.1" in r.stdout + r.stderr
    import importlib.metadata as md
    monkeypatch.setenv("FAKEAWS_AMI_VERSION", md.version("snakemake"))
    r, _ = run(tmp_path, FAILING, *host)
    assert "the AMI has Snakemake" not in r.stdout + r.stderr
