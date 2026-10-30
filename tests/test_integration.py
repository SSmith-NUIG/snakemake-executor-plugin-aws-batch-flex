"""Runs a real Snakemake workflow through the plugin against a fake AWS Batch."""
import json, os, shutil, subprocess, sys, textwrap
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
# The tool's own error in the rule's log: file, as published pipelines write it (2> {log})
LOGGED = FAILING.replace('    shell: "echo', '''    log: "logs/boom.log"
    shell: "echo '[E::bwa_idx_load_from_disk] fail to locate the index files' > {log}; echo''')
assert LOGGED != FAILING


def run(tmp_path, snakefile, *extra):
    (tmp_path / "Snakefile").write_text(snakefile)
    log = tmp_path / "calls.jsonl"
    env = dict(os.environ, PYTHONPATH=FAKE, FAKEAWS_LOG=str(log),
               SMK_FLEX_CONDA_ROOT=str(tmp_path / "conda-cache"))  # the fake jobs' "instance" folder
    cmd = [sys.executable, "-m", "snakemake", "--executor", "aws-batch-flex",
           "--aws-batch-flex-queue", "default-queue", "--aws-batch-flex-default-image", "debian:12",
           "--default-storage-provider", "fs", "--default-storage-prefix", str(tmp_path / "store"),
           "--default-resources", "mem_mb=500", "-j", "4", "--latency-wait", "5",
           "--seconds-between-status-checks", "1", *extra]
    r = subprocess.run(cmd, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=300)
    calls = [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []
    return r, calls


def test_per_rule_images_queues_and_resources(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE)
    assert r.returncode == 0, r.stdout + r.stderr
    # every workaround fits this Snakemake (CI runs the newest weekly: this fails when one stops)
    assert "workaround is off" not in r.stdout + r.stderr
    summary = (r.stdout + r.stderr).split("AWS Batch usage of this run")[1]
    assert "make_a" in summary and "make_b" in summary and "total" in summary
    # what was used, not what it cost (that's AWS Billing's, by the jobs' tags)
    assert "AWS Billing" in summary and "USD" not in summary
    assert (tmp_path / "store" / "b.txt").read_text() == "A\nB\n"
    images = {c["image"] for c in calls if c["call"] == "register_job_definition"}
    assert images == {"ubuntu:24.04", "quay.io/biocontainers/bwa:0.7.17"}
    a = next(c for c in calls if c["call"] == "submit_job" and c["queue"] == "special-queue")
    assert {"type": "VCPU", "value": "4"} in a["res"]
    # Snakemake derives mem_mib 2862 from mem_mb 3000; plus the default 256 MiB overhead
    assert {"type": "MEMORY", "value": "3118"} in a["res"]
    assert sum(c["queue"] == "default-queue" for c in calls if c["call"] == "submit_job") >= 1


def test_failure_surfaces_log_tail(tmp_path):
    r, _ = run(tmp_path, LOGGED)
    assert r.returncode != 0
    out = r.stdout + r.stderr
    assert "FAILED" in out and "exit code: 1" in out and "exit status 3" in out
    assert "about-to-fail-marker" in out  # log tail was fetched and shown
    # and the end of the rule's log: file, from storage, with what it means
    assert "end of log file" in out and "logs/boom.log" in out and "fail to locate the index files" in out
    assert "multiext(" in out
    assert not list(tmp_path.glob(".snakemake/storage/**/boom.log"))  # read, not left behind


def test_failed_group_job_shows_why(tmp_path):
    # Snakemake's report of a failed group job leaves out the executor's message
    snakefile = textwrap.dedent('''
        rule all:
            input: "b.txt"
        rule a:
            output: "a.txt"
            group: "g"
            shell: "touch {output}"
        rule b:
            input: "a.txt"
            output: "b.txt"
            group: "g"
            shell: "echo group-member-failed-marker >&2; exit 3"
    ''')
    r, calls = run(tmp_path, snakefile)
    out = r.stdout + r.stderr
    assert r.returncode != 0 and sum(c["call"] == "submit_job" for c in calls) == 1
    assert "Error in group job" in out and "exit code: 1" in out and "group-member-failed-marker" in out


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
    # ECS allows 8192 characters for a job's overrides (command and environment, as JSON);
    # this is install-at-start, the longest start-up script, so keep a margin for long targets
    sub = [c for c in _ if c["call"] == "submit_job"][-1]
    overrides = json.dumps({"command": ["/bin/sh", "-c", cmd.read_text()],
                            "environment": [{"name": k, "value": v} for k, v in sub["env"].items()
                                            if k.startswith(("SMK_", "SNAKEMAKE"))]})
    assert len(overrides) < 7400, len(overrides)


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



DISK = textwrap.dedent('''
    container: "docker://debian:12"
    rule all:
        input: "a.txt", "b.txt", "c.txt", "d.txt"
    rule a:
        output: "a.txt"
        resources: disk_mb=60000, batch_queue="my-fargate-queue"
        shell: "echo a > {output}"
    rule b:
        output: "b.txt"
        resources: disk_mb=500000, batch_queue="my-fargate-queue"
        shell: "echo b > {output}"
    rule c:
        output: "c.txt"
        resources: batch_queue="my-fargate-queue"
        shell: "echo c > {output}"
    rule d:
        output: "d.txt"
        resources: disk_mb=50000
        shell: "echo d > {output}"
''')


def test_fargate_disk_from_disk_mb(tmp_path):
    r, calls = run(tmp_path, DISK, "--aws-batch-flex-execution-role-arn", "arn:aws:iam::1:role/exec")
    assert r.returncode == 0, r.stdout + r.stderr
    defs = {c["name"]: c for c in calls if c["call"] == "register_job_definition"}
    disk = {c["jobName"].split("_")[1]: defs[c["jobdef"]]["disk"] for c in calls if c["call"] == "submit_job"}
    # 60000 MB = 56 GiB, rounded up to 75; 500000 is capped at 200; c gets Snakemake's
    # placeholder 50000 for rules without inputs, which is ignored; d runs on EC2
    assert disk == {"a": {"sizeInGiB": 75}, "b": {"sizeInGiB": 200}, "c": None, "d": None}
    assert "Fargate allows at most 200" in r.stdout + r.stderr


def test_own_log_group(tmp_path):
    r, calls = run(tmp_path, FAILING.replace("echo about-to-fail-marker >&2; exit 3", "touch {output}"),
                   "--aws-batch-flex-log-group", "/aws/batch/smkflex")
    assert r.returncode == 0, r.stdout + r.stderr
    logs = [c["logs"] for c in calls if c["call"] == "register_job_definition"]
    assert logs == [{"logDriver": "awslogs",
                     "options": {"awslogs-group": "/aws/batch/smkflex", "awslogs-region": "eu-west-1"}}]


OOM = textwrap.dedent('''
    rule all:
        input: "big.txt", "fargate.txt"
    rule oom_big:
        output: "big.txt"
        resources: mem_mb=1000
        shell: "echo ok > {output}"
    rule oom_fargate:
        output: "fargate.txt"
        resources: mem_mb=1000, batch_queue="my-fargate-queue"
        shell: "echo ok > {output}"
''')


def _mem(calls, rule):
    return [next(int(x["value"]) for x in c["res"] if x["type"] == "MEMORY")
            for c in calls if c["call"] == "submit_job" and f"_{rule}_" in c["jobName"]]


def test_out_of_memory_retries_with_more(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKEAWS_OOM_BELOW_MIB", "3000")
    r, calls = run(tmp_path, OOM, "--retries", "2", "--aws-batch-flex-execution-role-arn", "arn:aws:iam::1:role/x")
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert _mem(calls, "oom_big") == [1210, 2420, 4840]  # 954 + 256, then twice, then 4 times
    # Fargate: 1210 -> 2048 (the smallest size for 1 vCPU), then twice what it got
    assert _mem(calls, "oom_fargate") == [2048, 4096]
    assert "ran out of memory with 1210 MiB; retrying with 2420 MiB" in out
    assert "Snakemake retries it with more" in out


def test_out_of_memory_rule_that_scales_itself_isnt_scaled_again(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKEAWS_OOM_BELOW_MIB", "3000")
    snakefile = textwrap.dedent('''
        rule oom_scaled:
            output: "scaled.txt"
            resources: mem_mb=lambda wildcards, attempt: 1000 * attempt
            shell: "echo ok > {output}"
    ''')
    r, calls = run(tmp_path, snakefile, "--retries", "2")
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    first, second, third = _mem(calls, "oom_scaled")
    assert first == 1210 and 2000 < second < 2420 and 3000 < third < 4840  # the rule's, not doubled too
    assert f"its rule asks for {second} MiB this time" in out


def test_out_of_memory_without_retries_says_how(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKEAWS_OOM_BELOW_MIB", "3000")
    r, calls = run(tmp_path, OOM.replace(', "fargate.txt"', ""))
    assert r.returncode != 0
    assert _mem(calls, "oom_big") == [1210]
    assert "run with --retries 2" in r.stdout + r.stderr


UNUSED_TEMP = textwrap.dedent('''
    rule all:
        input: "x.bed"
    rule make:
        output: bed="x.bed", db=temp("x.db")
        shell: "echo db > {output.db}; echo bed > {output.bed}"
''')


def test_temp_output_nobody_uses(tmp_path):
    # Snakemake tells the job to delete x.db (--unneeded-temp-files) but then waits for it in
    # storage; the plugin drops that instruction, so the job uploads it and Snakemake removes it
    r, calls = run(tmp_path, UNUSED_TEMP)
    assert r.returncode == 0, r.stdout + r.stderr
    assert (tmp_path / "store" / "x.bed").read_text() == "bed\n"
    assert not (tmp_path / "store" / "x.db").exists()
    assert not any("--unneeded-temp-files" in c["cmdtail"] for c in calls if c["call"] == "submit_job")


FOLDER = textwrap.dedent('''
    from lib.util import X
    S = open("samples.tsv").read().split()
    rule all:
        input: "a.txt", "b.txt", "c.txt", expand("{s}.txt", s=S)
    rule a:
        output: "a.txt"
        shell: "python3 src/tool.py > {output}"
    rule b:
        output: "b.txt"
        shell: "sh helper.sh > {output}"
    rule c:
        output: "c.txt"
        params: sheet="config/private.tsv"
        shell: "echo {X} $(cat {params.sheet}) $(test -e notes.secret && echo SENT || echo kept) > {output}"
    rule s:
        output: "{s}.txt"
        wildcard_constraints: s="s[0-9]"
        shell: "echo {wildcards.s} > {output}"
''')


def test_jobs_get_the_workflow_folder_without_git(tmp_path):
    # what works locally: a shell: command runs a script from anywhere in the folder, the
    # Snakefile imports a module of its own and reads a sheet at the top; none of it in git
    files = {"src/tool.py": "print('tool')", "helper.sh": "echo helper", "lib/__init__.py": "",
             "lib/util.py": "X = 'lib'", "samples.tsv": "s1 s2\n", "config/private.tsv": "sheet",
             "notes.secret": "x", ".gitignore": "*.secret\nconfig/\n",
             # data and results are read from and written to storage, never sent; not even when
             # .gitignore doesn't list them, or a local copy of the results is big
             "data/reads.fq": "x" * 2_000_000, "results/old.txt": "x" * 2_000_000}
    for p, text in files.items():
        (tmp_path / p).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / p).write_text(text)
    r, calls = run(tmp_path, FOLDER, "--aws-batch-flex-max-sources-mb", "1")
    assert r.returncode == 0, r.stdout + r.stderr
    store = tmp_path / "store"
    assert (store / "a.txt").read_text() == "tool\n" and (store / "b.txt").read_text() == "helper\n"
    # config/ goes along even though .gitignore lists it; other ignored files don't
    assert (store / "c.txt").read_text() == "lib sheet kept\n"
    assert (store / "s2.txt").read_text() == "s2\n"

    # data left in another folder: stop before anything is submitted, and say where it is
    (tmp_path / "refs").mkdir()
    (tmp_path / "refs" / "genome.fa").write_bytes(b"x" * 2_000_000)
    (tmp_path / "calls.jsonl").unlink()
    r, calls = run(tmp_path, FOLDER, "--forceall", "--aws-batch-flex-max-sources-mb", "1")
    assert r.returncode != 0 and "refs/ 2 MB" in r.stdout + r.stderr and ".gitignore" in r.stdout + r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)

    # a file git tracks that was deleted here, not committed yet: left out, not a crash
    shutil.rmtree(tmp_path / "refs")
    (tmp_path / "old.txt").write_text("x")
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@t"]
    for args in (["init", "-q"], ["add", "-f", "old.txt", "Snakefile"], ["commit", "-qm", "x"]):
        subprocess.run(git + args, cwd=tmp_path, check=True)
    (tmp_path / "old.txt").unlink()
    r, _ = run(tmp_path, FOLDER, "--forceall", "--aws-batch-flex-max-sources-mb", "1")
    assert r.returncode == 0, r.stdout + r.stderr


def test_input_nowhere_says_where_jobs_read_it(tmp_path):
    snakefile = textwrap.dedent('''
        rule a:
            input: "reads/x.fq"
            output: "a.txt"
            shell: "cat {input} > {output}"
    ''')
    r, _ = run(tmp_path, snakefile)
    out = r.stdout + r.stderr
    assert r.returncode != 0 and "Jobs read their inputs from" in out and "smkflex push" in out, out


def test_input_only_on_this_machine_says_how_to_upload_it(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "x.txt").write_text("local\n")
    (tmp_path / "abs").mkdir()
    (tmp_path / "abs" / "y.txt").write_text("abs\n")
    snakefile = textwrap.dedent(f'''
        rule a:
            input: "data/x.txt", "{tmp_path}/abs/y.txt"
            output: "a.txt"
            shell: "cat {{input}} > {{output}}"
    ''')
    r, _ = run(tmp_path, snakefile)
    out = r.stdout + r.stderr
    assert r.returncode != 0 and "On this machine but not in storage" in out, out
    assert f"smkflex push {tmp_path}/abs data" in out
    # what smkflex push does: the same paths under the prefix (an absolute one without its /)
    store = tmp_path / "store"
    (store / "data").mkdir(parents=True)
    (store / "data" / "x.txt").write_text("local\n")
    (store / str(tmp_path).lstrip("/") / "abs").mkdir(parents=True)
    (store / str(tmp_path).lstrip("/") / "abs" / "y.txt").write_text("abs\n")
    r, _ = run(tmp_path, snakefile)
    assert r.returncode == 0, r.stdout + r.stderr
    assert (store / "a.txt").read_text() == "local\nabs\n"


def test_workdir_directive_stops_before_submitting(tmp_path):
    snakefile = 'workdir: "sub"\nrule a:\n    output: "a.txt"\n    shell: "echo ok > {output}"\n'
    r, calls = run(tmp_path, snakefile)
    out = r.stdout + r.stderr
    assert r.returncode != 0 and "is outside the folder Snakemake works in" in out and "Remove workdir:" in out, out
    assert not any(c["call"] == "submit_job" for c in calls)


def test_job_commands_fit_what_ecs_allows(tmp_path):
    # installing Snakemake at job start, with the prerequisites, makes the longest commands
    r, calls = run(tmp_path, SNAKEFILE, "--aws-batch-flex-bootstrap", "always", "--aws-batch-flex-install-prereqs")
    assert r.returncode == 0, r.stdout + r.stderr
    sizes = [c["overrides_chars"] for c in calls if c["call"] == "submit_job"]
    assert sizes and max(sizes) < 8192 - 500, sizes  # room for a longer storage prefix and the like
    # too long: stopped before it reaches AWS, saying what makes it long
    (tmp_path / "calls.jsonl").unlink()
    many = [f"some-long-package-name-{i}==1.0" for i in range(150)]
    (tmp_path / "x").mkdir()
    r, calls = run(tmp_path / "x", SNAKEFILE, "--aws-batch-flex-bootstrap", "always",
                   "--aws-batch-flex-extra-packages", *many)
    assert r.returncode != 0 and "more than the 8192 ECS allows per job" in r.stdout + r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)


def test_module_from_github(tmp_path):
    # snakedeploy's layout: the pipeline's rules come from GitHub; packing the sources used to
    # crash on their scripts ("not GithubFile")
    import pytest
    if not os.environ.get("RUN_NETWORK_TESTS"):
        pytest.skip("needs GitHub (RUN_NETWORK_TESTS=1)")
    pytest.importorskip("pandas")
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text(textwrap.dedent('''
        samples: config/samples.tsv
        units: config/units.tsv
        ref: {species: saccharomyces_cerevisiae, release: 114, build: R64-1-1}
        trimming: {activate: false}
        mergeReads: {activate: false}
        pca: {activate: false, labels: [condition]}
        diffexp:
          variables_of_interest: {condition: {base_level: untreated}}
          batch_effects: ""
          contrasts: {t: {variable_of_interest: condition, level_of_interest: treated}}
          model: ~condition
        params: {star: {index: "", align: ""}}
    '''))
    (tmp_path / "config" / "samples.tsv").write_text("sample_name\tcondition\nA\ttreated\nB\tuntreated\n")
    (tmp_path / "config" / "units.tsv").write_text(
        "sample_name\tunit_name\tfq1\tfq2\tsra\tadapters\tstrandedness\n"
        "A\t1\ta.fq\t\t\t\tnone\nB\t1\tb.fq\t\t\t\tnone\n")
    smk = textwrap.dedent('''
        configfile: "config/config.yaml"
        module rnaseq:
            snakefile: github("snakemake-workflows/rna-seq-star-deseq2", path="workflow/Snakefile", tag="v3.1.2")
            config: config
        use rule * from rnaseq
    ''')
    r, calls = run(tmp_path, smk, "resources/genome.gtf", "--keep-going")
    assert "GithubFile" not in r.stdout + r.stderr, r.stdout + r.stderr
    assert any(c["call"] == "submit_job" for c in calls), r.stdout + r.stderr


def test_peak_memory_in_the_table(tmp_path, monkeypatch):
    stat = tmp_path / "memory.stat"  # every fake job "used" 1 GiB (plus cache, which doesn't count)
    stat.write_text(f"anon {768 * 1024 * 1024}\nfile {3 * 1024 ** 3}\nshmem {256 * 1024 * 1024}\n")
    monkeypatch.setenv("SMK_FLEX_MEMSTAT_FILE", str(stat))
    cpu = tmp_path / "cpu.stat"  # a CPU counter that doesn't move
    cpu.write_text("usage_usec 1000000\n")
    monkeypatch.setenv("SMK_FLEX_CPUSTAT_FILE", str(cpu))
    smk = SNAKEFILE.replace("resources: mem_mb=3000", "resources: mem_mb=8000")
    r, _ = run(tmp_path, smk)
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    usage = out.split("AWS Batch usage of this run")[1]
    row = next(l for l in usage.splitlines() if l.strip().startswith("make_a "))
    cols = row.split()
    # vCPUs 4, busy (from the fake cpu.stat: unchanged, so 0), mem 8000 MB -> 7630 MiB + 256, peak 1 GiB
    assert cols[4:8] == ["4", "0.0", "7886", "1024"], row
    assert "set-resources:\n  make_a:\n    mem_mb: 1200    # job gets 1400 MiB; had 7886, peak 1024" in out


def test_conda_cache_is_set_up_for_jobs(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE)
    assert r.returncode == 0, r.stdout + r.stderr
    sub = next(c for c in calls if c["call"] == "submit_job")
    assert sub["env"]["SMK_FLEX_CONDA_CACHE"] == "instance"
    assert sub["env"]["AWS_REGION"] == sub["env"]["AWS_DEFAULT_REGION"] == "eu-west-1"
    code = sub["env"]["SMK_FLEX_JOB_CODE"]
    assert code.startswith(str(tmp_path / "store" / ".smkflex" / "code" / "jobtools-"))
    # (the launcher that turns it on in the job is tested in test_condacache.py: these fake jobs
    # find Snakemake on PATH, so they don't use it)
    jd = next(c for c in calls if c["call"] == "register_job_definition")
    assert any(m["containerPath"] == "/tmp/smkflex-conda" and not m["readOnly"] for m in jd["mounts"])

    (tmp_path / "off").mkdir()
    (tmp_path / "bad").mkdir()
    r, calls = run(tmp_path / "off", SNAKEFILE, "--aws-batch-flex-conda-cache", "none")
    assert r.returncode == 0, r.stdout + r.stderr
    sub = next(c for c in calls if c["call"] == "submit_job")
    assert "SMK_FLEX_CONDA_CACHE" not in sub["env"] and "SMK_FLEX_JOB_CODE" in sub["env"]
    assert sub["env"]["CONDA_CHANNEL_PRIORITY"] == "strict"  # as bioconda needs, whatever the image's conda says

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


def _jobdef(name, rev=1, image="debian:12", mounts=(), platform="EC2"):
    return {"jobDefinitionName": name, "revision": rev, "status": "ACTIVE", "type": "container",
            "jobDefinitionArn": f"arn:aws:batch:eu-west-1:1:job-definition/{name}:{rev}",
            "platformCapabilities": [platform],
            "containerProperties": {"image": image, "mountPoints": [{"containerPath": m} for m in mounts]}}


def test_existing_job_definition_global_and_per_rule(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKEAWS_JOBDEFS", json.dumps([_jobdef("team-def", 1), _jobdef("team-def", 2, "ubuntu:24.04"),
                                                      _jobdef("special", 5, "debian:12-slim")]))
    smk = SNAKEFILE.replace('resources: mem_mb=3000, batch_queue="special-queue"',
                            'resources: mem_mb=3000, batch_job_definition="special:5"')
    r, calls = run(tmp_path, smk, "--aws-batch-flex-job-definition", "team-def")
    assert r.returncode == 0, r.stdout + r.stderr
    assert not any(c["call"] == "register_job_definition" for c in calls)
    jobdefs = sorted(c["jobdef"].rsplit("/", 1)[1] for c in calls if c["call"] == "submit_job")
    assert jobdefs == ["special:5", "team-def:2"]          # name alone -> latest revision
    out = r.stdout + r.stderr
    assert "its image ubuntu:24.04 is used, not quay.io/biocontainers/bwa:0.7.17" in out
    mem = next(c for c in calls if c["call"] == "submit_job" and c["jobdef"].endswith("special:5"))
    assert {"type": "MEMORY", "value": "3118"} in mem["res"]  # rule resources still apply


def test_existing_job_definition_without_host_mount_installs_at_start(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKEAWS_JOBDEFS", json.dumps([_jobdef("plain"), _jobdef("mounted", mounts=["/opt/snakemake-env"])]))
    cmd = tmp_path / "cmd.txt"
    monkeypatch.setenv("FAKEAWS_CMD", str(cmd))
    host = ["--aws-batch-flex-bootstrap", "host", "--aws-batch-flex-host-env-wait-seconds", "0"]
    r, _ = run(tmp_path, FAILING, "--aws-batch-flex-job-definition", "plain", *host)
    assert "doesn't mount /opt/snakemake-env, so Snakemake is installed at job start" in r.stdout + r.stderr
    assert "bootstrapping Snakemake" in cmd.read_text()
    r, _ = run(tmp_path, FAILING, "--aws-batch-flex-job-definition", "mounted", *host)
    assert "SMK_ENV=/opt/snakemake-env" in cmd.read_text()


def test_existing_job_definition_errors(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKEAWS_JOBDEFS", json.dumps([_jobdef("fg", platform="FARGATE")]))
    r, calls = run(tmp_path, FAILING, "--aws-batch-flex-job-definition", "nope")
    assert r.returncode != 0 and "job definition 'nope' not found" in r.stdout + r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)
    r, calls = run(tmp_path, FAILING, "--aws-batch-flex-job-definition", "fg")
    assert r.returncode != 0 and "is for FARGATE but queue default-queue runs on EC2" in r.stdout + r.stderr


PRIO = textwrap.dedent('''
    container: "docker://debian:12"
    rule all:
        input: "a.txt", "b.txt", "c.txt"
    rule a:
        output: "a.txt"
        resources: batch_priority=500, batch_spot_retries=0
        shell: "echo a > {output}"
    rule b:
        output: "b.txt"
        resources: aws_batch_scheduling_priority=7, aws_batch_spot_attempts=4
        shell: "echo b > {output}"
    rule c:
        output: "c.txt"
        shell: "echo c > {output}"
''')


def _by_rule(calls, key):
    return {c["jobName"].split("_")[1]: c[key] for c in calls if c["call"] == "submit_job"}


def test_priority_and_spot_retries_per_rule(tmp_path):
    r, calls = run(tmp_path, PRIO, "--aws-batch-flex-queue", "fair-queue", "--aws-batch-flex-scheduling-priority", "10")
    assert r.returncode == 0, r.stdout + r.stderr
    assert _by_rule(calls, "priority") == {"a": 500, "b": 7, "c": 10}
    attempts = {k: (v or {}).get("attempts") for k, v in _by_rule(calls, "retry").items()}
    assert attempts == {"a": None, "b": 4, "c": 3}       # 0 retries -> no strategy; default 2 -> 3 attempts


def test_priority_skipped_without_fair_share_and_validated(tmp_path):
    r, calls = run(tmp_path, PRIO)
    assert r.returncode == 0, r.stdout + r.stderr
    assert set(_by_rule(calls, "priority").values()) == {None}
    assert "has no fair-share scheduling policy" in r.stdout + r.stderr
    r, calls = run(tmp_path, PRIO.replace("batch_priority=500", "batch_priority=99999"), "--forceall",
                   "--aws-batch-flex-queue", "fair-queue")
    assert r.returncode != 0 and "batch_priority must be a whole number from 0 to 9999" in r.stdout + r.stderr
