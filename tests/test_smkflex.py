"""`smkflex run`: the whole workflow runs in a head job on the fake Batch, which itself submits
the rule jobs."""
import hashlib, json, os, subprocess, sys, tarfile, textwrap
from pathlib import Path

FAKE = str(Path(__file__).parent / "fakeaws")

SNAKEFILE = textwrap.dedent('''
    rule all:
        input: "b.txt"
    rule a:
        output: "a.txt"
        shell: "echo A > {output}"
    rule b:
        input: "a.txt"
        output: "b.txt"
        shell: "cat {input} > {output}; echo B >> {output}"
''')


def setup(tmp_path, **extra):
    wf = tmp_path / "wf"
    (wf / "profile").mkdir(parents=True)
    (wf / "Snakefile").write_text(SNAKEFILE)
    (wf / ".snakemake").mkdir()
    (wf / ".snakemake" / "junk").write_text("not sent")
    cfg = {
        "executor": "aws-batch-flex",
        "default-storage-provider": "fs",
        "default-storage-prefix": str(tmp_path / "store"),
        "aws-batch-flex-queue": "default-queue",
        "aws-batch-flex-default-image": "debian:12",
        "aws-batch-flex-head-role-arn": "arn:aws:iam::1:role/head",
        "default-resources": ["mem_mb=500"],
        "latency-wait": 5,
        "jobs": 2,
        **extra,
    }
    (wf / "profile" / "config.yaml").write_text("\n".join(f"{k}: {json.dumps(v)}" for k, v in cfg.items()))
    return wf


def smkflex(wf, *args, env_extra=None):
    log = wf.parent / "calls.jsonl"
    env = dict(os.environ, PYTHONPATH=FAKE, FAKEAWS_LOG=str(log), **(env_extra or {}))
    r = subprocess.run([sys.executable, "-m", "snakemake_executor_plugin_aws_batch_flex.remote", *args],
                       cwd=wf, env=env, capture_output=True, text=True, timeout=300)
    calls = [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []
    return r, calls


def test_run_on_batch_and_rerun_with_saved_state(tmp_path):
    wf = setup(tmp_path)
    r, calls = smkflex(wf, "run", "--wait")
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    store = tmp_path / "store"
    assert (store / "b.txt").read_text() == "A\nB\n"
    head = next(c for c in calls if c["call"] == "submit_job" and c["jobName"] == "smkflex-wf")
    assert head["tags"]["smkflex-workflow"] == "wf"
    assert {"type": "MEMORY", "value": "2048"} in head["res"]
    bundles = list((store / ".smkflex" / "wf" / "runs").glob("*/workflow.tar.gz"))
    names = tarfile.open(bundles[0]).getnames()
    assert "Snakefile" in names and ".smkflex/profile/config.yaml" in names and ".smkflex/head.py" in names
    assert ".smkflex/site/snakemake_executor_plugin_aws_batch_flex/__init__.py" in names
    assert tarfile.open(bundles[0]).extractfile(".smkflex/files.txt").read().decode().split() == ["Snakefile", "profile/config.yaml"]
    assert not any(n.startswith(".snakemake") for n in names)
    state = tarfile.open(store / ".smkflex" / "wf" / "state.tar.gz").getnames()
    assert any(n.startswith(".snakemake/metadata") for n in state)
    assert "snakemake exited with 0" in out

    # second run: state comes back, nothing left to do
    r, _ = smkflex(wf, "run", "--wait")
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "restored .snakemake state" in out and "Nothing to be done" in out

    # arguments after the options go to snakemake
    r, _ = smkflex(wf, "run", "--wait", "--", "--forceall", "a.txt")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "--forceall a.txt" in r.stdout + r.stderr


def test_refuses_a_second_run_of_the_same_workflow(tmp_path):
    wf = setup(tmp_path)
    key = hashlib.sha256(f"{tmp_path / 'store'}/.smkflex/wf/state.tar.gz".encode()).hexdigest()[:16]
    running = [{"jobId": "old-head", "queue": "default-queue", "jobDefinition": "snakemake-x",
                "tags": {"smkflex-state": key}}]
    r, calls = smkflex(wf, "run", env_extra={"FAKEAWS_ORPHANS": json.dumps(running)})
    assert r.returncode == 2 and "already running as head job old-head" in r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)


def test_warns_about_a_spot_queue(tmp_path):
    wf = setup(tmp_path)
    r, calls = smkflex(wf, "run", "--queue", "my-spot-queue")
    assert r.returncode == 0, r.stderr
    assert "uses Spot instances" in r.stderr
    assert any(c["call"] == "submit_job" and c["queue"] == "my-spot-queue" for c in calls)


def test_needs_remote_storage_and_a_head_role(tmp_path):
    wf = setup(tmp_path, **{"aws-batch-flex-head-role-arn": None})
    r, _ = smkflex(wf, "run")
    assert r.returncode == 2 and "aws-batch-flex-head-role-arn" in r.stderr
    wf = setup(tmp_path / "x", **{"default-storage-provider": "local"})
    r, _ = smkflex(wf, "run")
    assert r.returncode == 2 and "needs remote storage" in r.stderr


def test_bundle_follows_gitignore(tmp_path):
    from snakemake_executor_plugin_aws_batch_flex.remote import workflow_files
    (tmp_path / "Snakefile").write_text("")
    (tmp_path / "big.bam").write_text("x")
    (tmp_path / ".gitignore").write_text("*.bam\n")
    (tmp_path / ".snakemake").mkdir()
    (tmp_path / ".snakemake" / "x").write_text("")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    assert workflow_files(str(tmp_path)) == [".gitignore", "Snakefile"]


def test_git_stand_in_lists_the_sent_files(tmp_path, monkeypatch):
    import shutil
    from snakemake_executor_plugin_aws_batch_flex import headjob
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".smkflex").mkdir()
    (tmp_path / ".smkflex" / "files.txt").write_text("Snakefile\nscripts/a.py\n")
    monkeypatch.setattr(shutil, "which", lambda name: None)
    d = headjob.git_shim()
    out = subprocess.run([os.path.join(d, "git"), "ls-files", "--recurse-submodules", "."],
                         capture_output=True, text=True)
    assert out.returncode == 0 and out.stdout == "Snakefile\nscripts/a.py\n"
    assert subprocess.run([os.path.join(d, "git"), "status"], capture_output=True).returncode != 0
    monkeypatch.undo()
    assert headjob.git_shim() is None  # a real git is used when there is one


def test_check_all_good_and_with_problems(tmp_path):
    wf = setup(tmp_path, **{"aws-batch-flex-job-role-arn": "arn:aws:iam::1:role/job"})
    r, _ = smkflex(wf, "check")
    assert r.returncode == 0, r.stdout + r.stderr
    for line in ("AWS login: arn:aws:iam::1:user/tester", "queue default-queue in eu-west-1: enabled, EC2",
                 "job role job", "head role head", "storage plugin fs installed here", ": write, read, delete"):
        assert line in r.stdout, r.stdout
    assert "All good." in r.stdout
    assert not list((tmp_path / "store" / ".smkflex").glob("check-*"))  # the probe file is removed

    wf = setup(tmp_path / "bad", **{"aws-batch-flex-queue": "no-such-queue",
                                    "aws-batch-flex-job-role-arn": "arn:aws:iam::1:role/missing-role"})
    r, _ = smkflex(wf, "check")
    assert r.returncode == 1
    assert "FAIL ] queue: Job queue 'no-such-queue' not found" in r.stdout
    assert "FAIL ] Job role 'arn:aws:iam::1:role/missing-role' does not exist." in r.stdout
    assert "2 problem(s)" in r.stdout


def test_pull(tmp_path):
    wf = setup(tmp_path)
    store = tmp_path / "store"
    for rel, text in (("results/a.txt", "A"), ("results/sub/b.txt", "BB"), ("results2/c.txt", "C"),
                      (".smkflex/wf/state.tar.gz", "x")):
        (store / rel).parent.mkdir(parents=True, exist_ok=True)
        (store / rel).write_text(text)
    r, _ = smkflex(wf, "pull")
    assert r.returncode == 2 and "In storage: results, results2" in r.stderr
    r, _ = smkflex(wf, "pull", "results")
    assert r.returncode == 0, r.stderr
    assert (wf / "results/a.txt").read_text() == "A" and (wf / "results/sub/b.txt").read_text() == "BB"
    assert not (wf / "results2").exists()
    r, _ = smkflex(wf, "pull", "results")
    assert "0 downloaded, 2 already here" in r.stdout
    r, _ = smkflex(wf, "pull", "results2/c.txt", "--to", str(tmp_path / "out"), "--dry-run")
    assert "would download results2/c.txt" in r.stdout and not (tmp_path / "out").exists()
