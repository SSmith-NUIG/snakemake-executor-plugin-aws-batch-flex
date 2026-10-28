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
    env = dict(os.environ, PYTHONPATH=FAKE, FAKEAWS_LOG=str(log),
               SMK_FLEX_CONDA_ROOT=str(wf.parent / "conda-cache"), **(env_extra or {}))
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
    # the head job's image has git (Snakemake needs it for pipelines loaded from GitHub)
    jobdefs = {c["name"]: c["image"] for c in calls if c["call"] == "register_job_definition"}
    assert jobdefs[head["jobdef"]] == "buildpack-deps:bookworm-scm"
    assert head["tags"]["smkflex-workflow"] == "wf"
    assert {"type": "MEMORY", "value": "2048"} in head["res"]
    # the head job removes the uploaded workflow when it's done
    assert not list((store / ".smkflex" / "runs").glob("*/workflow.tar.gz"))
    import io
    from snakemake_executor_plugin_aws_batch_flex.bundle import make_bundle
    bundle = io.BytesIO(make_bundle(str(wf), str(wf / "profile"), 100))
    names = tarfile.open(fileobj=bundle).getnames()
    assert "Snakefile" in names and ".smkflex/profile/config.yaml" in names and ".smkflex/head.py" in names
    assert ".smkflex/site/snakemake_executor_plugin_aws_batch_flex/__init__.py" in names
    bundle.seek(0)
    assert tarfile.open(fileobj=bundle).extractfile(".smkflex/files.txt").read().decode().split() == ["Snakefile", "profile/config.yaml"]
    assert not any(n.startswith(".snakemake") for n in names)
    state = tarfile.open(store / ".smkflex" / "state.tar.gz").getnames()
    assert any(n.startswith(".snakemake/metadata") for n in state)
    assert "snakemake exited with 0" in out
    assert "--wrapper-prefix https://github.com/snakemake/snakemake-wrappers/raw/" in out

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
    key = hashlib.sha256(f"{tmp_path / 'store'}/.smkflex/state.tar.gz".encode()).hexdigest()[:16]
    running = [{"jobId": "old-head", "queue": "default-queue", "jobDefinition": "snakemake-x",
                "tags": {"smkflex-state": key}}]
    r, calls = smkflex(wf, "run", env_extra={"FAKEAWS_ORPHANS": json.dumps(running)})
    assert r.returncode == 2 and "still going: head job old-head" in r.stderr
    assert "smkflex init" in r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)
    # one run per project (storage prefix), whatever the run is called
    r, calls = smkflex(wf, "run", "--name", "other", env_extra={"FAKEAWS_ORPHANS": json.dumps(running)})
    assert r.returncode == 2 and "old-head" in r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)
    # smkflex has no --force: it's Snakemake's and reaches it
    r, calls = smkflex(wf, "run", "--wait", "--force", "-n")
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    assert "--force -n" in out


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
    from snakemake_executor_plugin_aws_batch_flex.sources import workflow_files
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
    # made again in storage, at the same size: downloaded again
    (store / "results/a.txt").write_text("Z")
    later = (wf / "results/a.txt").stat().st_mtime + 10
    os.utime(store / "results/a.txt", (later, later))
    r, _ = smkflex(wf, "pull", "results")
    assert "1 downloaded" in r.stdout and "1 already here" in r.stdout and (wf / "results/a.txt").read_text() == "Z"
    r, _ = smkflex(wf, "pull", "results2/c.txt", "--to", str(tmp_path / "out"), "--dry-run")
    assert "would download results2/c.txt" in r.stdout and not (tmp_path / "out").exists()


def _git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd,
                          check=True, capture_output=True, text=True).stdout.strip()


def test_run_from_git(tmp_path):
    wf = setup(tmp_path)
    repo = tmp_path / "pipeline"
    (repo / "workflow").mkdir(parents=True)
    (repo / "workflow" / "Snakefile").write_text(SNAKEFILE.replace("echo B", "echo v1"))
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "v1")
    _git(repo, "tag", "v1")
    v1 = _git(repo, "rev-parse", "HEAD")
    (repo / "workflow" / "Snakefile").write_text(SNAKEFILE.replace("echo B", "echo v2"))
    _git(repo, "commit", "-q", "-am", "v2")
    url = f"file://{repo}"

    r, calls = smkflex(wf, "run", "--wait", "--git", url, "--rev", "v1", "--path", "workflow")
    assert r.returncode == 0, r.stdout + r.stderr
    assert (tmp_path / "store" / "b.txt").read_text() == "A\nv1\n"
    head = next(c for c in calls if c["call"] == "submit_job" and c["jobName"] == "smkflex-pipeline")
    assert head["tags"]["smkflex-commit"] == v1[:12] and head["tags"]["smkflex-source"] == f"{url} v1"
    assert f"Submitted workflow 'pipeline' at {v1[:12]}" in r.stdout

    # the rest of a run doesn't depend on what was cloned: a commit needs a full clone, and
    # without --rev it's the default branch (v2)
    from snakemake_executor_plugin_aws_batch_flex.bundle import clone
    assert clone(url, v1, str(tmp_path / "c1")) == v1
    assert "echo v1" in (tmp_path / "c1" / "workflow" / "Snakefile").read_text()
    assert clone(url, None, str(tmp_path / "c2")) == _git(repo, "rev-parse", "HEAD")
    assert "echo v2" in (tmp_path / "c2" / "workflow" / "Snakefile").read_text()

    r, _ = smkflex(wf, "run", "--git", url, "--rev", "no-such-tag")
    assert r.returncode == 2 and "has no branch, tag or commit 'no-such-tag'" in r.stderr
    r, _ = smkflex(wf, "run", "--rev", "v1")
    assert r.returncode == 2 and "go with --git" in r.stderr


def test_local_runs_record_the_commit(tmp_path):
    from snakemake_executor_plugin_aws_batch_flex.bundle import local_commit
    assert local_commit(str(tmp_path)) is None
    (tmp_path / "Snakefile").write_text("x")
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "x")
    sha = _git(tmp_path, "rev-parse", "HEAD")[:12]
    assert local_commit(str(tmp_path)) == sha
    # a new file is sent with the workflow, so it counts; an ignored one isn't, so it doesn't
    (tmp_path / ".gitignore").write_text("data/\n")
    _git(tmp_path, "add", ".gitignore")
    _git(tmp_path, "commit", "-q", "-m", "ignore")
    sha = _git(tmp_path, "rev-parse", "HEAD")[:12]
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "reads.fq").write_text("x")
    assert local_commit(str(tmp_path)) == sha
    (tmp_path / "rules.smk").write_text("x")
    assert local_commit(str(tmp_path)) == sha + "-modified"
    (tmp_path / "rules.smk").unlink()
    (tmp_path / "Snakefile").write_text("y")
    assert local_commit(str(tmp_path)) == sha + "-modified"


def test_git_with_local_config_added(tmp_path):
    wf = setup(tmp_path)
    repo = tmp_path / "pipe"
    (repo / "config").mkdir(parents=True)
    (repo / "Snakefile").write_text(textwrap.dedent('''
        configfile: "config/config.yaml"
        rule all:
            input: "msg.txt"
        rule msg:
            output: "msg.txt"
            shell: "echo {config[msg]} > {output}"
    '''))
    (repo / "config" / "config.yaml").write_text("msg: from-repo\n")
    (repo / "config" / "other.yaml").write_text("x: 1\n")
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "c")
    sha = _git(repo, "rev-parse", "HEAD")[:12]
    (wf / "config").mkdir()
    (wf / "config" / "config.yaml").write_text("msg: from-here\n")

    r, calls = smkflex(wf, "run", "--wait", "--git", f"file://{repo}", "--add", "config")
    assert r.returncode == 0, r.stdout + r.stderr
    assert (tmp_path / "store" / "msg.txt").read_text() == "from-here\n"
    head = next(c for c in calls if c["call"] == "submit_job" and c["jobName"] == "smkflex-pipe")
    assert head["tags"]["smkflex-commit"] == sha + "+added"
    assert "Adding 1 local file(s): config/config.yaml" in r.stderr

    r, _ = smkflex(wf, "run", "--git", f"file://{repo}", "--add", "../outside")
    assert r.returncode == 2 and "inside this folder" in r.stderr
    r, _ = smkflex(wf, "run", "--git", f"file://{repo}", "--add", "nope.yaml")
    assert r.returncode == 2 and "no such file" in r.stderr


def test_add_sends_ignored_files_and_replaces(tmp_path):
    import io
    from snakemake_executor_plugin_aws_batch_flex.bundle import added_files, make_bundle
    (tmp_path / "Snakefile").write_text("x")
    (tmp_path / ".gitignore").write_text("samples.tsv\n")
    (tmp_path / "samples.tsv").write_text("s1\n")
    (tmp_path / "profile").mkdir()
    (tmp_path / "profile" / "config.yaml").write_text("")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    t = tarfile.open(fileobj=io.BytesIO(make_bundle(str(tmp_path), str(tmp_path / "profile"), 100)))
    assert "samples.tsv" not in t.getnames()
    extra = added_files(["samples.tsv"], base=str(tmp_path))
    t = tarfile.open(fileobj=io.BytesIO(make_bundle(str(tmp_path), str(tmp_path / "profile"), 100, extra)))
    assert "samples.tsv" in t.getnames()
    assert "samples.tsv" in t.extractfile(".smkflex/files.txt").read().decode().split()


def test_push(tmp_path):
    wf = setup(tmp_path)
    (wf / "data" / "sub").mkdir(parents=True)
    (wf / "data" / "a.fq").write_text("AAA")
    (wf / "data" / "sub" / "b.fq").write_text("BB")
    r, _ = smkflex(wf, "push", "data", "--dry-run")
    assert r.returncode == 0 and "would upload data/a.fq" in r.stdout and not (tmp_path / "store" / "data").exists()
    r, _ = smkflex(wf, "push", "data")
    assert r.returncode == 0, r.stderr
    assert (tmp_path / "store" / "data" / "sub" / "b.fq").read_text() == "BB"
    r, _ = smkflex(wf, "push", "data")
    assert "0 uploaded" in r.stdout and "2 already there" in r.stdout
    # edited since, to the same size: sent again
    (wf / "data" / "a.fq").write_text("CCC")
    later = (tmp_path / "store" / "data" / "a.fq").stat().st_mtime + 10
    os.utime(wf / "data" / "a.fq", (later, later))
    r, _ = smkflex(wf, "push", "data")
    assert "1 uploaded" in r.stdout and "1 already there" in r.stdout
    assert (tmp_path / "store" / "data" / "a.fq").read_text() == "CCC"
    r, _ = smkflex(wf, "push", "../x")
    assert r.returncode == 2 and "outside the workflow folder" in r.stderr


def test_push_absolute_paths_and_many_files(tmp_path):
    # a cluster config's /shared/refs/x is <prefix>/shared/refs/x in storage, as Snakemake maps it
    wf = setup(tmp_path)
    refs = tmp_path / "shared" / "refs"
    refs.mkdir(parents=True)
    for i in range(40):
        (refs / f"chr{i}.fa").write_text(f">chr{i}\nACGT\n")
    r, _ = smkflex(wf, "push", str(refs))
    assert r.returncode == 0, r.stderr
    assert "40 uploaded" in r.stdout
    stored = tmp_path / "store" / str(refs).lstrip("/")
    assert (stored / "chr7.fa").read_text() == ">chr7\nACGT\n"
    # pull puts an absolute path under --to (here: the workflow folder), never at the real path
    (refs / "chr7.fa").unlink()
    r, _ = smkflex(wf, "pull", str(refs / "chr7.fa"))
    assert r.returncode == 0, r.stderr
    assert (wf / str(refs).lstrip("/") / "chr7.fa").exists() and not (refs / "chr7.fa").exists()


def test_profile_by_name(tmp_path):
    wf = setup(tmp_path)
    cfg_home = tmp_path / "cfg"
    (cfg_home / "snakemake" / "lab").mkdir(parents=True)
    (cfg_home / "snakemake" / "lab" / "config.yaml").write_text((wf / "profile" / "config.yaml").read_text())
    (wf / "profile" / "config.yaml").unlink()
    (wf / "profile").rmdir()
    r, _ = smkflex(wf, "check", "--profile", "lab", env_extra={"XDG_CONFIG_HOME": str(cfg_home)})
    assert f"profile {cfg_home}/snakemake/lab" in r.stdout, r.stdout + r.stderr
    # the only installed profile for this plugin is used without --profile
    r, _ = smkflex(wf, "check", env_extra={"XDG_CONFIG_HOME": str(cfg_home)})
    assert "(using profile lab" in r.stderr and f"profile {cfg_home}/snakemake/lab" in r.stdout
    # two of them: say which exist instead of guessing
    (cfg_home / "snakemake" / "lab2").mkdir()
    (cfg_home / "snakemake" / "lab2" / "config.yaml").write_text("executor: aws-batch-flex\n")
    r, _ = smkflex(wf, "check", env_extra={"XDG_CONFIG_HOME": str(cfg_home)})
    assert "2 profiles for this plugin" in r.stdout + r.stderr and "lab, lab2" in r.stdout + r.stderr


def test_plain_folder_follows_its_gitignore(tmp_path):
    from snakemake_executor_plugin_aws_batch_flex.sources import workflow_files
    for f in ("workflow/Snakefile", "config/config.yaml", "data/a.fq", "x.bam", "sub/y.bam", "results/r.txt"):
        (tmp_path / f).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / f).write_text("")
    (tmp_path / ".gitignore").write_text("# data stays here\ndata/\n*.bam\n/results\n")
    assert workflow_files(str(tmp_path)) == [".gitignore", "config/config.yaml", "workflow/Snakefile"]


def test_workflow_profile_sets_the_project_prefix(tmp_path):
    wf = setup(tmp_path)
    (wf / "workflow" / "profiles" / "default").mkdir(parents=True)
    (wf / "workflow" / "Snakefile").write_text("")  # snakedeploy's layout
    (wf / "workflow" / "profiles" / "default" / "config.yaml").write_text(
        f"default-storage-prefix: {tmp_path / 'store' / 'proj'}\n")
    (wf / "data").mkdir()
    (wf / "data" / "a.fq").write_text("A")
    r, _ = smkflex(wf, "push", "data")
    assert r.returncode == 0, r.stderr
    assert (tmp_path / "store" / "proj" / "data" / "a.fq").read_text() == "A"


def test_init(tmp_path):
    wf = setup(tmp_path)
    (wf / ".gitignore").write_text("*.bam")
    r, _ = smkflex(wf, "init")
    assert r.returncode == 0, r.stderr
    store = tmp_path / "store"
    wp = (wf / "profiles" / "default" / "config.yaml").read_text()
    assert wp == f"default-storage-prefix: {store}/wf\n"
    gi = (wf / ".gitignore").read_text()
    assert gi.startswith("*.bam\n") and "data/" in gi and ".snakemake/" in gi
    # again: nothing changes
    r, _ = smkflex(wf, "init")
    assert "already has its folder in storage" in r.stdout
    assert (wf / ".gitignore").read_text() == gi
    # push now goes to the project's folder
    (wf / "data").mkdir()
    (wf / "data" / "x").write_text("x")
    r, _ = smkflex(wf, "push", "data")
    assert (store / "wf" / "data" / "x").exists(), r.stdout + r.stderr


def test_init_uses_the_pipelines_own_workflow_profile(tmp_path):
    # Snakemake reads only one workflow profile: add to the pipeline's instead of shadowing it
    wf = setup(tmp_path)
    own = wf / "workflow" / "profiles" / "default"
    own.mkdir(parents=True)
    (wf / "workflow" / "Snakefile").write_text("")
    (own / "config.yaml").write_text("set-threads: {align: 4}\n")
    r, _ = smkflex(wf, "init")
    assert r.returncode == 0, r.stderr
    assert (own / "config.yaml").read_text().startswith("set-threads: {align: 4}\ndefault-storage-prefix: ")
    assert not (wf / "profiles").exists()


def test_snakemake_reads_the_profile_init_writes(tmp_path):
    # the same file must be found by Snakemake itself, for both layouts
    for layout in ("Snakefile", "workflow/Snakefile"):
        wf = tmp_path / layout.replace("/", "_")
        (wf / layout).parent.mkdir(parents=True, exist_ok=True)
        (wf / layout).write_text('rule all:\n    run:\n        print("PREFIX", workflow.storage_settings.default_storage_prefix)\n')
        (wf / "profiles" / "default").mkdir(parents=True)
        (wf / "profiles" / "default" / "config.yaml").write_text("default-storage-prefix: s3://b/proj\ncores: 1\n")
        r = subprocess.run([sys.executable, "-m", "snakemake", "-n"], cwd=wf, capture_output=True, text=True)
        assert "workflow specific profile profiles/default" in r.stdout + r.stderr, (layout, r.stdout + r.stderr)


def test_push_says_when_run_would_send_the_data_too(tmp_path):
    wf = setup(tmp_path)
    (wf / "reads").mkdir()
    (wf / "reads" / "S1.fq").write_text("@r\nACGT\n+\nIIII\n")
    r, _ = smkflex(wf, "push", "reads")
    assert r.returncode == 0, r.stderr
    assert "smkflex run would also send reads" in r.stdout and "reads/" in r.stdout.splitlines()[-1]
    (wf / ".gitignore").write_text("reads/\n")
    r, _ = smkflex(wf, "push", "reads", "--force")
    assert "would also send" not in r.stdout


def test_logs_and_cancel_take_the_region_from_the_installed_profile(tmp_path, monkeypatch):
    from snakemake_executor_plugin_aws_batch_flex import remote

    prof = tmp_path / "config" / "snakemake" / "smkflex"
    prof.mkdir(parents=True)
    (prof / "config.yaml").write_text("executor: aws-batch-flex\naws-batch-flex-region: eu-west-1\n")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("SNAKEMAKE_PROFILE", raising=False)
    monkeypatch.chdir(tmp_path)
    seen = []
    monkeypatch.setattr(remote, "_clients", lambda p: seen.append(p.get("aws-batch-flex-region")) or (None, None, None))
    monkeypatch.setattr(remote, "follow", lambda *a: 0)
    assert remote.main(["logs", "J1"]) == 0
    assert remote.main(["logs", "J1", "--region", "us-east-1"]) == 0
    assert seen == ["eu-west-1", "us-east-1"]
    # no profile at all: the AWS config's region
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "none"))
    assert remote.main(["logs", "J1"]) == 0 and seen[-1] is None


def test_status_reads_every_page_and_describes_100_at_a_time(tmp_path, monkeypatch, capsys):
    from snakemake_executor_plugin_aws_batch_flex import remote

    jobs = [{"jobId": f"j{i}", "jobName": f"smkflex-wf{i}", "status": "SUCCEEDED", "createdAt": i * 1000}
            for i in range(250)]

    class Batch:
        def list_jobs(self, nextToken=None, **kw):
            start = int(nextToken or 0)
            page = {"jobSummaryList": jobs[start:start + 100]}
            if start + 100 < len(jobs):
                page["nextToken"] = str(start + 100)
            return page

        def describe_jobs(self, jobs):
            assert len(jobs) <= 100
            return {"jobs": [{"jobId": j, "tags": {"smkflex-commit": "abc"}} for j in jobs]}

    monkeypatch.setattr(remote, "_load_profile", lambda p: ("p", {"aws-batch-flex-queue": "q"}))
    monkeypatch.setattr(remote, "_clients", lambda p: (Batch(), None, None))
    assert remote.main(["status", "--limit", "150"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 150 and out[0].startswith("j249 ") and "abc" in out[0]


def test_a_bad_setting_in_the_profile_is_reported_not_a_traceback(tmp_path):
    wf = setup(tmp_path, **{"aws-batch-flex-bootstrap": "sometimes"})
    r, calls = smkflex(wf, "run")
    assert r.returncode == 2 and "smkflex: --aws-batch-flex-bootstrap must be" in r.stderr, r.stderr
    assert "Traceback" not in r.stderr and not any(c["call"] == "submit_job" for c in calls)


def test_run_refuses_options_nobody_knows(tmp_path):
    wf = setup(tmp_path)
    r, calls = smkflex(wf, "run", "--wiat")
    assert r.returncode == 2 and "--wiat" in r.stderr, r.stderr
    assert not any(c["call"] == "submit_job" for c in calls)


def test_head_job_saves_state_while_running_and_when_stopped(tmp_path, monkeypatch):
    import time
    from snakemake_executor_plugin_aws_batch_flex import headjob
    monkeypatch.chdir(tmp_path)
    url = str(tmp_path / "store" / "state.tar.gz")
    uploads = []
    put = headjob.put
    monkeypatch.setattr(headjob, "put", lambda u, data: (uploads.append(u), put(u, data)))
    (tmp_path / ".snakemake" / "metadata").mkdir(parents=True)
    (tmp_path / ".snakemake" / "metadata" / "a").write_text("{}")

    def wait_for(n):
        deadline = time.time() + 10
        while len(uploads) < n and time.time() < deadline:
            time.sleep(0.02)
        return len(uploads)

    # every `every` seconds while Snakemake runs; an unchanged state isn't uploaded again
    saver = headjob.StateSaver(url, every=0.05)
    assert wait_for(1) == 1
    time.sleep(0.3)
    assert len(uploads) == 1
    saver.finish()

    # at once when the head job is told to stop (SIGTERM), and at the end
    saver = headjob.StateSaver(url, every=3600)
    (tmp_path / ".snakemake" / "incomplete").mkdir()
    (tmp_path / ".snakemake" / "incomplete" / "b").write_text("{}")
    saver.wake.set()
    assert wait_for(2) == 2
    (tmp_path / ".snakemake" / "metadata" / "c").write_text("{}")
    assert saver.finish() and len(uploads) == 3
    assert not saver.thread.is_alive()
    assert sorted(tarfile.open(url).getnames()) == [
        ".snakemake/incomplete/b", ".snakemake/metadata/a", ".snakemake/metadata/c"]
