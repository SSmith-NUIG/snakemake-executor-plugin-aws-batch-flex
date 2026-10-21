"""scripts/setup.sh with a stub `aws`: the checks before anything is created."""
import os, stat, subprocess, textwrap
from pathlib import Path

SCRIPT = Path(__file__).parent.parent / "scripts" / "setup.sh"

STUB = textwrap.dedent('''\\
    #!/usr/bin/env bash
    echo "$*" >> "$AWS_STUB_LOG"
    case "$*" in
      *get-caller-identity*Arn*) echo arn:aws:iam::123456789012:user/me;;
      *get-caller-identity*Account*) echo 123456789012;;
      *describe-job-queues*) echo "${QUEUE:-None}";;
      *head-bucket*) [ -n "$BUCKET_EXISTS" ];;
      *describe-subnets*) echo "${SUBNETS-subnet-a subnet-b}";;
      *) true;;
    esac
''')


def run(tmp_path, *args, **env_extra):
    stub = tmp_path / "aws"; stub.write_text(STUB); stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    # Terraform isn't needed before anything is created (and isn't installed in CI)
    tf = tmp_path / "terraform"; tf.write_text("#!/bin/sh\nexit 0\n"); tf.chmod(0o755)
    log = tmp_path / "calls.log"
    env = dict(os.environ, AWS=str(stub), TERRAFORM=str(tf), AWS_STUB_LOG=str(log), **env_extra)
    r = subprocess.run(["bash", str(SCRIPT), "--region", "eu-west-1", "--tf-dir", str(tmp_path / "tf"),
                        "--workflow-dir", str(tmp_path / "wf"), "--dry-run", *args],
                       env=env, capture_output=True, text=True, cwd=tmp_path)
    return r, log.read_text() if log.exists() else ""


def test_dry_run_shows_the_plan_and_creates_nothing(tmp_path):
    r, calls = run(tmp_path, "--name", "labtest", "--spot")
    assert r.returncode == 0, r.stderr
    assert "queue labtest-queue, bucket labtest-123456789012-eu-west-1" in r.stdout
    assert "subnet-a subnet-b" in r.stdout and "Spot queue: true" in r.stdout
    assert "nothing was created" in r.stdout and not (tmp_path / "tf").exists()
    assert "run-instances" not in calls


def test_refuses_names_in_use(tmp_path):
    r, _ = run(tmp_path, QUEUE="smkflex-queue")
    assert r.returncode == 1 and "a queue named smkflex-queue already exists" in r.stderr
    r, _ = run(tmp_path, BUCKET_EXISTS="1")
    assert r.returncode == 1 and "a bucket named smkflex-123456789012-eu-west-1 already exists" in r.stderr
    r, _ = run(tmp_path, "--name", "Bad_Name")
    assert r.returncode == 1 and "lowercase" in r.stderr


def test_no_default_subnets(tmp_path):
    r, _ = run(tmp_path, SUBNETS="")
    assert r.returncode == 1 and "aws ec2 create-default-vpc" in r.stderr
    r, _ = run(tmp_path, "--subnets", "subnet-x,subnet-y", SUBNETS="")
    assert r.returncode == 0 and "subnet-x subnet-y" in r.stdout


def test_continues_a_started_setup(tmp_path):
    (tmp_path / "tf").mkdir()
    (tmp_path / "tf" / "terraform.tfvars").write_text(
        'region = "eu-west-1"\nname   = "older"   # comment\nsubnet_ids = ["subnet-1", "subnet-2"]\nami_id = "ami-0abc"\n')
    r, _ = run(tmp_path)
    assert r.returncode == 0, r.stderr
    assert "continuing the setup" in r.stdout and "older-queue" in r.stdout
    assert "image:     ami-0abc" in r.stdout and "subnet-1 subnet-2" in r.stdout
    r, _ = run(tmp_path, "--name", "other")
    assert r.returncode == 1 and "is for name 'older'" in r.stderr
