"""cleanup-ami.sh with a stub `aws`: it must never delete an AMI that is in use."""
import os, stat, subprocess, textwrap
from pathlib import Path

SCRIPT = Path(__file__).parent.parent / "scripts" / "cleanup-ami.sh"
AMI = "ami-0123456789abcdef0"

STUB = textwrap.dedent('''\
    #!/usr/bin/env bash
    echo "$*" >> "$AWS_STUB_LOG"
    [ -n "$FAIL_ON" ] && case "$*" in *"$FAIL_ON"*) echo "AccessDenied" >&2; exit 255;; esac
    case "$*" in
      *describe-compute-environments*imageIdOverride*) echo "${CE_IMAGES:-None}";;
      *describe-compute-environments*launchTemplate*) printf '%b\\n' "${CE_LTS:-lt-ce\\t\\$Latest}";;
      *describe-launch-template-versions*lt-ce*) echo "${CE_LT_AMI:-ami-other}";;
      *describe-launch-template-versions*) echo "${LT_AMI:-ami-other}";;
      *describe-launch-templates*) echo "lt-1 lt-2";;
      *describe-instances*) echo "${INSTANCES:-0}";;
      *describe-images*ManagedBy*) echo snakemake-batch-flex;;
      *describe-images*Name*) echo snakemake-host-env-test;;
      *describe-images*SnapshotId*) echo snap-1;;
      *) true;;
    esac
''')


def run(tmp_path, **env_extra):
    stub = tmp_path / "aws"; stub.write_text(STUB); stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.log"
    env = dict(os.environ, AWS=str(stub), AWS_STUB_LOG=str(log), **env_extra)
    r = subprocess.run(["bash", str(SCRIPT), "--region", "eu-west-1", "--tfvars", str(tmp_path / "none"),
                        "--yes", AMI], env=env, capture_output=True, text=True, cwd=tmp_path)
    calls = log.read_text().splitlines() if log.exists() else []
    return r, [c for c in calls if "deregister-image" in c or "delete-snapshot" in c]


def test_unused_ami_is_deleted(tmp_path):
    r, deletes = run(tmp_path)
    assert r.returncode == 0, r.stderr
    assert any(f"deregister-image --image-id {AMI}" in c for c in deletes)


def test_refuses_ami_a_compute_environment_overrides_with(tmp_path):
    r, deletes = run(tmp_path, CE_IMAGES=f"None\t{AMI}")
    assert r.returncode != 0 and "in use" in r.stderr and not deletes


def test_refuses_ami_in_the_launch_template_version_a_compute_environment_uses(tmp_path):
    r, deletes = run(tmp_path, CE_LT_AMI=AMI)
    assert r.returncode != 0 and "in use" in r.stderr and not deletes


def test_refuses_ami_in_any_launch_templates_current_version(tmp_path):
    r, deletes = run(tmp_path, LT_AMI=AMI)
    assert r.returncode != 0 and "in use" in r.stderr and not deletes


def test_refuses_ami_with_instances(tmp_path):
    r, deletes = run(tmp_path, INSTANCES="2")
    assert r.returncode != 0 and "2 instance(s)" in r.stderr and not deletes


def test_refuses_when_usage_cannot_be_checked(tmp_path):
    for call in ("describe-compute-environments", "describe-launch-templates",
                 "describe-launch-template-versions", "describe-instances"):
        r, deletes = run(tmp_path, FAIL_ON=call)
        assert r.returncode != 0 and "could not check" in r.stderr and not deletes, call


def test_refuses_ami_in_tfvars(tmp_path):
    tf = tmp_path / "none"; tf.write_text(f'ami_id = "{AMI}"\n')
    r, deletes = run(tmp_path)
    assert r.returncode != 0 and "in use" in r.stderr and not deletes


def test_no_tfvars_skips_only_the_file_check(tmp_path):
    tf = tmp_path / "terraform" / "terraform.tfvars"; tf.parent.mkdir(); tf.write_text(f'ami_id = "{AMI}"\n')
    stub = tmp_path / "aws"; stub.write_text(STUB); stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    for extra, env_extra, ok in (([], {}, False), (["--no-tfvars"], {}, True), (["--no-tfvars"], {"LT_AMI": AMI}, False)):
        log = tmp_path / "calls.log"; log.unlink(missing_ok=True)
        env = dict(os.environ, AWS=str(stub), AWS_STUB_LOG=str(log), **env_extra)
        r = subprocess.run(["bash", str(SCRIPT), "--region", "eu-west-1", *extra, "--yes", AMI],
                           env=env, capture_output=True, text=True, cwd=tmp_path)
        deleted = "deregister-image" in log.read_text() if log.exists() else False
        assert (r.returncode == 0) == ok and deleted == ok, (extra, env_extra, r.stderr)
