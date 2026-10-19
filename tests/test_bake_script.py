"""bake-ami.sh with a stub `aws`: call order, and it only touches the builder it created."""
import os, stat, subprocess, textwrap
from pathlib import Path

SCRIPT = Path(__file__).parent.parent / "scripts" / "bake-ami.sh"
BASE = "ami-0123456789abcdef0"

STUB = textwrap.dedent('''\
    #!/usr/bin/env bash
    echo "$*" >> "$AWS_STUB_LOG"
    case "$*" in
      # Real AWS: these two waiters fail right after launch / on slow AMI creation. Never use them.
      *"wait instance-stopped"*|*"wait image-available"*) echo "FORBIDDEN WAITER" >&2; exit 255;;
      *describe-instances*) echo "${INSTANCE_STATE:-stopped}";;
      *describe-images*) echo "${IMAGE_STATE:-available}";;
      *describe-subnets*) echo vpc-1;;
      *create-security-group*) echo sg-new;;
      *run-instances*) echo i-builder;;
      *get-console-output*) [ -n "$CONSOLE" ] && printf 'boot...\\n%s\\n' "$CONSOLE";;
      *create-image*) echo ami-new;;
      *) true;;
    esac
''')

# every aws sub-command the script may use, and what it may touch
ALLOWED = {"describe-instances", "describe-images", "describe-subnets", "create-security-group", "run-instances", "wait",
           "get-console-output", "create-image", "terminate-instances", "delete-security-group"}


def run(tmp_path, console, *extra, **env_extra):
    stub = tmp_path / "aws"; stub.write_text(STUB); stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.log"
    env = dict(os.environ, AWS=str(stub), AWS_STUB_LOG=str(log), CONSOLE=console, SMK_BAKE_SLEEP="true", **env_extra)
    r = subprocess.run(["bash", str(SCRIPT), "--base-ami", BASE, "--subnet", "subnet-1",
                        "--region", "eu-west-1", *extra], env=env, capture_output=True, text=True, cwd=tmp_path)
    return r, (log.read_text().splitlines() if log.exists() else [])


def test_happy_path_order_and_read_only_use_of_base(tmp_path):
    r, calls = run(tmp_path, "")   # empty console: must not matter
    assert r.returncode == 0, r.stdout + r.stderr
    assert "ami_id = \"ami-new\"" in r.stdout
    subcmds = [c.split()[1] for c in calls]          # "ec2 <subcmd> ..."
    assert set(subcmds) <= ALLOWED, set(subcmds) - ALLOWED
    # base AMI appears only as the image to launch from
    assert [c for c in calls if BASE in c and "--image-id" in c and c.split()[1] == "run-instances"]
    assert not [c for c in calls if BASE in c and c.split()[1] != "run-instances" and "BaseAmi" not in c]
    # image is taken from the builder we launched, then everything temporary is removed
    assert any(c.startswith("ec2 create-image --instance-id i-builder") for c in calls)
    import importlib.metadata as md
    assert any(f"Key=SnakemakeVersion,Value={md.version('snakemake')}" in c for c in calls if "create-image" in c)
    assert any("terminate-instances --instance-ids i-builder" in c for c in calls)
    assert any("delete-security-group --group-id sg-new" in c for c in calls)
    assert subcmds.index("create-image") < subcmds.index("terminate-instances")
    assert not [c for c in calls if "wait instance-stopped" in c or "wait image-available" in c]


def test_dry_run_calls_nothing(tmp_path):
    r, calls = run(tmp_path, "", "--dry-run")
    assert r.returncode == 0 and calls == []


def test_builder_that_never_stops_times_out_and_cleans_up(tmp_path):
    r, calls = run(tmp_path, "SMK_BAKE_OK", INSTANCE_STATE="pending", SMK_BAKE_TIMEOUT="0")
    assert r.returncode != 0 and "timed out" in r.stderr
    subcmds = [c.split()[1] for c in calls]
    assert "create-image" not in subcmds
    assert "terminate-instances" in subcmds and "delete-security-group" in subcmds


def test_pending_state_is_not_treated_as_failure(tmp_path):
    """'pending' right after launch means keep waiting, not failure."""
    r, _ = run(tmp_path, "SMK_BAKE_OK", INSTANCE_STATE="pending", SMK_BAKE_TIMEOUT="0")
    assert "ended in state" not in r.stderr   # only the timeout path, never a failure state


def test_terminated_builder_is_a_failure(tmp_path):
    r, calls = run(tmp_path, "SMK_BAKE_OK", INSTANCE_STATE="terminated")
    assert r.returncode != 0 and "ended in state 'terminated'" in r.stderr
    assert "create-image" not in [c.split()[1] for c in calls]




def test_userdata_stops_the_builder_only_on_success(tmp_path):
    r, _ = run(tmp_path, "", "--dry-run")
    ud = r.stdout.split("Builder user-data:", 1)[1]
    after_installer = ud.split("\nSMK_INSTALL_EOF\n", 1)[1]     # the installer has its own if/else inside
    then_part, else_part = after_installer.split("\nelse\n", 1)
    assert "shutdown -h now" in then_part and "shutdown" not in else_part
    assert "sleep" in else_part                     # failure: stay running
    assert "SMK_BAKE_OK" not in ud                  # no console-marker protocol any more


def test_failed_install_leaves_builder_running_and_is_diagnosed_best_effort(tmp_path):
    r, calls = run(tmp_path, "SMK_BAKE_FAILED\nboom: pip could not reach the index",
                   INSTANCE_STATE="running", SMK_BAKE_TIMEOUT="0")
    assert r.returncode != 0 and "did not stop itself" in r.stderr
    assert "pip could not reach the index" in r.stderr        # console detail shown when available
    subcmds = [c.split()[1] for c in calls]
    assert "create-image" not in subcmds
    assert "terminate-instances" in subcmds and "delete-security-group" in subcmds


def test_failure_without_any_console_output_still_fails_cleanly(tmp_path):
    r, calls = run(tmp_path, "", INSTANCE_STATE="running", SMK_BAKE_TIMEOUT="0")
    assert r.returncode != 0 and "no console output available" in r.stderr
    assert "create-image" not in [c.split()[1] for c in calls]
