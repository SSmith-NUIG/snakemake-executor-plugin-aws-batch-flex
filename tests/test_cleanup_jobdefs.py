"""cleanup-jobdefs.sh with a stub `aws`: only the plugin's own, unused job definitions go."""
import os, stat, subprocess, textwrap
from pathlib import Path

SCRIPT = Path(__file__).parent.parent / "scripts" / "cleanup-jobdefs.sh"
ARN = "arn:aws:batch:eu-west-1:1:job-definition/"
DEFS = "\\n".join([
    f"{ARN}snakemake-0123456789ab:1\\tsnakemake-0123456789ab\\tsnakemake-batch-flex",   # ours, unused
    f"{ARN}snakemake-aaaaaaaaaaaa:1\\tsnakemake-aaaaaaaaaaaa\\tsnakemake-batch-flex",   # ours, in use
    f"{ARN}snakemake-bbbbbbbbbbbb:3\\tsnakemake-bbbbbbbbbbbb\\tNone",                   # ours, untagged
    f"{ARN}nextflow-thing:7\\tnextflow-thing\\tNone",                                   # not ours
    f"{ARN}snakemake-custom:1\\tsnakemake-custom\\tsnakemake-batch-flex",               # wrong pattern
])

STUB = textwrap.dedent(f'''\\
    #!/usr/bin/env bash
    echo "$*" >> "$AWS_STUB_LOG"
    [ -n "$FAIL_ON" ] && case "$*" in *"$FAIL_ON"*) echo AccessDenied >&2; exit 255;; esac
    case "$*" in
      *describe-job-definitions*) printf '%b\\n' "{DEFS}";;
      *describe-job-queues*) echo "q1 q2";;
      *"list-jobs --job-queue q1 --job-status RUNNING"*) echo "job-1";;
      *list-jobs*) true;;
      *describe-jobs*) echo "{ARN}snakemake-aaaaaaaaaaaa:1";;
      *) true;;
    esac
''')


def run(tmp_path, *extra, **env_extra):
    stub = tmp_path / "aws"; stub.write_text(STUB); stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.log"
    env = dict(os.environ, AWS=str(stub), AWS_STUB_LOG=str(log), **env_extra)
    r = subprocess.run(["bash", str(SCRIPT), "--region", "eu-west-1", *extra], env=env,
                       capture_output=True, text=True)
    calls = log.read_text().splitlines() if log.exists() else []
    return r, [c.split()[-1] for c in calls if "deregister-job-definition" in c]


def test_only_own_unused_tagged_definitions(tmp_path):
    r, gone = run(tmp_path, "--yes")
    assert r.returncode == 0, r.stderr
    assert gone == [f"{ARN}snakemake-0123456789ab:1"]
    assert "keep (in use)" in r.stdout


def test_include_untagged(tmp_path):
    r, gone = run(tmp_path, "--yes", "--include-untagged")
    assert sorted(gone) == [f"{ARN}snakemake-0123456789ab:1", f"{ARN}snakemake-bbbbbbbbbbbb:3"]


def test_dry_run_and_refusals(tmp_path):
    r, gone = run(tmp_path, "--dry-run")
    assert r.returncode == 0 and not gone and "will deregister" in r.stdout
    for call in ("describe-job-definitions", "describe-job-queues", "list-jobs", "describe-jobs"):
        r, gone = run(tmp_path, "--yes", FAIL_ON=call)
        assert r.returncode != 0 and "could not check" in r.stderr and not gone, call
