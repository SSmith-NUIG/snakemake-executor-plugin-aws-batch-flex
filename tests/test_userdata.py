import email, subprocess, sys
from pathlib import Path
from snakemake_executor_plugin_aws_batch_flex import hostenv

FIX = Path(__file__).parent / "fixtures" / "existing_userdata.txt"
SCRIPT = hostenv.install_script("/opt/snakemake-env", "3.12", ["snakemake==9.27.0"])


def parts(text):
    m = email.message_from_string(text)
    assert m.is_multipart()
    return m.get_payload()


def test_existing_multipart_userdata_is_kept_and_ours_is_appended_last():
    merged = hostenv.launch_template_userdata(SCRIPT, (FIX.read_text(),))
    ps = parts(merged)
    assert [p.get_content_type() for p in ps] == ["text/cloud-config", "text/x-shellscript"]
    theirs, ours = ps[0].get_payload(), ps[1].get_payload()
    assert "systemctl enable --now --no-block ecs" in theirs and "site setup" in theirs
    original_body = FIX.read_text().split("--//\n", 1)[1].split("\n\n", 1)[1].rsplit("--//--", 1)[0]
    assert theirs.strip() == original_body.strip()  # their part is carried over unchanged
    assert ours.startswith("#!/bin/bash") and "snakemake host env" in ours


def test_plain_script_include_is_wrapped():
    merged = hostenv.launch_template_userdata(SCRIPT, ("#!/bin/bash\necho first\n",))
    ps = parts(merged)
    assert [p.get_content_type() for p in ps] == ["text/x-shellscript"] * 2
    assert "echo first" in ps[0].get_payload()


def test_no_include_still_works_and_script_part_is_valid_bash(tmp_path):
    ps = parts(hostenv.launch_template_userdata(SCRIPT))
    assert len(ps) == 1
    f = tmp_path / "s.sh"; f.write_text(ps[0].get_payload())
    assert subprocess.run(["bash", "-n", str(f)]).returncode == 0


def test_cli_include(tmp_path):
    out = subprocess.run(
        [sys.executable, "-m", "snakemake_executor_plugin_aws_batch_flex.hostenv", "userdata",
         "--include", str(FIX), "--storage-plugin", "s3"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert len(parts(out.stdout)) == 2
    bad = subprocess.run(
        [sys.executable, "-m", "snakemake_executor_plugin_aws_batch_flex.hostenv", "script",
         "--include", str(FIX)], capture_output=True, text=True)
    assert bad.returncode != 0 and "only applies" in bad.stderr
