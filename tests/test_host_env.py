"""bootstrap=host. Installing the env needs network: set RUN_NETWORK_TESTS=1."""
import os, subprocess, sys, textwrap
import pytest
from snakemake_executor_plugin_aws_batch_flex.jobdefs import JobDefinitions
from snakemake_executor_plugin_aws_batch_flex import hostenv
from snakemake_executor_plugin_aws_batch_flex.bootstrap import build_host_prelude
from test_integration import run

WHO = textwrap.dedent('''
    rule all:
        input: "who.txt"
    rule who:
        output: "who.txt"
        container: "docker://anything:1"
        run:
            import sys
            with open(output[0], "w") as f:
                f.write(sys.executable)
''')


def test_jobdef_mounts_are_read_only_binds():
    jd = JobDefinitions(None, None, prefix="p", platform="ec2", job_role_arn=None,
                        execution_role_arn=None, log_group="/aws/batch/job",
                        assign_public_ip="ENABLED", ephemeral_storage_gib=None, tags={},
                        mounts=[("/opt/snakemake-env", "/opt/snakemake-env", True),
                                ("/scratch", "/scratch", False)])
    p = jd.container_properties("img")
    assert p["volumes"][0] == {"name": "vol0", "host": {"sourcePath": "/opt/snakemake-env"}}
    assert p["mountPoints"][0] == {"sourceVolume": "vol0", "containerPath": "/opt/snakemake-env", "readOnly": True}
    assert p["mountPoints"][1]["readOnly"] is False


def test_host_prelude_fails_fast_with_clear_message(tmp_path):
    f = tmp_path / "p.sh"
    f.write_text(build_host_prelude(str(tmp_path / "nope"), 0) + "\necho SHOULD-NOT-RUN\n")
    r = subprocess.run(["sh", str(f)], capture_output=True, text=True)
    assert r.returncode == 127 and "host env" in r.stderr and "SHOULD-NOT-RUN" not in r.stdout


@pytest.mark.skipif(not os.environ.get("RUN_NETWORK_TESTS"), reason="needs network")
def test_snakemake_runs_from_the_mounted_host_env(tmp_path):
    env = tmp_path / "host-env"
    script = hostenv.install_script(str(env), f"{sys.version_info[0]}.{sys.version_info[1]}",
                                    hostenv.default_requirements(["fs"], []))
    sp = tmp_path / "install.sh"; sp.write_text(script)
    assert subprocess.run(["bash", str(sp)], capture_output=True).returncode == 0
    r, calls = run(tmp_path, WHO, "--aws-batch-flex-bootstrap", "host",
                   "--aws-batch-flex-host-env-path", str(env),
                   "--aws-batch-flex-host-env-wait-seconds", "0")
    assert r.returncode == 0, r.stdout + r.stderr
    # `run:` executes inside the Snakemake process: its interpreter is the host env's
    assert (tmp_path / "store" / "who.txt").read_text().startswith(str(env))
    reg = next(c for c in calls if c["call"] == "register_job_definition")
    assert reg["mounts"] == [{"sourceVolume": "vol0", "containerPath": str(env), "readOnly": True}]
