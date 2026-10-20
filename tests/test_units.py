import subprocess
import pytest
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_executor_plugin_aws_batch_flex.sizing import fargate_size, job_requirements
from snakemake_executor_plugin_aws_batch_flex.bootstrap import build_prelude
from snakemake_executor_plugin_aws_batch_flex import _normalize_image


def test_fargate_rounding():
    assert fargate_size(1, 3000) == ("1", 3072)
    assert fargate_size(3, 1000) == ("4", 8192)
    with pytest.raises(WorkflowError):
        fargate_size(32, 1000)


def test_requirements():
    r = job_requirements({"mem_mb": 4000, "runtime": 30, "gpu": 2}, 8, "ec2")
    assert (r.vcpu, r.memory_mib, r.gpu, r.timeout_seconds) == ("8", 4256, 2, 1800)
    assert job_requirements({}, 1, "ec2").memory_mib == 2048
    assert job_requirements({"mem_mb": 4000}, 1, "ec2", 0).memory_mib == 4000
    assert job_requirements({"mem_mib": 1000}, 1, "fargate").memory_mib == 2048
    assert job_requirements({"mem_mib": 1900}, 1, "fargate").memory_mib == 3072
    with pytest.raises(WorkflowError):
        job_requirements({"gpu": 1}, 1, "fargate")


def test_image_normalisation():
    assert _normalize_image("docker://ubuntu:24.04") == "ubuntu:24.04"
    assert _normalize_image("quay.io/x/y:1") == "quay.io/x/y:1"
    with pytest.raises(WorkflowError):
        _normalize_image("shub://x/y")


@pytest.mark.parametrize("mode", ["auto", "always", "never"])
def test_prelude_is_valid_posix_sh(mode, tmp_path):
    f = tmp_path / "p.sh"
    f.write_text(build_prelude(mode, "9.27.0", ["snakemake==9.27.0", "pkg==1"], "3.12"))
    assert subprocess.run(["sh", "-n", str(f)]).returncode == 0



def test_image_registry():
    from snakemake_executor_plugin_aws_batch_flex.jobdefs import image_registry, is_ecr
    assert image_registry("ubuntu:24.04") == "docker.io"
    assert image_registry("staphb/bwa:0.7.19") == "docker.io"
    assert image_registry("quay.io/biocontainers/bwa:0.7.17") == "quay.io"
    assert image_registry("localhost:5000/x") == "localhost:5000"
    assert is_ecr(image_registry("123456789012.dkr.ecr.eu-west-1.amazonaws.com/tool:1"))
    assert not is_ecr("quay.io")


def test_known_problem_hints():
    from snakemake_executor_plugin_aws_batch_flex import _known_problem_hints as hints
    two = hints(["snakemake: error: unrecognized arguments: --storage-gcs-project x --storage-http-retries 5"])
    assert len(two) == 2 and "snakemake-storage-plugin-gcs" in two[0] and "plugin-http" in two[1]
    assert "versions are probably too far apart" in hints(["error: unrecognized arguments: --new-flag"])[0]
    assert "ran out of disk" in hints(["cp: error writing 'x': No space left on device"])[0]
    assert hints(["all fine"]) == []


def test_version_note():
    from snakemake_executor_plugin_aws_batch_flex.preflight import version_note
    assert version_note(None, "9.27.0") == (None, None)
    assert version_note("9.27.0", "9.27.0") == (None, None)
    assert version_note("9.20.0", "9.27.0")[0] == "info"
    assert version_note("9.0.1", "9.27.0")[0] == "warning"
    assert version_note("10.0.0", "9.27.0")[0] == "warning"


class _Stub:
    def __init__(self, **calls): self.calls = calls
    def __getattr__(self, name):
        r = self.calls[name]
        if isinstance(r, Exception):
            def fail(**kw): raise r
            return fail
        return lambda **kw: r


def test_ami_snakemake_version_paths():
    from botocore.exceptions import ClientError
    from snakemake_executor_plugin_aws_batch_flex.preflight import ami_snakemake_version
    q = {"jobQueues": [{"computeEnvironmentOrder": [{"computeEnvironment": "ce"}]}]}
    tagged = {"Images": [{"Tags": [{"Key": "SnakemakeVersion", "Value": "9.27.0"}]}]}
    override = _Stub(describe_job_queues=q, describe_compute_environments={"computeEnvironments": [
        {"computeResources": {"ec2Configuration": [{"imageIdOverride": "ami-1"}]}}]})
    assert ami_snakemake_version(override, _Stub(describe_images=tagged), "q") == "9.27.0"
    via_lt = _Stub(describe_job_queues=q, describe_compute_environments={"computeEnvironments": [
        {"computeResources": {"launchTemplate": {"launchTemplateId": "lt-1", "version": "$Latest"}}}]})
    ec2 = _Stub(describe_images=tagged, describe_launch_template_versions={
        "LaunchTemplateVersions": [{"LaunchTemplateData": {"ImageId": "ami-2"}}]})
    assert ami_snakemake_version(via_lt, ec2, "q") == "9.27.0"
    assert ami_snakemake_version(override, _Stub(describe_images={"Images": [{"Tags": []}]}), "q") is None
    denied = ClientError({"Error": {"Code": "UnauthorizedOperation"}}, "DescribeImages")
    assert ami_snakemake_version(override, _Stub(describe_images=denied), "q") is None
