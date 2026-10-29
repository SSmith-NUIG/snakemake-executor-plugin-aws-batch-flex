"""Images with an ENTRYPOINT (imageinfo.py): the command each kind of entrypoint runs, and the
lookup in the registry."""
import json
import os
import subprocess

import pytest
from snakemake_interface_common.exceptions import WorkflowError

from snakemake_executor_plugin_aws_batch_flex import imageinfo
from snakemake_executor_plugin_aws_batch_flex.imageinfo import Entrypoints, command, parse_image

SCRIPT = "echo \"ran $1\"; exit 3"


@pytest.mark.parametrize("entrypoint, known", [
    ([], True), (["python3"], True), (["python3", "-u"], True), (["python3", "-c"], True),
    (["/usr/bin/env", "python3"], True), (["bash"], True), (["sh", "-c"], True), (["/bin/bash", "-lc"], True),
    (["env", "-u", "X", "A=1"], True), (["SCRIPT"], False),
])
def test_entrypoint_and_command_run_the_script(tmp_path, entrypoint, known):
    # Docker runs ENTRYPOINT + Cmd; the script's output and exit code come through
    entry = tmp_path / "entry.sh"
    entry.write_text('#!/bin/sh\nexec "$@"\n')
    entry.chmod(0o755)
    entrypoint = [str(entry) if a == "SCRIPT" else a for a in entrypoint]
    cmd, k = command(entrypoint, "echo ran; exit 3")
    r = subprocess.run(entrypoint + cmd, capture_output=True, text=True)
    assert (r.returncode, r.stdout, k) == (3, "ran\n", known)


def test_image_references():
    assert parse_image("samtools") == ("registry-1.docker.io", "library/samtools", "latest")
    assert parse_image("docker.io/vergaju/evoflow:1") == ("registry-1.docker.io", "vergaju/evoflow", "1")
    assert parse_image("quay.io/biocontainers/pandas:2.2.1") == ("quay.io", "biocontainers/pandas", "2.2.1")
    assert parse_image("localhost:5000/x@sha256:ab") == ("localhost:5000", "x", "sha256:ab")


class Registry:
    """Docker Hub's way: a token first, then an index of platforms, a manifest and the config."""

    def __init__(self, entrypoint):
        self.entrypoint, self.calls = entrypoint, []

    def __call__(self, url, headers):
        self.calls.append(url)
        if url.startswith("https://auth/"):
            return 200, {}, json.dumps({"token": "T"}).encode()
        if headers.get("Authorization") != "Bearer T":
            return 401, {"WWW-Authenticate": 'Bearer realm="https://auth/token",service="reg"'}, b""
        doc = ({"manifests": [{"digest": "arm", "platform": {"os": "linux", "architecture": "arm64"}},
                              {"digest": "amd", "platform": {"os": "linux", "architecture": "amd64"}}]}
               if url.endswith("/manifests/1") else
               {"config": {"digest": "cfg"}} if url.endswith("/manifests/amd") else
               {"config": {"Entrypoint": self.entrypoint}})
        return 200, {}, json.dumps(doc).encode()


class Log:
    def __init__(self):
        self.lines = []

    def info(self, msg):
        self.lines.append(msg)

    warning = info


def test_lookup_once_per_image(monkeypatch):
    reg = Registry(["python3"])
    monkeypatch.setattr(imageinfo, "_get", reg)
    log = Log()
    e = Entrypoints(logger=log)
    assert e.command("me/tool:1", SCRIPT)[:2] == ["-c", imageinfo.EXEC_SH]
    assert e.command("me/tool:1", SCRIPT)[:2] == ["-c", imageinfo.EXEC_SH]
    assert sum("/manifests/1" in c for c in reg.calls) == 2  # asked without, then with the token, once
    assert reg.calls[-1] == "https://registry-1.docker.io/v2/me/tool/blobs/cfg"
    assert log.lines == ["Image me/tool:1 has ENTRYPOINT ['python3']: its jobs start through it."]


def test_unreadable_registry_falls_back_and_says_how_to_override(monkeypatch):
    monkeypatch.setattr(imageinfo, "_get", lambda url, headers: (404, {}, b""))
    log = Log()
    e = Entrypoints(logger=log)
    assert e.command("me/gone:1", SCRIPT) == ["/bin/sh", "-c", SCRIPT]
    assert "HTTP 404" in log.lines[0] and "batch_entrypoint" in log.lines[0]
    # the rule's batch_entrypoint needs no registry
    assert e.command("me/gone:1", SCRIPT, '["python3"]')[:2] == ["-c", imageinfo.EXEC_SH]
    with pytest.raises(WorkflowError, match="JSON list"):
        e.command("me/gone:1", SCRIPT, "python3", "rule x")


@pytest.mark.skipif(not os.environ.get("RUN_NETWORK_TESTS"), reason="needs network")
def test_real_registries():
    e = Entrypoints()
    for image, first in [("vergaju/evoflow:latest", "-c"), ("continuumio/miniconda3:26.7.1-1", "/bin/sh"),
                         ("mambaorg/micromamba:2.3.0", "/bin/sh"), ("condaforge/miniforge3:25.3.1-0", "/bin/sh"),
                         ("quay.io/biocontainers/samtools:1.21--h50ea8bc_0", "/bin/sh"),
                         ("public.ecr.aws/docker/library/busybox:latest", "/bin/sh")]:
        assert e.command(image, SCRIPT)[0] == first
        assert not isinstance(e._found[image], str), e._found[image]  # read, not a failure
