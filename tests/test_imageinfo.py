"""ENTRYPOINT handling: registry lookup and the command built for each entrypoint."""
import json
import subprocess
import sys
import urllib.request

import pytest

from snakemake_executor_plugin_aws_batch_flex import imageinfo
from snakemake_executor_plugin_aws_batch_flex.imageinfo import (
    EntrypointLookupError,
    UnsupportedEntrypoint,
    command_for_image,
    container_command,
    parse_image_ref,
    resolve_entrypoint,
)

SCRIPT = "echo hello\nexit 0"


@pytest.fixture(autouse=True)
def _clean_caches():
    for cache in (imageinfo._CACHE, imageinfo._FAILED, imageinfo._LOGGED, imageinfo._BASIC_CACHE):
        cache.clear()
    yield


def test_parse_image_ref():
    assert parse_image_ref("debian:12") == ("registry-1.docker.io", "library/debian", "12")
    assert parse_image_ref("ubuntu") == ("registry-1.docker.io", "library/ubuntu", "latest")
    assert parse_image_ref("staphb/bwa:0.7.19") == ("registry-1.docker.io", "staphb/bwa", "0.7.19")
    assert parse_image_ref("quay.io/biocontainers/bwa:1") == (
        "quay.io",
        "biocontainers/bwa",
        "1",
    )
    assert parse_image_ref("localhost:5000/x") == ("localhost:5000", "x", "latest")
    assert parse_image_ref("127.0.0.1:5000/x/y") == ("127.0.0.1:5000", "x/y", "latest")
    assert parse_image_ref("x@sha256:abc") == ("registry-1.docker.io", "library/x", "sha256:abc")
    assert parse_image_ref("123.dkr.ecr.eu-west-1.amazonaws.com/t:1") == (
        "123.dkr.ecr.eu-west-1.amazonaws.com",
        "t",
        "1",
    )


def test_commands_for_common_entrypoints():
    # These are the arguments appended to ENTRYPOINT (Docker's Cmd), never the entrypoint again.
    assert container_command(None, SCRIPT) == ["/bin/sh", "-c", SCRIPT]
    assert container_command([], SCRIPT) == ["/bin/sh", "-c", SCRIPT]
    assert container_command(["/bin/sh"], SCRIPT) == ["-c", SCRIPT]
    assert container_command(["/bin/sh", "-c"], SCRIPT) == [SCRIPT]
    assert container_command(["bash", "-euo", "pipefail"], SCRIPT) == ["-c", SCRIPT]
    assert container_command(["tini", "--"], SCRIPT) == ["/bin/sh", "-c", SCRIPT]
    assert container_command(["dumb-init", "--"], SCRIPT) == ["/bin/sh", "-c", SCRIPT]
    assert container_command(["timeout", "60"], SCRIPT) == ["/bin/sh", "-c", SCRIPT]
    assert container_command(["env", "-i", "python3"], SCRIPT)[0] == "-c"
    assert container_command(["env"], SCRIPT) == ["/bin/sh", "-c", SCRIPT]


def test_commands_for_python_entrypoints():
    cmd = container_command(["/usr/bin/python3", "-u"], SCRIPT)
    assert cmd[0] == "-c" and "subprocess.call" in cmd[1]
    already_c = container_command(["python3", "-c"], SCRIPT)
    assert len(already_c) == 1 and "subprocess.call" in already_c[0]
    with pytest.raises(UnsupportedEntrypoint):
        container_command(["python3", "-m", "http.server"], SCRIPT)
    with pytest.raises(UnsupportedEntrypoint):
        container_command(["python3", "-c", "print(1)"], SCRIPT)
    with pytest.raises(UnsupportedEntrypoint):
        container_command(["/app/run.py"], SCRIPT)
    with pytest.raises(UnsupportedEntrypoint):
        container_command(["sh", "-c", "fixed command"], SCRIPT)
    with pytest.raises(UnsupportedEntrypoint):
        container_command(["java", "-jar", "app.jar"], SCRIPT)


def test_explicit_modes_skip_the_entrypoint():
    assert container_command(None, SCRIPT, "none") == ["/bin/sh", "-c", SCRIPT]
    assert container_command(None, SCRIPT, "shell") == ["-c", SCRIPT]
    assert container_command(None, SCRIPT, "sh-c") == [SCRIPT]
    assert container_command(None, SCRIPT, "python")[0] == "-c"
    assert container_command(None, SCRIPT, '["/bin/sh"]') == ["-c", SCRIPT]
    with pytest.raises(ValueError):
        container_command(None, SCRIPT, "nonsense")


@pytest.mark.parametrize(
    "entrypoint",
    [
        [],
        ["/bin/sh"],
        ["/bin/sh", "-c"],
        ["bash"],
        [sys.executable],
        [sys.executable, "-u"],
        ["env", sys.executable],
    ],
)
def test_entrypoint_plus_command_really_runs_the_script(entrypoint):
    # What Docker does: ENTRYPOINT + Cmd. This is the regression test for a duplicated
    # entrypoint (`python3 python3 -c ...`).
    script = "printf '%s\\n' \"it's\" 'a \"test\"'\nexit 3"
    argv = list(entrypoint) + container_command(entrypoint, script)
    result = subprocess.run(argv, capture_output=True, text=True)
    assert result.returncode == 3
    assert result.stdout.splitlines() == ["it's", 'a "test"']


# --------------------------------------------------------------------------- registry lookup


class Http:
    """A tiny fake registry: routes (status, headers, body) by URL suffix, and records headers."""

    def __init__(self, routes, token=None):
        self.routes, self.token, self.requests = routes, token, []

    def __call__(self, url, headers, timeout):
        self.requests.append((url, dict(headers)))
        if self.token and "/token" in url:
            return 200, {}, json.dumps({"token": self.token}).encode()
        for suffix, response in self.routes.items():
            if url.endswith(suffix):
                return response
        return 404, {}, b""


def _config(entrypoint):
    return 200, {}, json.dumps({"config": {"Entrypoint": entrypoint}}).encode()


def test_resolve_entrypoint_from_a_plain_manifest():
    http = Http({
        "/manifests/1": (200, {}, json.dumps({"config": {"digest": "sha256:cfg"}}).encode()),
        "/blobs/sha256:cfg": _config(["python3"]),
    })
    assert resolve_entrypoint("org/img:1", http=http) == ["python3"]
    # cached: the second lookup does not touch the network
    calls = len(http.requests)
    assert resolve_entrypoint("org/img:1", http=http) == ["python3"]
    assert len(http.requests) == calls


def test_resolve_entrypoint_chooses_the_platform():
    index = {
        "manifests": [
            {"digest": "sha256:arm", "platform": {"os": "linux", "architecture": "arm64"}},
            {"digest": "sha256:amd", "platform": {"os": "linux", "architecture": "amd64"}},
            {"digest": "sha256:att", "platform": {"os": "unknown", "architecture": "unknown"}},
        ]
    }
    http = Http({
        "/manifests/1": (200, {}, json.dumps(index).encode()),
        "/manifests/sha256:amd": (200, {}, json.dumps({"config": {"digest": "sha256:cfg"}}).encode()),
        "/blobs/sha256:cfg": _config(None),
    })
    assert resolve_entrypoint("org/img:1", http=http) is None


def test_resolve_entrypoint_token_challenge():
    http = Http(
        {
            "/manifests/1": (200, {}, json.dumps({"config": {"digest": "sha256:cfg"}}).encode()),
            "/blobs/sha256:cfg": _config(["/bin/sh", "-c"]),
        },
        token="tok",
    )
    challenged = []

    def gated(url, headers, timeout):
        if "/token" in url:
            return http(url, headers, timeout)
        if url.endswith("/manifests/1") and headers.get("Authorization") != "Bearer tok":
            challenged.append(url)
            return 401, {"www-authenticate": (
                'Bearer realm="https://auth.example/token",service="registry.example"')}, b""
        return http(url, headers, timeout)

    assert resolve_entrypoint("registry.example/org/img:1", http=gated) == ["/bin/sh", "-c"]
    assert challenged == ["https://registry.example/v2/org/img/manifests/1"]


class Session:
    def __init__(self, token):
        self.token = token
        self.calls = []

    def client(self, name, **kwargs):
        self.calls.append((name, kwargs))

        class Client:
            def get_authorization_token(self, **kw):
                return {"authorizationData": [{"authorizationToken": "dXNlcjpwYXNz"}]}

        return Client()


def test_resolve_entrypoint_uses_ecr_credentials():
    session = Session(None)
    seen = []

    def http(url, headers, timeout):
        seen.append(headers.get("Authorization"))
        if headers.get("Authorization") != "Basic dXNlcjpwYXNz":
            return 401, {"www-authenticate": 'Basic realm="ecr"'}, b""
        if url.endswith("/manifests/1"):
            return 200, {}, json.dumps({"config": {"digest": "sha256:cfg"}}).encode()
        if url.endswith("/blobs/sha256:cfg"):
            return _config(["python3"])
        return 404, {}, b""

    assert resolve_entrypoint(
        "123456789012.dkr.ecr.eu-west-1.amazonaws.com/repo/x:1", session=session, http=http
    ) == ["python3"]
    assert set(seen) == {"Basic dXNlcjpwYXNz"}
    assert len(session.calls) == 1  # the token is cached per registry
    assert session.calls[0][0] == "ecr" and session.calls[0][1]["region_name"] == "eu-west-1"


def test_lookup_failure_is_cached_and_can_be_overridden(monkeypatch):
    calls = []

    def boom(url, headers, timeout):
        calls.append(url)
        raise imageinfo.EntrypointLookupError("offline")

    with pytest.raises(EntrypointLookupError):
        resolve_entrypoint("org/img:1", http=boom)
    # the failure is remembered, without going to the network again
    with pytest.raises(EntrypointLookupError):
        resolve_entrypoint("org/img:1", http=boom)
    assert len(calls) == 1
    # the override wins even for an image whose lookup already failed
    monkeypatch.setenv("SMKFLEX_IMAGE_ENTRYPOINTS", '{"org/img:1": ["python3"]}')
    assert resolve_entrypoint("org/img:1", http=boom) == ["python3"]
    assert len(calls) == 1


def test_command_for_image_falls_back_with_a_warning():
    warnings = []

    def boom(url, headers, timeout):
        raise imageinfo.EntrypointLookupError("offline")

    assert command_for_image("org/img:1", SCRIPT, "auto", http=boom,
                             warn=warnings.append) == ["/bin/sh", "-c", SCRIPT]
    # the same image is warned about only once per run
    assert command_for_image("org/img:1", SCRIPT, "auto", http=boom,
                             warn=warnings.append) == ["/bin/sh", "-c", SCRIPT]
    assert len(warnings) == 1 and "ENTRYPOINT" in warnings[0]


def test_command_for_image_adapts_and_reports_unsupported():
    http = Http({
        "/manifests/1": (200, {}, json.dumps({"config": {"digest": "sha256:cfg"}}).encode()),
        "/blobs/sha256:cfg": _config(["python3"]),
    })
    cmd = command_for_image("org/img:1", SCRIPT, "auto", http=http)
    assert cmd[0] == "-c" and "subprocess.call" in cmd[1]
    assert "python3" not in cmd  # the entrypoint is provided by the image, not repeated

    unsupported = Http({
        "/manifests/1": (200, {}, json.dumps({"config": {"digest": "sha256:cfg"}}).encode()),
        "/blobs/sha256:cfg": _config(["java", "-jar", "app.jar"]),
    })
    with pytest.raises(UnsupportedEntrypoint) as caught:
        command_for_image("org/other:1", SCRIPT, "auto", http=unsupported)
    assert caught.value.entrypoint == ["java", "-jar", "app.jar"]
    assert caught.value.image == "org/other:1"


def test_redirects_to_storage_drop_the_authorization_header():
    handler = imageinfo._SafeRedirectHandler()
    request = urllib.request.Request(
        "https://123456789012.dkr.ecr.eu-west-1.amazonaws.com/v2/repo/blobs/sha256:cfg",
        headers={"Authorization": "Basic abc"},
    )
    to_storage = handler.redirect_request(
        request, None, 307, "Temporary Redirect", {},
        "https://s3.eu-west-1.amazonaws.com/bucket/key?X-Amz-Signature=x",
    )
    assert to_storage.get_header("Authorization") is None
    same_registry = handler.redirect_request(
        request, None, 307, "Temporary Redirect", {},
        "https://123456789012.dkr.ecr.eu-west-1.amazonaws.com/v2/repo/blobs/sha256:cfg2",
    )
    assert same_registry.get_header("Authorization") == "Basic abc"


def test_explicit_mode_never_looks_the_image_up():
    def boom(url, headers, timeout):
        raise AssertionError("the registry must not be contacted")

    assert command_for_image("org/img:1", SCRIPT, "none", http=boom) == [
        "/bin/sh", "-c", SCRIPT]
    assert command_for_image("org/img:1", SCRIPT, "shell", http=boom) == ["-c", SCRIPT]
    assert command_for_image("org/img:1", SCRIPT, "sh-c", http=boom) == [SCRIPT]
    assert command_for_image("org/img:1", SCRIPT, "python", http=boom)[0] == "-c"
