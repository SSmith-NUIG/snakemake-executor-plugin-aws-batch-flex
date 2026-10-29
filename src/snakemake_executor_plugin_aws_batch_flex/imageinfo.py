"""Images with an ENTRYPOINT. Batch can't set or clear an image's entrypoint (a job definition
has no field for it), and a job's command is Docker's Cmd, which Docker appends to the
entrypoint: an image with ENTRYPOINT ["python3"] would run `python3 /bin/sh -c <script>`.

So the plugin reads each image's entrypoint from its registry, once per run, and gives its jobs
a command that entrypoint runs: `-c <script>` for a shell, a `-c` that hands over to /bin/sh for
Python, and `/bin/sh -c <script>` for everything else. That includes no entrypoint, wrappers such
as tini, and entrypoint scripts, which end with `exec "$@"` (continuumio/miniconda3's,
mambaorg/micromamba's). If the registry can't be read, jobs get `/bin/sh -c` too, as before."""

import base64
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

import boto3
from snakemake_interface_common.exceptions import WorkflowError

from .jobdefs import image_registry, is_ecr

SHELLS = {"sh", "bash", "dash", "ash", "zsh", "ksh"}
WRAPPERS = {"tini", "dumb-init", "catatonit", "env", "env-execute"}  # run what follows their options
WITH_VALUE = {"-u", "--unset", "-C", "--chdir", "-e", "-p", "-r", "--rewrite"}  # env's, tini's, dumb-init's
PYTHON = re.compile(r"(python|pypy)[0-9.]*")
RUNS_NEXT = re.compile(r"-[A-Za-z]*c")  # -c, -lc, -ec: the shell or Python runs the next argument
# Python's part: become /bin/sh -c <script> (the argument after it), so that Batch's stop signal
# goes to the job and not to a Python waiting for it
EXEC_SH = "import os,sys;os.execv('/bin/sh',['/bin/sh','-c']+sys.argv[1:])"
MANIFESTS = ", ".join([
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.oci.image.index.v1+json",
])
TIMEOUT = 10  # seconds per registry request


def command(entrypoint, script):
    """(Docker's Cmd that makes `entrypoint` run `script`, whether the plugin knows that
    entrypoint). One it doesn't know gets /bin/sh -c, as an entrypoint script expects."""
    sh = ["/bin/sh", "-c", script]
    if not entrypoint:
        return sh, True
    exe, args = os.path.basename(entrypoint[0]), list(entrypoint[1:])
    options = all(a.startswith("-") for a in args)
    if exe in SHELLS and options:
        return ([script] if args and RUNS_NEXT.fullmatch(args[-1]) else ["-c", script]), True
    if PYTHON.fullmatch(exe) and options:  # e.g. ["python3"] or ["python3", "-u"]
        return ([EXEC_SH, script] if args and RUNS_NEXT.fullmatch(args[-1]) else ["-c", EXEC_SH, script]), True
    if exe in WRAPPERS:
        rest = args
        while rest and (rest[0].startswith("-") or (exe == "env" and "=" in rest[0])):
            rest = rest[2:] if rest[0] in WITH_VALUE else rest[1:]
        return command(rest, script) if rest else (sh, True)
    return sh, False


def parse_image(image: str):
    """(registry API host, repository, tag or digest) of an image reference."""
    name, ref = image.split("@", 1) if "@" in image else (image, "latest")
    if "@" not in image and name.rfind(":") > name.rfind("/"):
        name, ref = name.rsplit(":", 1)
    first, _, rest = name.partition("/")
    host, repo = (first, rest) if rest and ("." in first or ":" in first or first == "localhost") else ("docker.io", name)
    if host in ("docker.io", "index.docker.io", "registry-1.docker.io"):
        host = "registry-1.docker.io"
        if "/" not in repo:  # Docker Hub's own images
            repo = f"library/{repo}"
    return host, repo, ref


class _NoAuthElsewhere(urllib.request.HTTPRedirectHandler):
    """Registries send blobs from other hosts (ECR from S3, with a presigned URL that refuses a
    request that also has an Authorization header): it isn't sent there."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None and urllib.parse.urlsplit(newurl).netloc != urllib.parse.urlsplit(req.full_url).netloc:
            new.remove_header("Authorization")
        return new


_OPENER = urllib.request.build_opener(_NoAuthElsewhere())


def _get(url, headers):
    """(status, headers, body)."""
    try:
        with _OPENER.open(urllib.request.Request(url, headers=headers), timeout=TIMEOUT) as r:
            return r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


class Entrypoints:
    """Image entrypoints from their registries: anonymously, with ECR's token, or with the
    registry-credentials secret. Each image is looked up once (a failure too)."""

    def __init__(self, credentials: Optional[dict] = None, logger=None):
        self.credentials = credentials or {}
        self.logger = logger
        self._found: dict = {}  # image -> entrypoint (list) or the reason it couldn't be read (str)
        self._auth: dict = {}  # registry -> Authorization header or None
        self._said: set = set()

    def command(self, image: str, script: str, override=None, what: str = "") -> list:
        """The job command for `image`. `override` is the batch_entrypoint resource: the
        entrypoint as a JSON list, for an image whose registry can't be read."""
        if override:
            try:
                entrypoint = json.loads(override)
                assert isinstance(entrypoint, list) and all(isinstance(a, str) for a in entrypoint)
            except (ValueError, AssertionError):
                raise WorkflowError(f"{what}: batch_entrypoint must be the image's ENTRYPOINT as a JSON list, "
                                    f"such as '[\"python3\"]' ('[]' for none), got {override!r}.") from None
            return command(entrypoint, script)[0]
        if image not in self._found:
            try:
                self._found[image] = self._lookup(image)
            except Exception as e:  # network, permissions, an unexpected registry answer
                self._found[image] = str(e) or type(e).__name__
        entrypoint = self._found[image]
        if isinstance(entrypoint, str):
            self._say(image, "warning", f"Could not read the ENTRYPOINT of image {image} ({entrypoint}), so its "
                      "jobs run `/bin/sh -c ...` as if it had none. If it has one that doesn't run that (such "
                      "as [\"python3\"]), give its rules batch_entrypoint='[\"python3\"]'.")
            return command(None, script)[0]
        cmd, known = command(entrypoint, script)
        if entrypoint:
            how = "" if known else " (as an entrypoint script that runs the command it's given)"
            self._say(image, "info", f"Image {image} has ENTRYPOINT {entrypoint}: its jobs start through it{how}.")
        return cmd

    def _say(self, image, level, text):
        if self.logger and image not in self._said:
            self._said.add(image)
            getattr(self.logger, level)(text)

    def _lookup(self, image: str) -> list:
        host, repo, ref = parse_image(image)

        def get(path):
            return json.loads(self._fetch(host, repo, path, image_registry(image)))

        manifest = get(f"manifests/{ref}")
        if "manifests" in manifest:  # one image per platform: Batch runs linux/amd64
            digest = next((m["digest"] for m in manifest["manifests"]
                           if m.get("platform", {}).get("os") == "linux"
                           and m.get("platform", {}).get("architecture") == "amd64"), None)
            if not digest:
                raise ValueError("no linux/amd64 image")
            manifest = get(f"manifests/{digest}")
        config = get(f"blobs/{manifest['config']['digest']}")
        return list((config.get("config") or {}).get("Entrypoint") or [])

    def _fetch(self, host, repo, path, registry) -> bytes:
        url = f"https://{host}/v2/{repo}/{path}"
        headers = {"Accept": MANIFESTS}
        auth = self._basic(registry)
        if auth:
            headers["Authorization"] = auth
        status, got, body = _get(url, headers)
        if status == 401:  # Docker Hub, quay.io, ghcr.io, public.ecr.aws: a token for this repository
            token = self._token(got.get("WWW-Authenticate", ""), repo, auth)
            if token:
                status, got, body = _get(url, {**headers, "Authorization": token})
        if status != 200:
            raise ValueError(f"{host} answered HTTP {status} for {repo}/{path}")
        return body

    def _token(self, challenge, repo, auth):
        scheme, _, params = challenge.partition(" ")
        fields = dict(re.findall(r'(\w+)="([^"]*)"', params))
        if scheme.lower() != "bearer" or "realm" not in fields:
            return None
        query = {"scope": fields.get("scope") or f"repository:{repo}:pull"}
        if "service" in fields:
            query["service"] = fields["service"]
        realm = fields["realm"]
        status, _, body = _get(f"{realm}{'&' if '?' in realm else '?'}{urllib.parse.urlencode(query)}",
                               {"Authorization": auth} if auth else {})
        doc = json.loads(body) if status == 200 else {}
        token = doc.get("token") or doc.get("access_token")
        return f"Bearer {token}" if token else None

    def _basic(self, registry):
        """Authorization for a private registry: ECR's token, or the registry-credentials secret."""
        if registry not in self._auth:
            header = None
            if is_ecr(registry):
                account, region = registry.split(".")[0], registry.split(".")[3]
                data = boto3.client("ecr", region_name=region).get_authorization_token(
                    registryIds=[account])["authorizationData"][0]
                header = "Basic " + data["authorizationToken"]
            elif registry in self.credentials:
                arn = self.credentials[registry]
                raw = boto3.client("secretsmanager", region_name=arn.split(":")[3]).get_secret_value(
                    SecretId=arn)["SecretString"]
                secret = json.loads(raw)  # ECS's format: {"username": ..., "password": ...}
                pair = f"{secret['username']}:{secret['password']}"
                header = "Basic " + base64.b64encode(pair.encode()).decode()
            self._auth[registry] = header
        return self._auth[registry]
