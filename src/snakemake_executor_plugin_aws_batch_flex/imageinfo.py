"""How to launch a shell script in an image that has an ENTRYPOINT.

AWS Batch cannot override an image's ENTRYPOINT. `SubmitJob`'s `containerOverrides` only has
`command`, which maps to Docker's Cmd (the arguments Docker appends to the image's ENTRYPOINT),
and `RegisterJobDefinition` has no `entryPoint` field either, in `containerProperties` or in
`ecsProperties`. So an image with `ENTRYPOINT ["python3"]` runs the plugin's command as
`python3 /bin/sh -c ...`.

To work with such images the plugin reads the image's config (`Entrypoint`) from its registry and
builds a command the entrypoint can execute: a shell entrypoint gets `-c <script>`, a Python
entrypoint gets `-c <bootstrap that runs /bin/sh>`, and so on. Images whose entrypoint can't be
driven that way (e.g. `["java", "-jar", "app.jar"]`) are reported before the job is submitted;
`ENTRYPOINT []` in a wrapper image, or the explicit entrypoint modes, are the way out.

`SMKFLEX_IMAGE_ENTRYPOINTS` (a JSON object mapping an image, or "*", to an entrypoint list or
"none") skips the registry lookup. It is useful offline and in the test suite.
"""

import base64
import http.client
import json
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import List, Optional

import boto3

from .jobdefs import image_registry, is_ecr

MANIFEST_ACCEPT = ", ".join(
    [
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
    ]
)

# Entrypoint programs the plugin knows how to launch a shell script through.
SHELLS = {"sh", "bash", "dash", "ash", "zsh", "ksh", "busybox"}
PASSTHROUGH = {"tini", "dumb-init", "catatonit", "timeout", "nice", "stdbuf", "exec"}
PYTHON = re.compile(r"^(python|pypy)[0-9.]*$")

MODES = ("auto", "none", "shell", "sh-c", "python")

ENTRYPOINT_HINT = (
    "Use an image whose entrypoint the plugin can drive: rebuild or wrap it with "
    "`ENTRYPOINT []` (FROM <image>; ENTRYPOINT []), or say how to launch jobs with the "
    "`aws-batch-flex-container-entrypoint` setting (or the `batch_entrypoint` resource): "
    "none, shell, sh-c, python, or the entrypoint as a JSON list such as '[\"/bin/sh\",\"-c\"]'."
)


class EntrypointLookupError(Exception):
    """The image's config could not be read (network, credentials, unknown manifest format)."""


class UnsupportedEntrypoint(Exception):
    """The image's ENTRYPOINT can't be made to run the plugin's shell script."""

    def __init__(self, entrypoint, image: Optional[str] = None):
        self.entrypoint = list(entrypoint or [])
        self.image = image
        super().__init__(f"unsupported ENTRYPOINT {self.entrypoint}")


# --------------------------------------------------------------------------- image references


def parse_image_ref(image: str):
    """(registry API host, repository, tag or digest) for an image reference."""
    image = image.strip()
    if "@" in image:
        name, ref = image.split("@", 1)
    else:
        name, ref = image, "latest"
        colon = name.rfind(":")
        if colon > name.rfind("/"):
            name, ref = name[:colon], name[colon + 1 :]
    first, sep, rest = name.partition("/")
    if sep and ("." in first or ":" in first or first == "localhost"):
        registry, repository = first, rest
    else:
        registry, repository = "docker.io", name
    if registry in ("docker.io", "index.docker.io", "registry-1.docker.io"):
        registry = "registry-1.docker.io"
        if "/" not in repository:  # Docker Hub official images live under library/
            repository = f"library/{repository}"
    return registry, repository, ref


# ------------------------------------------------------------------------------- registry HTTP


class _IPv4FirstConnection(http.client.HTTPSConnection):
    """IPv4 addresses before IPv6 ones. A machine whose IPv6 route goes nowhere otherwise
    spends one connection timeout per IPv6 address before falling back to IPv4."""

    def connect(self):
        if getattr(self, "_tunnel_host", None):  # proxies: leave it to the standard code
            return super().connect()
        addresses = socket.getaddrinfo(self.host, self.port, type=socket.SOCK_STREAM)
        addresses.sort(key=lambda info: info[0] != socket.AF_INET)
        error = None
        for family, kind, protocol, _name, address in addresses:
            sock = socket.socket(family, kind, protocol)
            sock.settimeout(self.timeout)
            try:
                sock.connect(address)
            except OSError as e:
                error = e
                sock.close()
                continue
            try:
                self.sock = self._context.wrap_socket(sock, server_hostname=self.host)
            except OSError:
                sock.close()
                raise
            return
        raise error or OSError(f"cannot connect to {self.host}")


class _IPv4FirstHandler(urllib.request.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(_IPv4FirstConnection, req, context=self._context)


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Drop the registry's Authorization header when a redirect leaves the registry.

    Registries (ECR in particular) answer blob requests with a redirect to object storage, and a
    presigned URL rejects a request that also carries an Authorization header."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and (
            urllib.parse.urlsplit(newurl).netloc != urllib.parse.urlsplit(req.full_url).netloc
        ):
            redirected.remove_header("Authorization")
        return redirected


_OPENER = urllib.request.build_opener(_IPv4FirstHandler(), _SafeRedirectHandler())


def _http(url, headers, timeout=10.0):
    """(status, lower-case headers, body). Raises EntrypointLookupError when unreachable."""
    request = urllib.request.Request(url, headers=headers)
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            return (
                response.status,
                {k.lower(): v for k, v in response.headers.items()},
                response.read(),
            )
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in (e.headers or {}).items()}, e.read()
    except (urllib.error.URLError, OSError) as e:
        raise EntrypointLookupError(f"cannot reach {url.split('/v2/')[0]}: {e}")


_BASIC_CACHE: dict = {}


def _basic_auth(registry: str, session, credentials) -> Optional[str]:
    """Authorization header for a registry, from ECR's token or a Secrets Manager secret."""
    if registry in _BASIC_CACHE:
        return _BASIC_CACHE[registry]
    header = None
    try:
        if is_ecr(registry):
            match = re.match(
                r"^(\d{12})\.dkr\.ecr(-fips)?\.([a-z0-9-]+)\.amazonaws\.com(\.cn)?$", registry
            )
            account, region = match.group(1), match.group(3)
            client = session.client("ecr", region_name=region)
            data = client.get_authorization_token(registryIds=[account])["authorizationData"][0]
            header = "Basic " + data["authorizationToken"]
        elif registry == "public.ecr.aws":
            data = session.client("ecr-public", region_name="us-east-1").get_authorization_token()
            data = data.get("authorizationData", data)
            header = "Basic " + data["authorizationToken"]
        else:
            arn = (credentials or {}).get(registry)
            if arn:
                raw = session.client("secretsmanager").get_secret_value(SecretId=arn)["SecretString"]
                try:
                    secret = json.loads(raw)
                    user = secret.get("username", "")
                    password = secret.get("password", raw)
                except (ValueError, AttributeError):
                    user, password = "", raw
                token = base64.b64encode(f"{user}:{password}".encode()).decode()
                header = "Basic " + token
    except Exception as e:  # boto3's error types vary; the caller turns this into a warning
        raise EntrypointLookupError(f"cannot get credentials for {registry}: {e}")
    _BASIC_CACHE[registry] = header
    return header


def _challenge_auth(challenge, repository, basic, http, timeout):
    """Authorization header from a WWW-Authenticate challenge (Bearer token or Basic)."""
    scheme, _, params = challenge.partition(" ")
    if scheme.lower() == "basic":
        return basic
    if scheme.lower() != "bearer":
        return basic
    fields = dict(re.findall(r'(\w+)="([^"]*)"', params))
    realm = fields.get("realm")
    if not realm:
        return basic
    query = {"scope": fields.get("scope") or f"repository:{repository}:pull"}
    if fields.get("service"):
        query["service"] = fields["service"]
    url = realm + ("&" if "?" in realm else "?") + urllib.parse.urlencode(query)
    status, _headers, body = http(url, {"Authorization": basic} if basic else {}, timeout)
    if status != 200:
        return basic
    try:
        doc = json.loads(body)
    except ValueError:
        return basic
    token = doc.get("token") or doc.get("access_token")
    return f"Bearer {token}" if token else basic


def _registry_get(host, repository, path, registry, credentials, session, http, timeout):
    url = f"https://{host}/v2/{repository}/{path}"
    basic = _basic_auth(registry, session, credentials)
    headers = {"Accept": MANIFEST_ACCEPT}
    if basic:  # ECR and private registries: no need for the extra 401 round trip
        headers["Authorization"] = basic
    status, response_headers, body = http(url, headers, timeout)
    if status == 401:
        challenge = response_headers.get("www-authenticate", "")
        auth = _challenge_auth(challenge, repository, basic, http, timeout)
        if auth and auth != basic:
            status, response_headers, body = http(url, {**headers, "Authorization": auth}, timeout)
    if status == 200:
        return body
    if status == 404:
        raise EntrypointLookupError(f"{repository}:{path.split('/')[-1]} not found in {host}")
    raise EntrypointLookupError(f"{host} returned HTTP {status} for {repository}")


def _choose_manifest(manifests, arch):
    """Digest of the manifest for `arch` (linux), preferring an exact architecture match."""
    candidates = [m for m in manifests if (m.get("platform") or {}).get("os", "linux") == "linux"]
    real = [
        m
        for m in candidates
        if (m.get("platform") or {}).get("architecture") not in (None, "unknown")
    ]
    for group in (real, candidates, manifests):
        for manifest in group:
            if (manifest.get("platform") or {}).get("architecture") == arch and manifest.get("digest"):
                return manifest["digest"]
        for manifest in group:
            if manifest.get("digest") and (manifest.get("platform") or {}).get("architecture") not in (
                None,
                "unknown",
            ):
                return manifest["digest"]
    raise EntrypointLookupError("no usable platform in the image's manifest list")


# --------------------------------------------------------------------------------- resolution


_CACHE: dict = {}  # (image, arch) -> entrypoint list or None
_FAILED: dict = {}  # (image, arch) -> reason
_LOGGED: set = set()
_MISSING = object()


def _override(image):
    """An entrypoint forced by SMKFLEX_IMAGE_ENTRYPOINTS, or _MISSING."""
    raw = os.environ.get("SMKFLEX_IMAGE_ENTRYPOINTS")
    if not raw:
        return _MISSING
    try:
        table = json.loads(raw)
    except ValueError:
        return _MISSING
    if not isinstance(table, dict):
        return _MISSING
    value = table.get(image, table.get("*", _MISSING))
    if value is _MISSING:
        return _MISSING
    if value is None or (isinstance(value, str) and value.strip().lower() == "none"):
        return []
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def resolve_entrypoint(
    image: str,
    *,
    credentials: Optional[dict] = None,
    session=None,
    arch: str = "amd64",
    http=None,
    timeout: float = 10.0,
) -> Optional[List[str]]:
    """The image's ENTRYPOINT (None when it has none), or EntrypointLookupError if it can't be
    read. Results are cached per image and architecture."""
    key = (image, arch)
    if key in _CACHE:
        return _CACHE[key]
    override = _override(image)
    if override is not _MISSING:
        _CACHE[key] = override
        return override
    if key in _FAILED:
        raise EntrypointLookupError(_FAILED[key])

    http = http or _http
    session = session or boto3.session.Session()
    registry = image_registry(image)
    host, repository, ref = parse_image_ref(image)
    try:
        body = _registry_get(
            host, repository, f"manifests/{ref}", registry, credentials, session, http, timeout
        )
        manifest = json.loads(body)
        if "manifests" in manifest:
            digest = _choose_manifest(manifest["manifests"], arch)
            body = _registry_get(
                host, repository, f"manifests/{digest}", registry, credentials, session, http, timeout
            )
            manifest = json.loads(body)
        config_digest = (manifest.get("config") or {}).get("digest")
        if not config_digest:
            raise EntrypointLookupError("schema 1 manifest: the image config can't be read")
        body = _registry_get(
            host, repository, f"blobs/{config_digest}", registry, credentials, session, http, timeout
        )
        config = (json.loads(body) or {}).get("config") or {}
        entrypoint = config.get("Entrypoint")
        entrypoint = [str(item) for item in entrypoint] if entrypoint else None
    except EntrypointLookupError as e:
        _FAILED[key] = str(e)
        raise
    except (ValueError, TypeError, KeyError) as e:
        reason = f"unexpected image manifest: {e}"
        _FAILED[key] = reason
        raise EntrypointLookupError(reason)
    _CACHE[key] = entrypoint
    return entrypoint


# ---------------------------------------------------------------------------- command building


def _python_bootstrap(script: str) -> str:
    return (
        "import subprocess,sys;"
        "sys.exit(subprocess.call(['/bin/sh','-c'," + repr(script) + "]))"
    )


def _after_env(args):
    """The program `env` runs: env's flags and VAR=value assignments are skipped."""
    index = 0
    while index < len(args):
        arg = args[index]
        if arg in ("-u", "--unset"):
            index += 2
            continue
        if arg in ("-C", "--chdir"):
            index += 2
            continue
        if arg.startswith("-") or re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", arg):
            index += 1
            continue
        return args[index:]
    return []


def _for_entrypoint(entrypoint, script: str):
    """The Cmd to append to `entrypoint` so that it runs `script`.

    Batch's `containerOverrides.command` is Docker's Cmd: Docker runs ENTRYPOINT + Cmd, so these
    are the arguments after the entrypoint, not the entrypoint program again."""
    if not entrypoint:
        return ["/bin/sh", "-c", script]
    entrypoint = [str(item) for item in entrypoint]
    exe = entrypoint[0].rsplit("/", 1)[-1]
    if exe in SHELLS:
        # `sh` gets `-c <script>`; `sh -c` only gets the script
        if "-c" in entrypoint[1:-1]:  # the entrypoint already runs a command of its own
            raise UnsupportedEntrypoint(entrypoint)
        return [script] if entrypoint[-1] == "-c" else ["-c", script]
    if exe in PASSTHROUGH:
        # tini --, timeout 60, nice -n 10 ... all accept a program and its arguments
        return ["/bin/sh", "-c", script]
    if exe == "env":
        program = _after_env(entrypoint[1:])
        if not program:
            return ["/bin/sh", "-c", script]
        return _for_entrypoint(program, script)
    if PYTHON.match(exe):
        # a python entrypoint may carry interpreter flags (-u, -s...), or already -c
        if entrypoint[1:] and entrypoint[-1] == "-c":
            return [_python_bootstrap(script)]
        # ... but not a module (-m), a script, or a code argument of its own
        if any(not arg.startswith("-") for arg in entrypoint[1:]):
            raise UnsupportedEntrypoint(entrypoint)
        return ["-c", _python_bootstrap(script)]
    raise UnsupportedEntrypoint(entrypoint)


def container_command(entrypoint, script: str, mode: str = "auto") -> List[str]:
    """The `containerOverrides.command` (Docker's Cmd, appended to the image ENTRYPOINT) that
    runs `script` under `entrypoint`.

    mode: "auto" (adapt to `entrypoint`), "none" (no entrypoint), "shell" (a shell: `-c` is
    appended), "sh-c" (a shell that already has `-c`), "python", or an entrypoint as JSON."""
    mode = str(mode or "auto").strip()
    if mode.lower() == "auto":
        return _for_entrypoint(entrypoint, script)
    if mode.lower() == "none":
        return _for_entrypoint(None, script)
    if mode.lower() == "shell":
        return ["-c", script]
    if mode.lower() == "sh-c":
        return [script]
    if mode.lower() == "python":
        return ["-c", _python_bootstrap(script)]
    if mode.startswith("["):
        try:
            return _for_entrypoint(json.loads(mode), script)
        except ValueError:
            raise ValueError(f"container entrypoint mode {mode!r} is not a JSON list")
    raise ValueError(
        f"unknown container entrypoint mode {mode!r}; use one of {', '.join(MODES)} or a JSON list"
    )


def command_for_image(
    image: str,
    script: str,
    mode: str = "auto",
    *,
    credentials: Optional[dict] = None,
    session=None,
    arch: str = "amd64",
    http=None,
    warn=None,
    info=None,
) -> List[str]:
    """The command for a job in `image`, reading the image's entrypoint when mode is "auto".

    An unreadable image config is a warning, not an error: the plugin falls back to the old
    behaviour (no entrypoint). An entrypoint that can't run the script raises
    UnsupportedEntrypoint."""
    mode = str(mode or "auto").strip()
    if mode.lower() != "auto":
        try:
            return container_command(None, script, mode)
        except ValueError as e:
            raise EntrypointLookupError(str(e)) from None
    try:
        entrypoint = resolve_entrypoint(
            image, credentials=credentials, session=session, arch=arch, http=http
        )
    except EntrypointLookupError as e:
        if warn and (image, arch, "warning") not in _LOGGED:
            _LOGGED.add((image, arch, "warning"))
            warn(
                f"could not read the ENTRYPOINT of image '{image}' ({e}); assuming it has none. "
                "If its jobs fail because the entrypoint cannot run /bin/sh, set "
                "aws-batch-flex-container-entrypoint (none, shell, sh-c, python) or use an image "
                "with ENTRYPOINT []."
            )
        return container_command(None, script, "none")
    if info and (image, arch) not in _LOGGED:
        _LOGGED.add((image, arch))
        if entrypoint:
            info(f"image '{image}' has ENTRYPOINT {entrypoint}; jobs are launched through it")
        else:
            info(f"image '{image}' has no ENTRYPOINT")
    try:
        return container_command(entrypoint, script, "auto")
    except UnsupportedEntrypoint as e:
        raise UnsupportedEntrypoint(e.entrypoint, image) from None
