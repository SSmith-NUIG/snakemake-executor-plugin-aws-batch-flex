"""Shell snippets that run before the Snakemake job command inside the container.

"host" mode uses the env mounted from the instance; the other modes install Snakemake into a
venv under $TMPDIR with uv. Either way Snakemake is only reachable through a shell function
called `python`, so the rule's own commands still see the image's python and PATH."""

import base64
import importlib.metadata as md
import shlex
import zlib
from typing import Iterable, List, Optional

MODES = ("auto", "always", "never", "host")

_TEMPLATE = r"""
set -eu
@@WORKDIR@@
SMK_BOOT="${TMPDIR:-/tmp}/.snakemake-batch-bootstrap"
SMK_NEED=1
@@CHECK@@
# the image's own matching Snakemake: still run it through the launcher (cache, usage figures)
if [ "$SMK_NEED" = 0 ]; then SMK_PY=$(command -v python); fi
if [ "$SMK_NEED" = 1 ]; then
  echo "[snakemake-batch] bootstrapping Snakemake into $SMK_BOOT" >&2
  mkdir -p "$SMK_BOOT"
  export UV_CACHE_DIR="$SMK_BOOT/cache" UV_PYTHON_INSTALL_DIR="$SMK_BOOT/python"
  if [ "@@PREREQ@@" = 1 ] && ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1 \
     && ! { command -v python3 >/dev/null 2>&1 && python3 -m pip --version >/dev/null 2>&1; }; then
    echo "[snakemake-batch] no curl/wget/pip in the image: installing curl, ca-certificates and bash" >&2
    if [ "$(id -u)" != 0 ]; then
      echo "[snakemake-batch] cannot install prerequisites: the image runs as a non-root user" >&2
    elif command -v apt-get >/dev/null 2>&1; then
      apt-get update -qq >&2 && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends curl ca-certificates bash >&2 || true
    elif command -v apk >/dev/null 2>&1; then apk add -q --no-cache curl ca-certificates bash >&2 || true
    elif command -v dnf >/dev/null 2>&1; then dnf install -y -q curl ca-certificates bash >&2 || true
    elif command -v microdnf >/dev/null 2>&1; then microdnf install -y curl ca-certificates bash >&2 || true
    elif command -v yum >/dev/null 2>&1; then yum install -y -q curl ca-certificates bash >&2 || true
    else echo "[snakemake-batch] no known package manager in the image" >&2
    fi
  fi
  UV=""
  if command -v uv >/dev/null 2>&1; then
    UV=uv
  else
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL https://astral.sh/uv/install.sh | UV_UNMANAGED_INSTALL="$SMK_BOOT/bin" sh >&2 || true
    elif command -v wget >/dev/null 2>&1; then
      wget -qO- https://astral.sh/uv/install.sh | UV_UNMANAGED_INSTALL="$SMK_BOOT/bin" sh >&2 || true
    fi
    if [ -x "$SMK_BOOT/bin/uv" ]; then
      UV="$SMK_BOOT/bin/uv"
    else
      for p in python3 python; do
        if command -v "$p" >/dev/null 2>&1 && "$p" -m pip install -q --target "$SMK_BOOT/uvlib" uv >&2; then
          UV="env PYTHONPATH=$SMK_BOOT/uvlib $p -m uv"
          break
        fi
      done
    fi
  fi
  if [ -z "$UV" ]; then
    echo "[snakemake-batch] cannot bootstrap: image needs curl, wget, or python3+pip (and internet access)" >&2
    exit 127
  fi
  $UV venv --clear --quiet --python @@PYVER@@ --system-site-packages "$SMK_BOOT/venv" >&2
  $UV pip install --quiet --python "$SMK_BOOT/venv/bin/python" @@REQS@@ >&2
  SMK_PY="$SMK_BOOT/venv/bin/python"
  python() { "$SMK_PY" "$@"; }
fi
"""


WORKDIR = (
    'SMK_WORK="${TMPDIR:-/tmp}/snakemake-job-$$"; mkdir -p "$SMK_WORK"; cd "$SMK_WORK"'
)

_HOST_TEMPLATE = r"""
set -eu
@@WORKDIR@@
SMK_ENV=@@ENV@@
SMK_WAITED=0
while [ ! -f "$SMK_ENV/.ready" ]; do
  if [ "$SMK_WAITED" -ge @@WAIT@@ ]; then
    echo "[snakemake-batch] host env $SMK_ENV is missing or not ready. Install it on the" >&2
    echo "  EC2 host/AMI (snakemake-batch-flex-host-env) and check the job definition mount." >&2
    exit 127
  fi
  sleep 2; SMK_WAITED=$((SMK_WAITED + 2))
done
SMK_PY="$SMK_ENV/venv/bin/python"
python() { "$SMK_PY" "$@"; }
if ! python -c "import snakemake" >/dev/null 2>&1; then
  echo "[snakemake-batch] the host env cannot run in this image (musl/Alpine? exotic libc?)." >&2
  exit 127
fi
"""

# Python `script:` rules normally run with Snakemake's own interpreter. This launcher makes them
# use the image's python (>= 3.10, which Snakemake's script header needs) when it has one, the
# same way Snakemake treats conda envs. Anything else falls through to the original code.
_LAUNCHER = r"""
import os, runpy, shutil, subprocess, sys
a = sys.argv[1:]
if a[:2] != ["-m", "snakemake"]:
    os.execv(sys.executable, [sys.executable] + a)
sys.argv = ["snakemake"] + a[2:]
_code = os.environ.get("SMK_FLEX_JOB_CODE")
if _code and "--deploy-sources" not in sys.argv:
    # the job tools (condacache.py, jobmonitor.py), uploaded by the plugin once per run: too
    # long to send with every job
    try:
        if _code.startswith("s3://"):
            import boto3
            _b, _, _k = _code[5:].partition("/")
            _src = boto3.client("s3").get_object(Bucket=_b, Key=_k)["Body"].read().decode()
        else:
            _src = open(_code).read()
        _ns = {"__name__": "smkflex_jobtools"}
        exec(_src, _ns)
        _ns["monitor"]()
        _cc = os.environ.get("SMK_FLEX_CONDA_CACHE", "none")
        if _cc != "none":
            sys.argv = ["snakemake"] + _ns["install"](sys.argv[1:], _cc)
    except Exception as e:
        print(f"[snakemake-batch] job tools off for this job (conda env cache, usage figures): {e!r}",
              file=sys.stderr)
if os.environ.get("SMK_FLEX_SCRIPT_PYTHON") == "host":
    runpy.run_module("snakemake", run_name="__main__", alter_sys=True)
    sys.exit()
found = []
def image_python():
    if not found:
        found.append(None)
        p = shutil.which("python") or shutil.which("python3")
        if p and os.path.realpath(p) != os.path.realpath(sys.executable):
            try:
                if subprocess.run([p, "-c", "import sys; sys.exit(sys.version_info < (3, 10))"]).returncode == 0:
                    found[0] = p
            except OSError:
                pass
    return found[0]
def own_packages():
    # Links to just this Snakemake's packages, put first on the script's path so that a
    # Snakemake installed in the image can't shadow them. Everything else: image first.
    import snakemake
    site = os.path.dirname(os.path.dirname(snakemake.__file__))
    d = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".smk-pkgs")
    if not os.path.isdir(d):
        os.makedirs(d)
        for n in os.listdir(site):
            if n.startswith("snakemake") and os.path.isdir(os.path.join(site, n)) and "." not in n:
                os.symlink(os.path.join(site, n), os.path.join(d, n))
    return d
try:
    from snakemake import script as sm_script
    orig = sm_script.PythonScript.execute_script
    def execute_script(self, fname, edit=False):
        if self.container_img is None and self.conda_env is None and self.env_modules is None and image_python():
            return self._execute_cmd(
                "PYTHONPATH={pkgs:q}${{PYTHONPATH:+:$PYTHONPATH}} {py_exec:q} {fname:q}",
                pkgs=own_packages(), py_exec=image_python(), fname=fname, is_python_script=True)
        return orig(self, fname, edit)
    sm_script.PythonScript.execute_script = execute_script
except Exception as e:
    print(f"[snakemake-batch] script: rules will use Snakemake's python ({e!r})", file=sys.stderr)
runpy.run_module("snakemake", run_name="__main__", alter_sys=True)
"""


def job_tools_source() -> str:
    """condacache.py and jobmonitor.py as one file, for the launcher to load in the job."""
    from pathlib import Path

    here = Path(__file__).parent
    return "\n\n".join((here / name).read_text() for name in ("condacache.py", "jobmonitor.py"))


def image_python_snippet() -> str:
    """Point the `python` function at the launcher above (only if the prelude defined one).
    The launcher is sent compressed: ECS allows only 8192 characters of overrides per job."""
    packed = base64.b64encode(zlib.compress(_LAUNCHER.encode(), 9)).decode()
    unpack = "import base64,sys,zlib;open(sys.argv[1],'wb').write(zlib.decompress(base64.b64decode(sys.argv[2])))"
    return (
        'if [ -n "${SMK_PY:-}" ]; then\n'
        f'  "$SMK_PY" -c "{unpack}" "$SMK_WORK/.smk-launch.py" {packed}\n'
        '  python() { "$SMK_PY" "$SMK_WORK/.smk-launch.py" "$@"; }\n'
        "fi\n"
    )


def _py_check(snakemake_version: str, package_names: Iterable[str]) -> str:
    code = (
        "import importlib.metadata as m\n"
        f"assert m.version('snakemake') == {snakemake_version!r}\n"
        + "".join(f"m.version({n!r})\n" for n in package_names)
    )
    return shlex.quote(code)


def _compact(script: str) -> str:
    """The script without indentation and blank lines: the job's command has to fit in
    ECS's 8192 characters, and sh doesn't need them."""
    return "\n".join(line.strip() for line in script.splitlines() if line.strip()) + "\n"


def build_prelude(
    mode: str,
    snakemake_version: str,
    requirements: list[str],
    python_version: str,
    install_prereqs: bool = False,
) -> str:
    """Return a POSIX-sh snippet; after it runs, `python -m snakemake` works."""
    if mode not in MODES:
        raise ValueError(f"bootstrap mode must be one of {MODES}, got {mode!r}")
    if mode == "never":
        return f"set -eu\n{WORKDIR}"

    names = [r.split("==")[0] for r in requirements]
    check = ""
    if mode == "auto":  # use the image's own Snakemake if it's this version
        check = (
            "if command -v python >/dev/null 2>&1 && "
            f"python -c {_py_check(snakemake_version, names)} >/dev/null 2>&1; "
            "then SMK_NEED=0; fi"
        )
    reqs = " ".join(shlex.quote(r) for r in requirements)
    return _compact(
        _TEMPLATE.replace("@@WORKDIR@@", WORKDIR)
        .replace("@@CHECK@@", check)
        .replace("@@PYVER@@", shlex.quote(python_version))
        .replace("@@PREREQ@@", "1" if install_prereqs else "0")
        .replace("@@REQS@@", reqs)
    )


def build_host_prelude(env_path: str, wait_seconds: int = 120) -> str:
    """Prelude for the host-mounted env. Waits for `.ready` because an instance that is
    still installing the env (user-data) may already have been given a job."""
    return _compact(
        _HOST_TEMPLATE.replace("@@WORKDIR@@", WORKDIR)
        .replace("@@ENV@@", shlex.quote(env_path))
        .replace("@@WAIT@@", str(int(wait_seconds)))
    )


def conda_snippet(conda_path: str) -> str:
    """Make a host-mounted conda usable in any image: only `condabin` (conda itself, no python)
    goes at the END of PATH, so an image's own conda wins; packages cache to a writable dir.
    The AMI's git (smkflex-bin, see hostenv) goes there too, for images without one."""
    return (
        f'export PATH="$PATH:{conda_path}/condabin:{conda_path}/smkflex-bin"\n'
        'export CONDA_PKGS_DIRS="$SMK_WORK/conda-pkgs"\n'
        '[ -w "${HOME:-/nonexistent}" ] || export HOME="$SMK_WORK"\n'
    )


def pinned(package: str) -> str:
    """A pip requirement for the version installed here (unpinned if it isn't installed)."""
    try:
        return f"{package}=={md.version(package)}"
    except md.PackageNotFoundError:
        return package


def storage_requirements(default_provider: Optional[str] = None) -> List[str]:
    """pip requirements for the storage plugins installed here, pinned to these versions."""
    packages = set()
    try:
        from snakemake_interface_storage_plugins.registry import StoragePluginRegistry

        reg = StoragePluginRegistry()
        packages = {reg.get_plugin_package_name(n) for n in reg.get_registered_plugins()}
    except Exception:  # registry API drift: fall back to the default provider
        if default_provider:
            packages = {f"snakemake-storage-plugin-{default_provider}"}
    return [pinned(p) for p in sorted(packages)]


def snakemake_prelude(s, mode: str, default_storage_provider: Optional[str] = None) -> str:
    """The prelude that gives a job Snakemake, for settings `s` (settings.ExecutorSettings):
    from the host env (mode "host") or installed at start, the same version as here."""
    if mode == "host":
        return build_host_prelude(s.host_env_path, s.host_env_wait_seconds)
    import sys

    version = md.version("snakemake")
    reqs = [f"snakemake=={version}"] + storage_requirements(default_storage_provider) + list(s.extra_packages or [])
    pyver = str(s.python_version or f"{sys.version_info[0]}.{sys.version_info[1]}")
    return build_prelude(mode, version, reqs, pyver, s.install_prereqs)
