"""Shell snippets that run before the Snakemake job command inside the container.

"host" mode uses the env mounted from the instance; the other modes install Snakemake into a
venv under $TMPDIR with uv. Either way Snakemake is only reachable through a shell function
called `python`, so the rule's own commands still see the image's python and PATH."""

import base64
import importlib.metadata as md
import re
import shlex
import textwrap
import zlib
from typing import Iterable, List, Optional

MODES = ("auto", "always", "never", "host")
# uv for jobs that install Snakemake at start, and for the AMI's env; its installer checks the
# download's sha256
UV_VERSION = "0.13.0"

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
@@PREREQS@@
@@UV@@
  if [ -z "$UV" ]; then
    echo "[snakemake-batch] cannot bootstrap: image needs curl, wget, or python3+pip (and internet access)" >&2
    exit 127
  fi
  $UV venv --clear --quiet --python @@PYVER@@ --system-site-packages "$SMK_BOOT/venv" >&2
  $UV pip install --quiet --python "$SMK_BOOT/venv/bin/python" @@NEWER@@@@REQS@@ >&2
  SMK_PY="$SMK_BOOT/venv/bin/python"
  python() { "$SMK_PY" "$@"; }
fi
"""


def uv_snippet(bin_dir: str, lib_dir: str) -> str:
    """Shell that sets UV to a uv to run: the image's own, else version UV_VERSION from its
    installer (curl or wget; it checks the download's sha256) into bin_dir, else from pip into
    lib_dir. UV stays empty if none of them worked. For jobs that install Snakemake at start
    and for the AMI's env (hostenv.py)."""
    return f"""UV=""
if command -v uv >/dev/null 2>&1; then
  UV=uv
else
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL https://astral.sh/uv/{UV_VERSION}/install.sh | UV_UNMANAGED_INSTALL="{bin_dir}" sh >&2 || true
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- https://astral.sh/uv/{UV_VERSION}/install.sh | UV_UNMANAGED_INSTALL="{bin_dir}" sh >&2 || true
  fi
  if [ -x "{bin_dir}/uv" ]; then
    UV="{bin_dir}/uv"
  else
    for p in python3 python; do
      if command -v "$p" >/dev/null 2>&1 && "$p" -m pip install -q --target "{lib_dir}" uv=={UV_VERSION} >&2; then
        UV="env PYTHONPATH={lib_dir} $p -m uv"
        break
      fi
    done
  fi
fi"""


# For an image without curl, wget or pip (only with install_prereqs): uv from the image's own
# conda, mamba or micromamba into $SMK_BOOT, which works as any user and leaves the image's
# environments alone; else curl from its package manager, which needs root
_PREREQS = r'''
  if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1 \
     && ! { command -v python3 >/dev/null 2>&1 && python3 -m pip --version >/dev/null 2>&1; }; then
    SMK_CONDA=$(command -v micromamba || command -v mamba || command -v conda || true)
    if [ -n "$SMK_CONDA" ]; then
      echo "[snakemake-batch] no curl/wget/pip in the image: getting uv with its $SMK_CONDA" >&2
      CONDA_PKGS_DIRS="$SMK_BOOT/pkgs" "$SMK_CONDA" create -y -q -p "$SMK_BOOT/uvenv" -c conda-forge "uv=@@UVVER@@" >&2 \
        && mkdir -p "$SMK_BOOT/bin" && ln -sf "$SMK_BOOT/uvenv/bin/uv" "$SMK_BOOT/bin/uv" || true
    elif [ "$(id -u)" != 0 ]; then
      echo "[snakemake-batch] no curl/wget/pip/conda in the image, and it runs as a non-root user, so none can be installed. Add curl to the image (or USER root), or run the rule on EC2 (batch_queue), where Snakemake comes from the AMI" >&2
    else
      echo "[snakemake-batch] no curl/wget/pip in the image: installing curl, ca-certificates and bash" >&2
      if command -v apt-get >/dev/null 2>&1; then
        apt-get update -qq >&2 && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends curl ca-certificates bash >&2 || true
      elif command -v apk >/dev/null 2>&1; then apk add -q --no-cache curl ca-certificates bash >&2 || true
      elif command -v dnf >/dev/null 2>&1; then dnf install -y -q curl ca-certificates bash >&2 || true
      elif command -v microdnf >/dev/null 2>&1; then microdnf install -y curl ca-certificates bash >&2 || true
      elif command -v yum >/dev/null 2>&1; then yum install -y -q curl ca-certificates bash >&2 || true
      else echo "[snakemake-batch] no known package manager in the image" >&2
      fi
    fi
  fi
'''


# The job works in a folder at the same path as the one Snakemake runs in (SMK_FLEX_WORKDIR), so
# that a Snakefile's os.path.abspath() gives the same paths in the job, as on a cluster's shared
# filesystem (zarp resolves its inputs so). Only where that's safe: not /, not a system or mount
# folder, and not a folder the image already has files in; otherwise a folder under /tmp.
WORKDIR = r"""SMK_WORK="${TMPDIR:-/tmp}/snakemake-job-$$"
case "${SMK_FLEX_WORKDIR:-/}" in
/|/opt|/opt/*|/usr*|/etc*|/bin*|/sbin*|/lib*|/proc*|/sys*|/dev*|/boot*|/var/lib*|/run*) ;;
*) if [ ! -e "$SMK_FLEX_WORKDIR" ] || [ -z "$(ls -A "$SMK_FLEX_WORKDIR" 2>/dev/null)" ]; then
mkdir -p "$SMK_FLEX_WORKDIR" 2>/dev/null && [ -w "$SMK_FLEX_WORKDIR" ] && SMK_WORK=$SMK_FLEX_WORKDIR \
&& export SMK_FLEX_SAME_PATH=1; fi ;;
esac
mkdir -p "$SMK_WORK"; cd "$SMK_WORK"
"""

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
if "monitor" in globals() and "--deploy-sources" not in sys.argv:
    # the job tools came with this launcher, in the same file (job_tools_source)
    try:
        make_numpy_loadable()
        _why = parallel_s3_directories()
        if _why:
            print(f"[snakemake-batch] directory() outputs on S3 move one file at a time ({_why})", file=sys.stderr)
        if os.environ.get("SMK_FLEX_SAME_PATH") == "1":
            _why = plain_local_paths(sys.argv)
            if _why:
                print(f"[snakemake-batch] storage files' local copies stay under .snakemake/storage ({_why})",
                      file=sys.stderr)
        monitor()
        _cc = os.environ.get("SMK_FLEX_CONDA_CACHE", "none")
        if _cc != "none":
            sys.argv = ["snakemake"] + install(sys.argv[1:], _cc)
    except Exception as e:
        print(f"[snakemake-batch] job tools off for this job (conda env cache, usage figures): {e!r}",
              file=sys.stderr)
try:
    # Snakemake hands scripts their params in a form any environment can load: DataFrame, Series
    # and numpy arrays become lists. In pandas 3 a text column's .values is a pandas array
    # (StringArray), not a numpy one, so it went to the script as is, and a script environment
    # with pandas 2 can't load it. It goes as the numpy array pandas 2 gave.
    import inspect
    try:
        from snakemake.iocontainers import Snakemake as SmkData
    except ImportError:  # earlier Snakemake 9 releases had it in snakemake.script
        from snakemake.script import Snakemake as SmkData
    orig_store = SmkData._safely_store_params
    if list(inspect.signature(orig_store).parameters) != ["self", "params"]:
        raise RuntimeError("this Snakemake's Snakemake._safely_store_params changed")
    def _safely_store_params(self, params):
        try:
            import numpy as np
            from pandas import StringDtype
            from pandas.api.extensions import ExtensionArray
        except ImportError:
            return orig_store(self, params)
        def text(v):  # only text: other pandas arrays (Categorical...) were pandas objects in pandas 2 too
            return isinstance(v, ExtensionArray) and isinstance(v.dtype, StringDtype)
        if any(text(v) for v in params):
            params = params.__class__(toclone=params)
            for i, v in enumerate(params):
                if text(v):
                    params[i] = np.asarray(v, dtype=object)
        return orig_store(self, params)
    SmkData._safely_store_params = _safely_store_params
except Exception as e:
    print(f"[snakemake-batch] pandas arrays in params go to scripts unchanged ({e!r})", file=sys.stderr)
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
has = {}
def image_lacks(params):
    # Snakemake hands a script its pandas, numpy and polars params as such (top-level values,
    # iocontainers._safely_store_params), so the script's python needs them just to start.
    # Locally such scripts usually run in Snakemake's own python, which has them.
    missing = []
    for v in params or []:
        m = type(v).__module__.split(".")[0]
        if m in ("pandas", "numpy", "polars") and m not in missing:
            if m not in has:
                has[m] = subprocess.run([image_python(), "-c", "import " + m], capture_output=True).returncode == 0
            if not has[m]:
                missing.append(m)
    return missing
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
    import inspect
    from snakemake import script as sm_script
    orig = sm_script.PythonScript.execute_script
    if list(inspect.signature(orig).parameters) != ["self", "fname", "edit"]:
        raise RuntimeError("this Snakemake's PythonScript.execute_script changed")
    def execute_script(self, fname, edit=False):
        if self.container_img is None and self.conda_env is None and self.env_modules is None and image_python():
            missing = image_lacks(self.params)
            if missing:
                print(f"[snakemake-batch] this script runs in Snakemake's python: its params are {', '.join(missing)} "
                      "objects, which the image's python can't load", file=sys.stderr)
                return orig(self, fname, edit)
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
    """The file every job downloads and runs (launcher_snippet): condacache.py, jobmonitor.py and
    jobfixes.py, then the launcher. Too long to send with each job; uploaded once per run."""
    from pathlib import Path

    here = Path(__file__).parent
    tools = [(here / name).read_text() for name in ("condacache.py", "jobmonitor.py", "jobfixes.py")]
    # the launcher runs when the job's loader runs the file, not when the tools are only loaded
    launcher = 'if __name__ == "smkflex_launcher":\n' + textwrap.indent(_LAUNCHER, "    ")
    return "\n\n".join(tools + [launcher])


# Fetches the job tools (with the launcher) named in SMK_FLEX_JOB_CODE, checks them against the
# sha256 in their name, and runs them; else plain Snakemake. In the job's command, so short.
_LOADER = (
    "import hashlib,os,runpy,sys\n"
    "u=os.environ.get(\"SMK_FLEX_JOB_CODE\");a=sys.argv[1:];d=None\n"
    "if a[:2]!=[\"-m\",\"snakemake\"]:os.execv(sys.executable,[sys.executable]+a)\n"
    "if u and \"--deploy-sources\" not in a:\n"
    " try:\n"
    "  if u.startswith(\"s3://\"):\n"
    "   import boto3;b,_,k=u[5:].partition(\"/\");d=boto3.client(\"s3\").get_object(Bucket=b,Key=k)[\"Body\"].read()\n"
    "  else:d=open(u,\"rb\").read()\n"
    "  if not u.endswith(\"jobtools-%s.py\"%hashlib.sha256(d).hexdigest()):raise ValueError(u+\" does not match its checksum\")\n"
    " except Exception as e:\n"
    "  d=None;print(\"[snakemake-batch] job tools off for this job (conda env cache, usage figures, image python for script:): %r\"%e,file=sys.stderr)\n"
    "if d:exec(compile(d,\"smkflex-jobtools.py\",\"exec\"),{\"__name__\":\"smkflex_launcher\",\"__file__\":os.path.join(os.getcwd(),\".smk-launch.py\")})\n"
    "else:sys.argv=[\"snakemake\"]+a[2:];runpy.run_module(\"snakemake\",run_name=\"__main__\",alter_sys=True)\n"
)


def launcher_snippet(job_code: bool) -> str:
    """Point the `python` function (if the prelude defined one) at the launcher: the short loader
    above when the job tools were uploaded (job_code), else the launcher itself, compressed (ECS
    allows only 8192 characters of overrides per job)."""
    if job_code:
        return 'if [ -n "${SMK_PY:-}" ]; then\n' f'  python() {{ "$SMK_PY" -c {shlex.quote(_LOADER)} "$@"; }}\n' "fi\n"
    return image_python_snippet()


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
    resolved_at: Optional[str] = None,
) -> str:
    """Return a POSIX-sh snippet; after it runs, `python -m snakemake` works. With resolved_at
    (an RFC 3339 time), packages are installed as they were then: the same versions in every
    job of a run, however long it takes."""
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
        .replace("@@PREREQS@@", _PREREQS.replace("@@UVVER@@", UV_VERSION) if install_prereqs else "")
        .replace("@@REQS@@", reqs)
        .replace("@@NEWER@@", f"--exclude-newer {shlex.quote(resolved_at)} " if resolved_at else "")
        .replace("@@UV@@", uv_snippet("$SMK_BOOT/bin", "$SMK_BOOT/uvlib"))
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
    The AMI's git, curl and wget (smkflex-bin, see hostenv) go there too, for images without them."""
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


def locked(packages: Iterable[str]):
    """(pins, skipped): name==version of everything installed here that `packages` need, all
    the way down, so the AMI's env has exactly this machine's versions; for a constraints file,
    so pins the env's Python doesn't need are left alone. A version that doesn't fit what the
    packages ask for (an environment mixed by hand) isn't pinned: it's in `skipped`."""
    from packaging.requirements import Requirement
    from packaging.specifiers import SpecifierSet

    def key(name):
        return re.sub(r"[-_.]+", "-", name).lower()

    found, wanted, todo = {}, {}, list(packages)
    while todo:
        name = todo.pop()
        if key(name) in found:
            continue
        try:
            dist = md.distribution(name)
        except md.PackageNotFoundError:
            continue
        found[key(name)] = dist.version
        for line in dist.requires or []:
            req = Requirement(line)
            if req.marker is None or req.marker.evaluate({"extra": ""}):
                wanted[key(req.name)] = wanted.get(key(req.name), SpecifierSet()) & req.specifier
                todo.append(req.name)
    pins, skipped = [], []
    for k, v in sorted(found.items()):
        # a local build ("+...") isn't on PyPI
        fits = "+" not in v and wanted.get(k, SpecifierSet()).contains(v, prereleases=True)
        (pins if fits else skipped).append(f"{k}=={v}")
    return pins, skipped


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


def snakemake_prelude(s, mode: str, default_storage_provider: Optional[str] = None,
                      resolved_at: Optional[str] = None) -> str:
    """The prelude that gives a job Snakemake, for settings `s` (settings.ExecutorSettings):
    from the host env (mode "host") or installed at start, the same version as here, with the
    other packages as they were at resolved_at."""
    if mode == "host":
        return build_host_prelude(s.host_env_path, s.host_env_wait_seconds)
    import sys

    version = md.version("snakemake")
    reqs = [f"snakemake=={version}"] + storage_requirements(default_storage_provider) + list(s.extra_packages or [])
    pyver = str(s.python_version or f"{sys.version_info[0]}.{sys.version_info[1]}")
    return build_prelude(mode, version, reqs, pyver, s.install_prereqs, resolved_at)
