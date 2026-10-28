"""Builds the Snakemake env that gets baked into the AMI and mounted into jobs.

Python comes from uv's standalone builds and lives inside the env directory, so the env works
in any glibc image as long as it's mounted at the same path. `.ready` is written last and jobs
wait for it."""

import argparse
import email
import os
import re
import shlex
import sys
import textwrap
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

DEFAULT_PATH = "/opt/snakemake-env"

_SCRIPT = r"""#!/bin/bash
set -euo pipefail
ENV=@@ENV@@
mkdir -p "$ENV"
if [ -f "$ENV/.ready" ] && [ "$(cat "$ENV/.ready")" = @@STAMP@@ ]; then
  echo "snakemake host env already up to date at $ENV"; exit 0
fi
rm -f "$ENV/.ready"
export UV_CACHE_DIR="$ENV/.cache" UV_PYTHON_INSTALL_DIR="$ENV/python" UV_PYTHON_PREFERENCE=only-managed
UV=""
if command -v uv >/dev/null 2>&1; then UV=uv
else
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL https://astral.sh/uv/@@UVVER@@/install.sh | UV_UNMANAGED_INSTALL="$ENV/.uv" sh || true
  fi
  if [ -x "$ENV/.uv/uv" ]; then UV="$ENV/.uv/uv"
  else
    for p in python3 python; do
      if command -v "$p" >/dev/null 2>&1 && "$p" -m pip install -q --target "$ENV/.uvlib" uv==@@UVVER@@; then
        UV="env PYTHONPATH=$ENV/.uvlib $p -m uv"; break
      fi
    done
  fi
fi
[ -n "$UV" ] || { echo "need curl or python3+pip to install uv" >&2; exit 1; }
$UV python install @@PYVER@@
$UV venv --clear --python @@PYVER@@ "$ENV/venv"
# exactly the versions on the machine that made this script (see bootstrap.locked)
cat > "$ENV/constraints.txt" <<'SMK_PINS_EOF'
@@PINS@@
SMK_PINS_EOF
$UV pip install --python "$ENV/venv/bin/python" -c "$ENV/constraints.txt" @@REQS@@
"$ENV/venv/bin/python" -c "import snakemake; print('snakemake', snakemake.__version__)"
rm -rf "$ENV/.cache"
chmod -R a+rX "$ENV"
echo @@STAMP@@ > "$ENV/.ready"
echo "snakemake host env ready at $ENV"
"""


def _packages(storage_plugins: list[str]) -> list[str]:
    return ["snakemake"] + [f"snakemake-storage-plugin-{p}" for p in storage_plugins]


def default_requirements(storage_plugins: list[str], extra: list[str]) -> list[str]:
    from .bootstrap import pinned

    return [pinned(p) for p in _packages(storage_plugins)] + list(extra)


def default_constraints(storage_plugins: list[str]) -> list[str]:
    from .bootstrap import locked

    pins, skipped = locked(_packages(storage_plugins))
    if skipped:
        print(f"note: not pinned, since this machine's version doesn't fit what the packages ask for: "
              f"{', '.join(skipped)}", file=sys.stderr)
    return pins


def install_script(env_path: str, python_version: str, requirements: list[str],
                   constraints: list[str] = ()) -> str:
    from .bootstrap import UV_VERSION

    stamp = shlex.quote("|".join([python_version, *sorted(requirements), *sorted(constraints)]))
    return (
        _SCRIPT.replace("@@ENV@@", shlex.quote(env_path))
        .replace("@@STAMP@@", stamp)
        .replace("@@PYVER@@", shlex.quote(python_version))
        .replace("@@PINS@@", "\n".join(constraints))
        .replace("@@REQS@@", " ".join(shlex.quote(r) for r in requirements))
        .replace("@@UVVER@@", UV_VERSION)
    )


_CONDA = r"""#!/bin/bash
set -euo pipefail
ROOT=@ROOT@
if [ ! -x "$ROOT/bin/conda" ]; then
  mkdir -p "$(dirname "$ROOT")"
  case "$(uname -m)" in
    x86_64) SUM=@X86_64@ ;;
    aarch64) SUM=@AARCH64@ ;;
    *) echo "no Miniforge checksum for $(uname -m)" >&2; exit 1 ;;
  esac
  curl -fsSL -o /tmp/miniforge.sh "https://github.com/conda-forge/miniforge/releases/download/@MINIFORGE@/Miniforge3-@MINIFORGE@-Linux-$(uname -m).sh"
  echo "$SUM  /tmp/miniforge.sh" | sha256sum -c -
  bash /tmp/miniforge.sh -b -p "$ROOT"
  rm -f /tmp/miniforge.sh
fi
@ENVS@
# git for jobs whose image has none: Snakemake needs it for pipelines loaded from GitHub
# (module ... github(...), what snakedeploy sets up). conda-forge's git brings its own libraries,
# so it works in any image; smkflex-bin holds only a link to it, for the end of jobs' PATH.
if [ ! -x "$ROOT/envs/smkflex-git/bin/git" ]; then
  "$ROOT/bin/conda" create -y -q -p "$ROOT/envs/smkflex-git" -c conda-forge git
fi
mkdir -p "$ROOT/smkflex-bin" && ln -sf ../envs/smkflex-git/bin/git "$ROOT/smkflex-bin/git"
"$ROOT/bin/conda" clean -a -y -q || true
chmod -R a+rX "$ROOT"
echo "conda ready at $ROOT"
"""

# Miniforge for the AMI, with the sha256 of its installers (from its GitHub release)
MINIFORGE = "26.7.2-0"
MINIFORGE_SHA256 = {
    "X86_64": "281b0ac7d550802efc81af633225a5e6116d29ae72f3ab4eae7168c3931a4c05",
    "AARCH64": "89b786c8d2c8b0fda7553914c1314ae4ddaa094503802f279377b19ac4463cb2",
}

_CONDA_ENV = r"""cat > /tmp/env-@NAME@.yaml <<'SMK_ENV_EOF'
@YAML@
SMK_ENV_EOF
if [ -d "$ROOT/envs/@NAME@" ]; then "$ROOT/bin/conda" env update -n @NAME@ -f /tmp/env-@NAME@.yaml --prune -q
else "$ROOT/bin/conda" env create -n @NAME@ -f /tmp/env-@NAME@.yaml -q; fi"""


def conda_script(conda_path: str, envs: list) -> str:
    """Install Miniforge at conda_path and create the named environments (name, yaml text).
    Named environments land in <conda_path>/envs/<name>, so `conda: "<name>"` finds them."""
    blocks = []
    for name, yaml_text in envs:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            raise ValueError(f"invalid environment name {name!r}")
        blocks.append(_CONDA_ENV.replace("@NAME@", name).replace("@YAML@", yaml_text.rstrip()))
    script = _CONDA.replace("@ROOT@", shlex.quote(conda_path)).replace("@MINIFORGE@", MINIFORGE)
    for arch, sha in MINIFORGE_SHA256.items():
        script = script.replace(f"@{arch}@", sha)
    return script.replace("@ENVS@", "\n".join(blocks))


def launch_template_userdata(script: str, includes: tuple = ()) -> str:
    """MIME multipart user-data, the format AWS Batch launch templates require.

    `includes` are existing user-data documents (e.g. the one in your current launch
    template): multipart documents are merged part by part, untouched; a plain script is
    wrapped as a shell-script part. Our installer is appended as the LAST part.
    """
    msg = MIMEMultipart("mixed", boundary="==SMKBOUNDARY==")
    for text in includes:
        parsed = email.message_from_string(text)
        if parsed.is_multipart():
            for part in parsed.get_payload():
                msg.attach(part)
        elif text.lstrip().startswith("#cloud-config"):
            msg.attach(MIMEText(text, "cloud-config", "us-ascii"))
        else:
            msg.attach(MIMEText(text, "x-shellscript", "us-ascii"))
    msg.attach(MIMEText(script, "x-shellscript", "us-ascii"))
    del msg["MIME-Version"]  # re-added first, matching AWS's documented layout
    return "MIME-Version: 1.0\n" + msg.as_string()


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="snakemake-batch-flex-host-env",
        description=textwrap.dedent(__doc__).split("\n\n")[0],
    )
    ap.add_argument("what", choices=["script", "userdata", "conda"],
                    help="script: shell script to run on an instance/AMI build; "
                    "userdata: same, wrapped for an EC2 launch template; "
                    "conda: script that installs Miniforge and creates --conda-env environments")
    ap.add_argument("--path", default=DEFAULT_PATH, help=f"install dir (default {DEFAULT_PATH}); "
                    "must match --aws-batch-flex-host-env-path")
    ap.add_argument("--storage-plugin", action="append", default=[], metavar="NAME",
                    help="storage plugin to include, e.g. s3 (repeatable)")
    ap.add_argument("--extra-package", action="append", default=[], metavar="REQ",
                    help="additional pip requirement (repeatable)")
    ap.add_argument("--include", action="append", default=[], metavar="FILE",
                    help="userdata only: existing user-data file to keep (repeatable), e.g. the "
                    "decoded user-data of your current launch template. Our installer is "
                    "appended after it.")
    ap.add_argument("--conda-path", default="/opt/miniforge3", help="conda: install dir (mounted into jobs)")
    ap.add_argument("--conda-env", action="append", default=[], metavar="FILE",
                    help="conda: environment YAML to create (repeatable); the name is the file name")
    ap.add_argument("--python-version", default=f"{sys.version_info[0]}.{sys.version_info[1]}")
    a = ap.parse_args(argv)
    if a.what == "conda":
        envs = [(os.path.splitext(os.path.basename(f))[0], open(f).read()) for f in a.conda_env]
        sys.stdout.write(conda_script(a.conda_path, envs))
        return
    if a.include and a.what != "userdata":
        ap.error("--include only applies to 'userdata'")
    includes = []
    for f in a.include:
        try:
            includes.append(open(f, encoding="ascii").read())
        except (OSError, UnicodeDecodeError) as e:
            ap.error(f"cannot read --include {f}: {e} (user-data must be plain ASCII)")
    script = install_script(
        a.path, a.python_version, default_requirements(a.storage_plugin, a.extra_package),
        default_constraints(a.storage_plugin),
    )
    sys.stdout.write(
        launch_template_userdata(script, tuple(includes)) if a.what == "userdata" else script
    )


if __name__ == "__main__":
    main()
