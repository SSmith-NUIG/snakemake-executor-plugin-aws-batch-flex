"""smkflex: the profile (as Snakemake finds it) and the AWS clients its commands use."""

import os
import sys

import boto3
import yaml
from botocore.config import Config as BotoConfig


class Fail(Exception):
    pass

class _Log:
    def info(self, msg):
        print(msg, file=sys.stderr)

    warning = info

def workflow_profile_file(create=False):
    """The project's workflow profile, as Snakemake finds it when started here: the first of
    default/, profiles/default/ and (next to workflow/Snakefile) workflow/profiles/default/ that
    exists. With create, profiles/default/config.yaml when there's none (Snakemake reads that
    for a Snakefile at the top or in workflow/)."""
    candidates = ["default", os.path.join("profiles", "default")]
    if os.path.isfile(os.path.join("workflow", "Snakefile")):
        candidates.append(os.path.join("workflow", "profiles", "default"))
    for d in candidates:
        if os.path.isdir(d):
            for name in ("profile.yaml", "config.yaml"):
                if os.path.isfile(os.path.join(d, name)):
                    return os.path.join(d, name)
            return os.path.join(d, "config.yaml")
    return os.path.join("profiles", "default", "config.yaml") if create else None

def _only_installed_profile():
    """The name of the one profile in ~/.config/snakemake that uses this plugin, if there is
    exactly one (setup.sh installs it there)."""
    base = os.path.join(os.environ.get("XDG_CONFIG_HOME") or os.path.expanduser("~/.config"), "snakemake")
    names = []
    try:
        entries = sorted(os.listdir(base))
    except OSError:
        return None
    for n in entries:
        for f in ("profile.yaml", "config.yaml"):
            cfg = os.path.join(base, n, f)
            if os.path.isfile(cfg):
                try:
                    with open(cfg) as fh:
                        if (yaml.safe_load(fh) or {}).get("executor") == "aws-batch-flex":
                            names.append(n)
                except (OSError, yaml.YAMLError):
                    pass
                break
    if len(names) == 1:
        print(f"(using profile {names[0]} from {base})", file=sys.stderr)
        return names[0]
    if names:
        raise Fail(f"there are {len(names)} profiles for this plugin in {base} ({', '.join(names)}): "
                   f"say which with --profile {names[0]} (or export SNAKEMAKE_PROFILE={names[0]})")
    return None

def _load_profile(path):
    """(profile folder, settings) for --profile: a folder, or a name that Snakemake looks up in
    ~/.config/snakemake/ (as `snakemake --profile NAME` does). Default: ./profile, then
    $SNAKEMAKE_PROFILE."""
    if path is None:
        path = "profile" if os.path.isdir("profile") else os.environ.get("SNAKEMAKE_PROFILE")
    if not path:
        path = _only_installed_profile()
    if not path:
        raise Fail("no profile: pass --profile (a folder with config.yaml, or a profile name)")
    found = None
    try:
        from snakemake.cli import get_profile_dir

        found = get_profile_dir(path)
    except ImportError:
        cfg = os.path.join(path, "config.yaml")
        found = (path, cfg) if os.path.isfile(cfg) else None
    if not found:
        raise Fail(f"profile {path} not found (looked for a folder with config.yaml or profile.yaml "
                   "here and in ~/.config/snakemake/)")
    folder, cfg = found
    with open(cfg) as f:
        settings = yaml.safe_load(f) or {}
    # The project's own workflow profile, which Snakemake loads on top (e.g. its storage prefix)
    wp = workflow_profile_file()
    if wp and os.path.isfile(wp):
        with open(wp) as f:
            settings.update(yaml.safe_load(f) or {})
    return str(folder), settings

def _clients(profile):
    cfg = BotoConfig(region_name=profile.get("aws-batch-flex-region"),
                     retries={"max_attempts": 10, "mode": "adaptive"})
    return boto3.client("batch", config=cfg), boto3.client("logs", config=cfg), cfg.region_name
