"""Which files of the workflow folder go along: to each job (patches.py, for plain `snakemake`)
and into `smkflex run`'s bundle. Only the standard library: patches.py imports it in every run."""

import fnmatch
import os
import subprocess

SKIP_DIRS = {".snakemake", ".smkflex", ".git", "__pycache__"}
ALWAYS = "config"
# Top-level folders that never go along, whatever .gitignore says: input data and results, which
# jobs read from and write to storage (a local copy, e.g. from `smkflex pull`, isn't used there)
NEVER = {"data", "results", "logs", "benchmarks"}
# Snakemake leaves out bigger files itself (with a warning), so they don't count
SNAKEMAKE_MAX_FILE = 10_000_000


def _ignore_patterns(root):
    """Patterns from a .gitignore at the top of a folder that isn't a git checkout (simple
    globs: `*.bam`, `data/`, `results`; no negation)."""
    try:
        with open(os.path.join(root, ".gitignore")) as f:
            lines = [l.strip() for l in f]
    except OSError:
        return []
    return [l for l in lines if l and not l.startswith(("#", "!"))]


def _ignored(rel, patterns):
    parts = rel.split("/")
    for p in patterns:
        anchored, p = p.startswith("/"), p.strip("/")
        if "/" in p or anchored:
            if fnmatch.fnmatch(rel, p) or any(fnmatch.fnmatch("/".join(parts[:i]), p) for i in range(1, len(parts))):
                return True
        elif any(fnmatch.fnmatch(part, p) for part in parts):
            return True
    return False


def workflow_files(root):
    """Files to send: what git would track (tracked plus untracked, not ignored), or
    everything outside .snakemake/.git when the folder isn't a git checkout, minus what a
    .gitignore there lists. Never data/, results/, logs/ or benchmarks/ (NEVER). Plus config/
    even if ignored: every job reads its sample sheets, which people keep out of git when they
    name patients or private samples."""
    try:
        out = subprocess.run(["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=root,
                             capture_output=True, check=True).stdout.decode()
        files = [f for f in out.split("\0") if f]
    except (OSError, subprocess.CalledProcessError):
        patterns = _ignore_patterns(root)
        files = []
        for d, dirs, names in os.walk(root):
            dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
            files += [os.path.relpath(os.path.join(d, n), root) for n in names]
        files = [f for f in files if not _ignored(f, patterns)]
    for d, dirs, names in os.walk(os.path.join(root, ALWAYS)):
        dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
        files += [os.path.relpath(os.path.join(d, n), root) for n in names]
    return sorted(set(f for f in files
                  if not SKIP_DIRS.intersection(f.split("/")) and f.split("/")[0] not in NEVER
                  and os.path.isfile(os.path.join(root, f))))


def named_files(config, root="."):
    """Files in the folder that config values name (sample sheets, gene lists, templates),
    relative to it. Scripts often open them by path instead of as inputs, so jobs need them
    where the config says, even when git-ignored or under data/. Only small ones (Snakemake's
    per-file limit): a big file is data, which a rule reads from storage as an input."""
    found, todo = set(), [config]
    while todo:
        v = todo.pop()
        if isinstance(v, dict):
            todo += v.values()
        elif isinstance(v, (list, tuple)):
            todo += v
        elif isinstance(v, str) and 0 < len(v) < 1000 and "\n" not in v and "://" not in v:
            path = os.path.join(root, v)
            try:
                if os.path.isfile(path) and os.path.getsize(path) <= SNAKEMAKE_MAX_FILE:
                    rel = os.path.relpath(path, root)
                    if not rel.startswith(".."):
                        found.add(rel)
            except (OSError, ValueError):
                pass
    return found


def outside_links(files, root="."):
    """The files that are links a job couldn't follow: Snakemake sends a link as a link, so one
    that is absolute or points outside the folder arrives pointing at nothing."""
    here = os.path.realpath(root)
    out = []
    for f in sorted(files):
        path = os.path.join(root, f)
        if os.path.islink(path):
            target = os.path.realpath(path)
            if os.path.isabs(os.readlink(path)) or os.path.commonpath([here, target]) != here:
                out.append(f)
    return out


def too_big(files, limit_mb, root="."):
    """None, or what to say when `files` add up to more than limit_mb (MB)."""
    total, by_top = 0, {}
    for f in files:
        try:
            n = os.path.getsize(os.path.join(root, f))
        except OSError:
            continue
        if n > SNAKEMAKE_MAX_FILE:
            continue
        total += n
        top = f.split("/")[0] + "/" if "/" in f else "files at the top"
        by_top[top] = by_top.get(top, 0) + n
    if total <= limit_mb * 1e6:
        return None
    biggest = ", ".join(f"{d} {n / 1e6:.0f} MB" for d, n in sorted(by_top.items(), key=lambda kv: -kv[1])[:5])
    return (f"Every job gets the workflow folder, and it has {total / 1e6:.0f} MB to send ({biggest}); the limit "
            f"is {limit_mb} MB. Data belongs in storage: add folders like these to .gitignore (and upload "
            "what the rules read as inputs with smkflex push), or raise aws-batch-flex-max-sources-mb.")
