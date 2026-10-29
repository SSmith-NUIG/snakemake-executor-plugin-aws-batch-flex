"""Workarounds for bugs in Snakemake and the S3 storage plugin, each limited to what this
plugin needs. They can go once the bugs are fixed upstream.

Each one first checks that the code it patches is still what it was written for (the
parameters it takes); if a new version changed it, the workaround isn't applied and the
executor says so at start (NOT_APPLIED), instead of failing quietly or calling it wrongly.
tests/test_units.py fails as soon as one doesn't apply, and CI runs the newest Snakemake weekly."""

import inspect
import os
import re

from snakemake_interface_common.exceptions import WorkflowError

from .settings import MAX_SOURCES_MB, PLUGIN_NAME
from .sources import too_big, workflow_files


SOURCES = ("sending the workflow folder to the jobs (Snakemake's DAG.get_sources): jobs get only "
           "what git knows about")
MISSING = "the hint for inputs that are only on this machine (Snakemake's MissingInputException)"
S3_DIRS = ("directory() outputs on S3 (the S3 storage plugin's StorageObject.exists): they can be "
           "reported missing after the job made them, or stop the run after a failed job removed them")
# Workarounds this Snakemake (or S3 plugin) doesn't let us apply; the executor warns about them
NOT_APPLIED: list = []


def fits(func, params, what) -> bool:
    """Does func take these parameters first, as the code a workaround was written for did?
    If not, `what` goes into NOT_APPLIED."""
    try:
        ok = list(inspect.signature(func).parameters)[: len(params)] == list(params)
    except (TypeError, ValueError):
        ok = False
    if not ok and what not in NOT_APPLIED:
        NOT_APPLIED.append(what)
    return ok


def skip_remote_module_scripts():
    """Two fixes to what Snakemake packs for the jobs. (1) Work around Snakemake (9.27): packing the workflow's sources for the jobs crashes with
    "TypeError: ... not GithubFile" when a rule comes from a module fetched from GitHub (as
    snakedeploy sets up), because it treats the rule's script as a local file. Those scripts
    don't need packing: each job fetches them from GitHub itself. So they're hidden while the
    sources are collected. (2) The whole workflow folder is packed, see below."""
    try:
        from snakemake.dag import DAG
        from snakemake.sourcecache import LocalSourceFile, SourceFile
    except ImportError:
        NOT_APPLIED.append(SOURCES)
        return
    orig = getattr(DAG, "get_sources", None)
    if getattr(orig, "_skips_remote", False) or not fits(orig, ["self"], SOURCES):
        return

    def get_sources(self):
        # Snakemake imports every installed executor plugin, so this runs for all of them:
        # leave other executors' runs as they are
        plugin = getattr(getattr(self, "workflow", None), "executor_plugin", None)
        if getattr(plugin, "name", None) != PLUGIN_NAME:
            return get_sources.__wrapped__(self)
        # Jobs only get files below the folder Snakemake works in; Snakemake leaves the rest out
        # with a warning, and every job then fails for want of the Snakefile
        main = str(getattr(self.workflow, "main_snakefile", "") or "")
        if main and os.path.exists(main) and os.path.relpath(main).startswith(".."):
            raise WorkflowError(
                f"The Snakefile ({os.path.relpath(main)}) is outside the folder Snakemake works in "
                f"({os.getcwd()}), set by a workdir: directive or --directory, so the jobs couldn't get it. "
                "Remove workdir: and start Snakemake in the workflow's folder: outputs go under the "
                "storage prefix anyway (a project of its own: smkflex init in its own folder)."
            )
        hidden = []
        for rule in getattr(self.workflow, "rules", []):
            base = getattr(rule, "basedir", None)
            if isinstance(base, SourceFile) and not isinstance(base, LocalSourceFile):
                for attr in ("script", "notebook"):
                    value = getattr(rule, attr, None)
                    if value:
                        hidden.append((rule, attr, value))
                        setattr(rule, attr, None)
        try:
            files = get_sources.__wrapped__(self)
        finally:
            for rule, attr, value in hidden:
                setattr(rule, attr, value)
        # Snakemake sends jobs only what git knows about, besides the Snakefile and the files
        # named in script:/conda:. Scripts a shell: command runs, modules the Snakefile imports,
        # sample sheets and the like are missing in a folder that isn't a git checkout (such as
        # one made by snakedeploy), or when never `git add`ed. Send the folder as smkflex run
        # does: what git would track, or everything .gitignore doesn't exclude, and config/.
        if isinstance(files, set):
            files.update(workflow_files("."))
            # git still lists a tracked file deleted here (not committed yet), and Snakemake
            # then fails to pack it
            files.difference_update([f for f in files if not os.path.exists(f)])
            settings = getattr(self.workflow, "executor_settings", None)
            limit = getattr(settings, "max_sources_mb", None) or MAX_SOURCES_MB
            problem = too_big(files, limit)
            if problem:
                raise WorkflowError(problem)
        return files

    get_sources._skips_remote = True
    get_sources.__wrapped__ = orig
    DAG.get_sources = get_sources


# Snakemake packs the sources before it creates the executor, so this can't wait for it.
skip_remote_module_scripts()


def explain_local_only_inputs():
    """Snakemake only says "Missing input files ... (in storage)" for an input that is on this
    machine but was never uploaded (data/, or an absolute path): jobs read their inputs from
    storage. Say so, with the smkflex push command that uploads it."""
    try:
        from snakemake.exceptions import MissingInputException
    except ImportError:
        NOT_APPLIED.append(MISSING)
        return
    orig = MissingInputException.__init__
    if getattr(orig, "_smkflex", False) or not fits(orig, ["self", "job", "files"], MISSING):
        return

    def __init__(self, job, files, *args, **kwargs):
        orig(self, job, files, *args, **kwargs)
        try:
            hint = local_only_hint(job.rule.workflow, files)
        except Exception:  # only an extra line: never let it break Snakemake's own error
            hint = None
        if hint and self.args:
            self.args = (self.args[0] + hint, *self.args[1:])

    __init__._smkflex = True
    MissingInputException.__init__ = __init__


def local_only_hint(workflow, files):
    if getattr(getattr(workflow, "executor_plugin", None), "name", None) != PLUGIN_NAME:
        return None
    prefix = str(getattr(workflow.storage_settings, "default_storage_prefix", "") or "").rstrip("/")
    here, stored = [], False
    for f in files:
        query = getattr(getattr(f, "storage_object", None), "query", None) if getattr(f, "is_storage", False) else None
        if not prefix or not query or not str(query).startswith(prefix + "/"):
            continue
        stored = True
        rel = str(query)[len(prefix) + 1:]
        # a relative path goes to prefix/path, an absolute one to prefix/path-without-its-/
        for local in (rel, "/" + rel):
            if os.path.exists(local):
                here.append(local)
                break
    if not here:
        return (f"\n    Jobs read their inputs from {prefix}/, not from this machine: upload them there, "
                "at the paths your rules and sample sheets use, with: smkflex push <files or folders>"
                if stored else None)
    push = sorted({os.path.dirname(p) if os.path.isabs(p) else p.split("/")[0] for p in here})
    files_text = ", ".join(here[:5]) + (f" and {len(here) - 5} more" if len(here) > 5 else "")
    return (f"\n    On this machine but not in storage, where the jobs read their inputs: {files_text}. "
            f"Upload with: smkflex push {' '.join(push)}")


explain_local_only_inputs()


def recheck_s3_directories():
    """Work around snakemake-storage-plugin-s3 (0.3.6): a storage object remembers its is_dir()
    answer, which a job then makes wrong. Remembered False: a directory() output the job
    uploads is reported "missing in storage". Remembered True: a failed job's directory output,
    which Snakemake removes, still exists, and its mtime() fails on the empty listing
    ("max() iterable argument is empty"), which stops the run. exists() looks it up again
    (it only asks is_dir() when there's no plain object of that name), and when mtime() or
    size() fail, Snakemake asks exists() and takes the file as missing. Only there: mtime() and
    size() of every file ask is_dir() too, and keep the remembered answer, as without the patch."""
    try:
        from snakemake_storage_plugin_s3 import StorageObject
    except ImportError:  # not installed: nothing to work around
        return
    orig = getattr(StorageObject, "exists", None)
    if getattr(orig, "_rechecks", False) or not fits(orig, ["self"], S3_DIRS):
        return
    # what it works around: the remembered is_dir() answer, in _is_dir
    if "_is_dir" not in inspect.getsource(StorageObject):
        NOT_APPLIED.append(S3_DIRS)
        return

    def exists(self):
        self._is_dir = None
        return orig(self)

    exists._rechecks = True
    StorageObject.exists = exists


def keep_unneeded_temp(cmd: str) -> str:
    """Drop --unneeded-temp-files from a job's command. Snakemake (9.x) tells the job to delete
    temp() outputs that no other job needs, but then waits for every output in storage after
    the job, and fails with "Missing files". Without the option the job uploads them, and
    Snakemake removes them from storage itself as with any other temp file."""
    return re.sub(r" --unneeded-temp-files(?: (?!-)(?:'[^']*'|\S+))+", "", cmd)
