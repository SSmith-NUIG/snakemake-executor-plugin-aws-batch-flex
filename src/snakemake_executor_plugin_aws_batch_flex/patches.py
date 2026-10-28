"""Workarounds for bugs in Snakemake and the S3 storage plugin, each limited to what this
plugin needs. They can go once the bugs are fixed upstream."""

import re

from snakemake_interface_common.exceptions import WorkflowError

from .settings import MAX_SOURCES_MB, PLUGIN_NAME
from .sources import too_big, workflow_files


def skip_remote_module_scripts():
    """Two fixes to what Snakemake packs for the jobs. (1) Work around Snakemake (9.27): packing the workflow's sources for the jobs crashes with
    "TypeError: ... not GithubFile" when a rule comes from a module fetched from GitHub (as
    snakedeploy sets up), because it treats the rule's script as a local file. Those scripts
    don't need packing: each job fetches them from GitHub itself. So they're hidden while the
    sources are collected. (2) The whole workflow folder is packed, see below."""
    try:
        from snakemake.dag import DAG
        from snakemake.sourcecache import LocalSourceFile, SourceFile
    except Exception:
        return
    orig = getattr(DAG, "get_sources", None)
    if orig is None or getattr(orig, "_skips_remote", False):
        return

    def get_sources(self):
        # Snakemake imports every installed executor plugin, so this runs for all of them:
        # leave other executors' runs as they are
        plugin = getattr(getattr(self, "workflow", None), "executor_plugin", None)
        if getattr(plugin, "name", None) != PLUGIN_NAME:
            return get_sources.__wrapped__(self)
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


def recheck_s3_directories():
    """Work around snakemake-storage-plugin-s3 (0.3.6): a storage object remembers is_dir() =
    False from before the job ran, so a directory() output the job then uploads is reported
    "missing in storage". A remembered False is looked up again; a True still counts."""
    try:
        from snakemake_storage_plugin_s3 import StorageObject
    except Exception:
        return
    orig = getattr(StorageObject, "is_dir", None)
    if orig is None or getattr(orig, "_rechecks", False):
        return

    def is_dir(self):
        if getattr(self, "_is_dir", None) is False:
            self._is_dir = None
        return orig(self)

    is_dir._rechecks = True
    StorageObject.is_dir = is_dir


def keep_unneeded_temp(cmd: str) -> str:
    """Drop --unneeded-temp-files from a job's command. Snakemake (9.x) tells the job to delete
    temp() outputs that no other job needs, but then waits for every output in storage after
    the job, and fails with "Missing files". Without the option the job uploads them, and
    Snakemake removes them from storage itself as with any other temp file."""
    return re.sub(r" --unneeded-temp-files(?: (?!-)(?:'[^']*'|\S+))+", "", cmd)
