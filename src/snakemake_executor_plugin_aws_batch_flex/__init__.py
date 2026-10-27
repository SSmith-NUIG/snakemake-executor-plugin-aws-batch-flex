"""AWS Batch executor for Snakemake that runs each rule in the image from its
`container:` directive. Images don't need Snakemake installed."""

from . import patches  # noqa: F401  (Snakemake packs the sources before it creates the executor)
from .executor import Executor
from .settings import ExecutorSettings, common_settings

__all__ = ["Executor", "ExecutorSettings", "common_settings"]
