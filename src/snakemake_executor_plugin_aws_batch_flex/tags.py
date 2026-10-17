"""Job tags: global (--aws-batch-flex-tags), automatic, and per-rule (`batch_tags`).

Precedence, lowest to highest: automatic < global < per-rule.

AWS Batch tag limits: max 50 tags, key 1-128 chars, value up to 256, characters limited
to letters, digits, spaces and _ . : / = + - @ (anything else is replaced with `_`).
Keys starting with `aws:` are reserved.
"""

import re
from typing import Iterable, Mapping, Optional

from snakemake_interface_common.exceptions import WorkflowError

_BAD = re.compile(r"[^A-Za-z0-9 _.:/=+\-@]")
MAX_TAGS = 50


def _clean(text: str, limit: int) -> str:
    return _BAD.sub("_", str(text))[:limit]


def parse_pairs(items: Iterable[str], source: str) -> dict:
    """['k=v', ...] -> dict, validating each entry."""
    out = {}
    for item in items:
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise WorkflowError(f"Invalid tag '{item}' in {source}, expected KEY=VALUE.")
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            raise WorkflowError(f"Empty tag key in {source}: '{item}'.")
        if k.lower().startswith("aws:"):
            raise WorkflowError(f"Tag key '{k}' in {source} uses the reserved 'aws:' prefix.")
        out[_clean(k, 128)] = _clean(v.strip(), 256)
    return out


def parse_resource_string(value: Optional[str]) -> dict:
    """The `batch_tags` resource: 'k1=v1,k2=v2'."""
    if not value:
        return {}
    return parse_pairs(str(value).split(","), "resource batch_tags")


def job_tags(
    *,
    run_id: str,
    workflow_id: str = "",
    rule_names: Iterable[str],
    jobid: int,
    global_tags: Mapping[str, str],
    rule_tags: Mapping[str, str],
    auto: bool,
) -> dict:
    tags: dict = {}
    if auto:
        tags["snakemake-run-id"] = _clean(run_id, 256)
        if workflow_id:
            tags["snakemake-workflow"] = _clean(workflow_id, 256)
        tags["snakemake-rule"] = _clean("+".join(sorted(set(rule_names))), 256)
        tags["snakemake-job-id"] = _clean(str(jobid), 256)
    tags.update(global_tags)
    tags.update(rule_tags)
    if len(tags) > MAX_TAGS:
        raise WorkflowError(
            f"{len(tags)} tags exceed the AWS Batch limit of {MAX_TAGS}: {sorted(tags)}"
        )
    return tags
