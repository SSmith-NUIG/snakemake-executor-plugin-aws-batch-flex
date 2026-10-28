"""Jobs left running by an earlier run of the same workflow (Snakemake killed, laptop lost its
connection, ...). Snakemake would submit them again, so they'd run twice."""

import hashlib
import os

ACTIVE = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING")


def workflow_id(workdir: str, snakefile: str, storage_prefix: str) -> str:
    key = "|".join([os.path.abspath(workdir), os.path.abspath(snakefile), storage_prefix or ""])
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def list_jobs(batch, **kw) -> list:
    """The job summaries list_jobs(**kw) gives, from every page."""
    out, token = [], None
    while True:
        resp = batch.list_jobs(**kw, **({"nextToken": token} if token else {}))
        out += resp.get("jobSummaryList", [])
        token = resp.get("nextToken")
        if not token:
            return out


def describe_jobs(batch, ids) -> list:
    """The jobs described (with their tags and job definition), 100 at a time as Batch takes them."""
    ids, jobs = list(ids), []
    for i in range(0, len(ids), 100):
        jobs += batch.describe_jobs(jobs=ids[i : i + 100])["jobs"]
    return jobs


def active_jobs(batch, queue: str) -> list:
    """Every active job in `queue`, described. The summaries have no job definition or tags
    (despite the API model), so every one is described."""
    return describe_jobs(batch, [j["jobId"] for status in ACTIVE
                                 for j in list_jobs(batch, jobQueue=queue, jobStatus=status, maxResults=1000)])


def find_orphans(batch, queue: str, jobdef_prefix: str, wf_id: str, run_id: str) -> list:
    """[(job id, job name, status)] of active jobs in `queue` from this workflow but another run."""
    found = []
    for d in active_jobs(batch, queue):
        tags = d.get("tags") or {}
        jobdef = d.get("jobDefinition", "").rsplit("/", 1)[-1].split(":")[0]
        if (jobdef.startswith(jobdef_prefix + "-")
                and tags.get("snakemake-workflow") == wf_id
                and tags.get("snakemake-run-id") != run_id):
            found.append((d["jobId"], d.get("jobName", ""), d.get("status", "")))
    return found
