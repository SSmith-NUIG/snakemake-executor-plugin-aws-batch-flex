"""Jobs left running by an earlier run of the same workflow (Snakemake killed, laptop lost its
connection, ...). Snakemake would submit them again, so they'd run twice."""

import hashlib
import os

ACTIVE = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING")


def workflow_id(workdir: str, snakefile: str, storage_prefix: str) -> str:
    key = "|".join([os.path.abspath(workdir), os.path.abspath(snakefile), storage_prefix or ""])
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def find_orphans(batch, queue: str, jobdef_prefix: str, wf_id: str, run_id: str) -> list:
    """[(job id, job name, status)] of active jobs in `queue` from this workflow but another run."""
    ids = []
    for status in ACTIVE:
        token = None
        while True:
            kw = dict(jobQueue=queue, jobStatus=status, maxResults=1000)
            if token:
                kw["nextToken"] = token
            resp = batch.list_jobs(**kw)
            # The summaries have no job definition or tags (despite the API model), so
            # everything active is described below.
            ids += [j["jobId"] for j in resp.get("jobSummaryList", [])]
            token = resp.get("nextToken")
            if not token:
                break
    found = []
    for i in range(0, len(ids), 100):
        for d in batch.describe_jobs(jobs=ids[i : i + 100])["jobs"]:
            tags = d.get("tags") or {}
            jobdef = d.get("jobDefinition", "").rsplit("/", 1)[-1].split(":")[0]
            if (jobdef.startswith(jobdef_prefix + "-")
                    and tags.get("snakemake-workflow") == wf_id
                    and tags.get("snakemake-run-id") != run_id):
                found.append((d["jobId"], d.get("jobName", ""), d.get("status", "")))
    return found
