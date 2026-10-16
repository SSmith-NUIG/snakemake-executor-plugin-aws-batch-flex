"""Replaces boto3.client with an in-process fake AWS Batch that really executes the
submitted container command locally (via `sh -c`). Only for tests."""
import itertools, json, subprocess, tempfile, time, os
import boto3

LOG = os.environ.get("FAKEAWS_LOG")
_ids = itertools.count(1)


def _rec(kind, **kw):
    if LOG:
        with open(LOG, "a") as f:
            f.write(json.dumps({"call": kind, **kw}, default=str) + "\n")


class FakeBatch:
    def __init__(self):
        self.defs, self.jobs = {}, {}

    # Startup checks: every queue exists except "no-such-queue"; a queue whose name contains
    # "fargate" sits on a FARGATE compute environment, all others on EC2.
    def describe_job_queues(self, jobQueues=None):
        names = jobQueues or ["default-queue"]
        return {"jobQueues": [
            {"jobQueueName": q, "state": "ENABLED", "status": "VALID",
             "computeEnvironmentOrder": [{"order": 1, "computeEnvironment": f"ce-{q}"}]}
            for q in names if q != "no-such-queue"]}

    def describe_compute_environments(self, computeEnvironments):
        return {"computeEnvironments": [
            {"computeEnvironmentName": ce, "status": "VALID",
             "computeResources": {"type": "FARGATE" if "fargate" in ce else "EC2"}}
            for ce in computeEnvironments]}

    def describe_job_definitions(self, jobDefinitionName, status):
        d = self.defs.get(jobDefinitionName)
        return {"jobDefinitions": [d] if d else []}

    def register_job_definition(self, **kw):
        self.defs[kw["jobDefinitionName"]] = kw
        _rec("register_job_definition", image=kw["containerProperties"]["image"],
             name=kw["jobDefinitionName"], platform=kw.get("platformCapabilities"),
             mounts=kw["containerProperties"].get("mountPoints"),
             volumes=kw["containerProperties"].get("volumes"))
        return {}

    def submit_job(self, **kw):
        jid = f"job-{next(_ids)}"
        cmd = kw["containerOverrides"]["command"]
        env = dict(os.environ)
        if os.environ.get("FAKEAWS_JOB_PATH"): env["PATH"] = os.environ["FAKEAWS_JOB_PATH"]
        env.update({e["name"]: e["value"] for e in kw["containerOverrides"]["environment"]})
        env["TMPDIR"] = tempfile.mkdtemp(prefix="fakejob-")  # each real container has its own /tmp
        if os.environ.get("FAKEAWS_CMD"): open(os.environ["FAKEAWS_CMD"], "w").write(cmd[2])
        p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.jobs[jid] = dict(p=p, name=kw["jobName"], t=time.time(), kw=kw)
        _rec("submit_job", queue=kw["jobQueue"], jobdef=kw["jobDefinition"],
             res=kw["containerOverrides"]["resourceRequirements"], jobName=kw["jobName"],
             tags=kw.get("tags"), propagate=kw.get("propagateTags"),
             cmdtail=kw["containerOverrides"]["command"][2][-700:])
        return {"jobId": jid, "jobName": kw["jobName"]}

    def describe_jobs(self, jobs):
        out = []
        for jid in jobs:
            j = self.jobs[jid]
            rc = j["p"].poll()
            d = {"jobId": jid, "jobName": j["name"], "createdAt": int(j["t"] * 1000)}
            if rc is None:
                d["status"] = "RUNNING"
            else:
                d["status"] = "SUCCEEDED" if rc == 0 else "FAILED"
                j["out"] = j.get("out") or j["p"].stdout.read()
                if os.environ.get("FAKEAWS_OUT"): open(os.environ["FAKEAWS_OUT"], "a").write(j["out"] + "\n=====\n")
                d["container"] = {"exitCode": rc, "logStreamName": jid,
                                  "reason": "Essential container in task exited"}
            out.append(d)
        return {"jobs": out}

    def terminate_job(self, jobId, reason):
        self.jobs[jobId]["p"].kill()

    def _tail(self, jid):
        return (self.jobs[jid].get("out") or "").splitlines()[-40:]


class FakeLogs:
    def __init__(self, batch): self.b = batch
    def get_log_events(self, logGroupName, logStreamName, limit, startFromHead):
        return {"events": [{"message": l} for l in self.b._tail(logStreamName)]}


_batch = FakeBatch()
_real = boto3.client
def client(name, *a, **k):
    if name == "batch": return _batch
    if name == "logs": return FakeLogs(_batch)
    return _real(name, *a, **k)
boto3.client = client
