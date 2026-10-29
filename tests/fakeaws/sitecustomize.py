"""Replaces boto3.client with an in-process fake AWS Batch that really executes the
submitted container command locally (via `sh -c`). Only for tests."""
import email.message, io, itertools, json, re, subprocess, tempfile, time, os, types
import urllib.request, urllib.response
import boto3

LOG = os.environ.get("FAKEAWS_LOG")
# Images' ENTRYPOINTs (FAKEAWS_ENTRYPOINTS: JSON {image: [...]}; others have none). A fake
# registry tells the plugin, and jobs run ENTRYPOINT + command, as Docker does.
ENTRYPOINTS = json.loads(os.environ.get("FAKEAWS_ENTRYPOINTS") or "{}")
_ids = itertools.count(1)


def _rec(kind, **kw):
    if LOG:
        with open(LOG, "a") as f:
            f.write(json.dumps({"call": kind, **kw}, default=str) + "\n")


class FakeBatch:
    def __init__(self):
        self.meta = types.SimpleNamespace(region_name="eu-west-1")
        self.defs, self.jobs = {}, {}
        # Jobs "left over" from an earlier run: FAKEAWS_ORPHANS = JSON list of
        # {"jobId", "queue", "jobDefinition", "tags"}; they stay RUNNING until terminated.
        for o in json.loads(os.environ.get("FAKEAWS_ORPHANS") or "[]"):
            self.jobs[o["jobId"]] = dict(p=None, name="old-job", t=time.time(), status="RUNNING",
                                         kw={"jobQueue": o["queue"], "jobDefinition": o["jobDefinition"],
                                             "tags": o["tags"]})

    def list_jobs(self, jobQueue, jobStatus=None, maxResults=100, nextToken=None, filters=None):
        out = []
        for jid, j in self.jobs.items():
            if filters:  # only JOB_NAME with a trailing * is used; any status, like real AWS
                if j["kw"]["jobQueue"] == jobQueue and j["name"].startswith(filters[0]["values"][0].rstrip("*")):
                    out.append({"jobId": jid, "jobName": j["name"], "status": self._status(j),
                                "createdAt": int(j["t"] * 1000)})
            elif j["kw"]["jobQueue"] == jobQueue and self._status(j) == jobStatus:
                # like real AWS: no jobDefinition or tags in the summaries
                out.append({"jobId": jid, "jobName": j["name"], "status": jobStatus})
        return {"jobSummaryList": out}

    def _status(self, j):
        if j["p"] is None:
            return j["status"]
        rc = j["p"].poll()
        return "RUNNING" if rc is None else ("SUCCEEDED" if rc == 0 else "FAILED")

    # Startup checks: every queue exists except "no-such-queue"; a queue whose name contains
    # "fargate" sits on a FARGATE compute environment, one with "spot" on SPOT, all others on EC2.
    def describe_job_queues(self, jobQueues=None):
        names = jobQueues or ["default-queue"]
        return {"jobQueues": [
            {"jobQueueName": q, "state": "ENABLED", "status": "VALID",
             **({"schedulingPolicyArn": "arn:aws:batch:eu-west-1:1:scheduling-policy/fs"} if "fair" in q else {}),
             "computeEnvironmentOrder": [{"order": 1, "computeEnvironment": f"ce-{q}"}]}
            for q in names if q != "no-such-queue"]}

    def describe_compute_environments(self, computeEnvironments):
        return {"computeEnvironments": [
            {"computeEnvironmentName": ce, "status": "VALID",
             "computeResources": {"type": "FARGATE" if "fargate" in ce else "SPOT" if "spot" in ce else "EC2",
                                  "ec2Configuration": [{"imageType": "ECS_AL2023", "imageIdOverride": "ami-fake"}]}}
            for ce in computeEnvironments]}

    def describe_job_definitions(self, jobDefinitionName=None, status=None, jobDefinitions=None, nextToken=None):
        # Pre-made definitions (FAKEAWS_JOBDEFS: JSON list in the real API's shape) come first.
        made = json.loads(os.environ.get("FAKEAWS_JOBDEFS") or "[]")
        if jobDefinitions:
            ref = jobDefinitions[0]
            return {"jobDefinitions": [d for d in made if ref in (d["jobDefinitionArn"],
                    f'{d["jobDefinitionName"]}:{d["revision"]}')]}
        hits = [d for d in made if d["jobDefinitionName"] == jobDefinitionName]
        if hits:
            return {"jobDefinitions": hits}
        d = self.defs.get(jobDefinitionName)
        return {"jobDefinitions": [d] if d else []}

    def register_job_definition(self, **kw):
        self.defs[kw["jobDefinitionName"]] = kw
        _rec("register_job_definition", image=kw["containerProperties"]["image"],
             name=kw["jobDefinitionName"], platform=kw.get("platformCapabilities"), tags=kw.get("tags"),
             mounts=kw["containerProperties"].get("mountPoints"),
             volumes=kw["containerProperties"].get("volumes"),
             creds=kw["containerProperties"].get("repositoryCredentials"),
             linux=kw["containerProperties"].get("linuxParameters"),
             disk=kw["containerProperties"].get("ephemeralStorage"),
             logs=kw["containerProperties"].get("logConfiguration"),
             exec_role=kw["containerProperties"].get("executionRoleArn"))
        return {}

    def _image(self, jobdef):
        d = self.defs.get(jobdef) or next(d for d in json.loads(os.environ.get("FAKEAWS_JOBDEFS") or "[]")
                                          if jobdef in (d["jobDefinitionArn"], f'{d["jobDefinitionName"]}:{d["revision"]}'))
        return d["containerProperties"]["image"]

    def submit_job(self, **kw):
        jid = f"job-{next(_ids)}"
        cmd = ENTRYPOINTS.get(self._image(kw["jobDefinition"]), []) + kw["containerOverrides"]["command"]
        env = dict(os.environ)
        if os.environ.get("FAKEAWS_JOB_PATH"): env["PATH"] = os.environ["FAKEAWS_JOB_PATH"]
        env.update({e["name"]: e["value"] for e in kw["containerOverrides"]["environment"]})
        env["TMPDIR"] = tempfile.mkdtemp(prefix="fakejob-")  # each real container has its own /tmp
        if os.environ.get("FAKEAWS_CMD"): open(os.environ["FAKEAWS_CMD"], "w").write(cmd[-1])
        # FAKEAWS_OOM_BELOW_MIB: a job of a rule named oom_* runs out of memory when it asks for
        # less than this
        mem = next(int(r["value"]) for r in kw["containerOverrides"]["resourceRequirements"] if r["type"] == "MEMORY")
        oom = "'oom_" in cmd[-1] and mem < int(os.environ.get("FAKEAWS_OOM_BELOW_MIB") or 0)
        if oom:
            cmd = ["/bin/sh", "-c", "echo killed; exit 137"]
        p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.jobs[jid] = dict(p=p, name=kw["jobName"], t=time.time(), kw=kw, oom=oom)
        _rec("submit_job", queue=kw["jobQueue"], jobdef=kw["jobDefinition"],
             res=kw["containerOverrides"]["resourceRequirements"], jobName=kw["jobName"],
             tags=kw.get("tags"), propagate=kw.get("propagateTags"),
             env={e["name"]: e["value"] for e in kw["containerOverrides"]["environment"]},
             retry=kw.get("retryStrategy"), priority=kw.get("schedulingPriorityOverride"),
             cmd=kw["containerOverrides"]["command"][:-1], cmdtail=kw["containerOverrides"]["command"][-1][-700:],
             # ECS allows 8192 characters of overrides (command and environment) per job
             overrides_chars=len(json.dumps(kw["containerOverrides"], separators=(",", ":"))))
        return {"jobId": jid, "jobName": kw["jobName"]}

    def describe_jobs(self, jobs):
        out = []
        for jid in jobs:
            j = self.jobs[jid]
            d = {"jobId": jid, "jobName": j["name"], "createdAt": int(j["t"] * 1000),
                 "tags": j["kw"].get("tags") or {},
                 "jobDefinition": f"arn:aws:batch:eu-west-1:1:job-definition/{j['kw']['jobDefinition']}:1"}
            if j["p"] is None:  # a pre-loaded leftover job
                out.append(dict(d, status=j["status"]))
                continue
            rc = j["p"].poll()
            q = j["kw"]["jobQueue"]
            ci = None if "fargate" in q else f"arn:aws:ecs:eu-west-1:1:container-instance/fake-cluster/{'spot' if 'spot' in q else 'od'}"
            if rc is None:
                d["status"] = "RUNNING"
                if ci:
                    d["container"] = {"containerInstanceArn": ci}
            else:
                d["status"] = "SUCCEEDED" if rc == 0 else "FAILED"
                j.setdefault("stopped", time.time())
                d["startedAt"], d["stoppedAt"] = int(j["t"] * 1000), int(j["stopped"] * 1000)
                j["out"] = j.get("out") or j["p"].stdout.read()
                if os.environ.get("FAKEAWS_OUT"): open(os.environ["FAKEAWS_OUT"], "a").write(j["out"] + "\n=====\n")
                d["container"] = {"exitCode": rc, "logStreamName": jid, "containerInstanceArn": ci,
                                  "reason": "OutOfMemoryError: Container killed due to memory usage"
                                  if j.get("oom") else "Essential container in task exited"}
            out.append(d)
        return {"jobs": out}

    def terminate_job(self, jobId, reason):
        _rec("terminate_job", jobId=jobId, reason=reason)
        j = self.jobs[jobId]
        if j["p"] is None:
            j["status"] = "FAILED"
        else:
            j["p"].kill()

    def _tail(self, jid, n=40):
        lines = (self.jobs[jid].get("out") or "").splitlines()
        return lines[-n:] if n else lines


class FakeLogs:
    def __init__(self, batch): self.b = batch
    def get_log_events(self, logGroupName, logStreamName, startFromHead, limit=None, nextToken=None):
        if nextToken == "end":  # already read everything
            return {"events": [], "nextForwardToken": "end"}
        lines = self.b._tail(logStreamName, limit)
        return {"events": [{"message": l} for l in lines], "nextForwardToken": "end" if lines else None}


class FakeEC2:
    """The AMI behind every fake queue is ami-fake, tagged with FAKEAWS_AMI_VERSION if set."""
    def describe_images(self, ImageIds):
        v = os.environ.get("FAKEAWS_AMI_VERSION")
        tags = [{"Key": "SnakemakeVersion", "Value": v}] if v else []
        return {"Images": [{"ImageId": i, "Tags": tags} for i in ImageIds]}

    def describe_launch_template_versions(self, **kw):
        return {"LaunchTemplateVersions": []}


class FakeSTS:
    def get_caller_identity(self):
        return {"Arn": "arn:aws:iam::1:user/tester"}


class FakeIAM:
    """Every role exists except ones with "missing" in the name."""
    def get_role(self, RoleName):
        if "missing" in RoleName:
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "NoSuchEntity", "Message": "no role"}}, "GetRole")
        return {"Role": {"RoleName": RoleName}}


_real_open = urllib.request.OpenerDirector.open


def _registry(self, req, data=None, timeout=None):
    """The fake registry: every image's manifest, and a config with its FAKEAWS_ENTRYPOINTS."""
    url = req if isinstance(req, str) else req.full_url
    m = re.match(r"https://([^/]+)/v2/(.+)/(manifests|blobs)/(.+)$", url)
    if not m:
        return _real_open(self, req, data, timeout)
    host, repo, kind, ref = m.groups()
    if kind == "manifests":
        doc = {"config": {"digest": ref}}
    else:
        from snakemake_executor_plugin_aws_batch_flex.imageinfo import parse_image
        image = next((i for i in ENTRYPOINTS if parse_image(i) == (host, repo, ref)), None)
        doc = {"config": {"Entrypoint": ENTRYPOINTS.get(image)}}
    return urllib.response.addinfourl(io.BytesIO(json.dumps(doc).encode()), email.message.Message(), url, 200)


urllib.request.OpenerDirector.open = _registry
_batch = FakeBatch()
_real = boto3.client
def client(name, *a, **k):
    if name == "batch": return _batch
    if name == "logs": return FakeLogs(_batch)
    if name == "ec2": return FakeEC2()
    if name == "sts": return FakeSTS()
    if name == "iam": return FakeIAM()
    return _real(name, *a, **k)
boto3.client = client
