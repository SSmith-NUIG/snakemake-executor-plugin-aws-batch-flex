"""What a run used on Batch, per rule, from Batch's own start and stop times of every attempt
(Spot retries included) and the figures each job logs (jobmonitor.py): vCPU- and GiB-hours,
memory, CPUs and disk, with the resources to paste into the profile. Printed when the run ends.

What it cost is AWS's to say: every job carries the tags snakemake-run-id and snakemake-rule,
which AWS Billing (with split cost allocation data) breaks costs down by."""

import math
import re
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

from botocore.exceptions import BotoCoreError, ClientError

# A Batch job costs time on top of its command: measured on real runs, about 15 s from submitted to
# running on an instance that's already up (a minute or more when one has to start), and about
# 10 s for Snakemake in the job and its files. Many short jobs of one rule are better grouped.
SHORT_JOB_SECONDS = 60
MANY_JOBS = 20

# Usage figures: each job logs them when it ends (jobmonitor.py). Reading the logs costs an
# API call per job, so big runs read a sample of each rule's jobs.
USAGE_RE = re.compile(r"\[snakemake-batch\] (peak-memory-bytes=\S+.*)")
PEAK_SAMPLE = 25  # jobs per rule, for runs with more than ALL_PEAKS jobs
ALL_PEAKS = 300
HEADROOM = 1.3  # suggested memory = peak * HEADROOM
THREAD_HEADROOM = 1.25  # suggested threads = most in use at once * THREAD_HEADROOM
FEW_THREADS = 0.6  # suggest fewer threads when a rule never had more than this share busy
BIG_DISK_GIB = 20  # say so when a rule's job used more disk than this


def _seconds(d: dict) -> float:
    attempts = d.get("attempts") or []
    spans = [(a.get("startedAt"), a.get("stoppedAt")) for a in attempts] or [(d.get("startedAt"), d.get("stoppedAt"))]
    return sum(max(0, stop - start) / 1000 for start, stop in spans if start is not None and stop is not None)


@dataclass
class Row:
    """One rule's jobs."""
    jobs: int = 0
    vcpu_h: float = 0.0
    gib_h: float = 0.0


class Usage:
    def __init__(self):
        self.rows = defaultdict(Row)  # rule -> Row
        self.seconds = defaultdict(list)  # rule -> run time of each job that ran
        self.asked = defaultdict(int)  # rule -> most memory a job asked for (MiB)
        self.logs = defaultdict(list)  # rule -> [(log group, stream)] of jobs that succeeded
        self.peaks = defaultdict(list)  # rule -> peak memory of jobs (MiB) read from their logs
        self.busy = defaultdict(list)  # rule -> most vCPUs in use at once, per job
        self.disk = defaultdict(list)  # rule -> most disk a job's working folder took (GiB)
        self.threads = defaultdict(float)  # rule -> most vCPUs a job asked for
        self.oom_kills = defaultdict(int)  # rule -> its jobs' processes killed for lack of memory
        self.platform = {}  # rule -> ec2 | fargate
        # group job name (mapping_bwa_sort) -> (group, its rules): not a rule, so no set-resources
        self.groups = {}
        self.sampled = False

    def add(self, rule: str, d: dict, vcpu: float, mem_mib: int, platform: str, group=None):
        if group:
            self.groups[rule] = group
        secs = _seconds(d)
        if secs:
            self.seconds[rule].append(secs)
        hours = secs / 3600
        self.asked[rule] = max(self.asked[rule], mem_mib)
        self.threads[rule] = max(self.threads[rule], vcpu)
        self.platform[rule] = platform
        row = self.rows[rule]
        row.jobs += 1
        row.vcpu_h += vcpu * hours
        row.gib_h += mem_mib / 1024 * hours

    def add_log(self, rule: str, group: str, stream: str):
        self.logs[rule].append((group, stream))

    def fetch_peaks(self, logs_client):
        """Read the peak-memory line from the end of each job's log (a sample per rule on big
        runs). Best effort: jobs whose line can't be read are left out."""
        total = sum(len(v) for v in self.logs.values())
        todo = []
        for rule, streams in self.logs.items():
            if total > ALL_PEAKS and len(streams) > PEAK_SAMPLE:
                # spread over the run: a rule's biggest inputs aren't always its last jobs
                self.sampled = True
                step = len(streams) / PEAK_SAMPLE
                streams = [streams[int(i * step)] for i in range(PEAK_SAMPLE)]
            todo += [(rule, g, st) for g, st in streams]

        def one(item):
            rule, group, stream = item
            try:
                events = logs_client.get_log_events(
                    logGroupName=group, logStreamName=stream, limit=20, startFromHead=False
                )["events"]
            except (ClientError, BotoCoreError):
                return rule, None
            for e in reversed(events):
                m = USAGE_RE.search(e.get("message", ""))
                if m:
                    return rule, dict(kv.split("=", 1) for kv in m.group(1).split() if "=" in kv)
            return rule, None

        with ThreadPoolExecutor(8) as pool:
            for rule, fields in pool.map(one, todo):
                if not fields:
                    continue
                try:
                    if int(fields.get("peak-memory-bytes", 0)):
                        self.peaks[rule].append(int(fields["peak-memory-bytes"]) / 1024 / 1024)
                    if "peak-cpus" in fields:
                        self.busy[rule].append(float(fields["peak-cpus"]))
                    if "peak-disk-bytes" in fields:
                        self.disk[rule].append(int(fields["peak-disk-bytes"]) / 1024 ** 3)
                    self.oom_kills[rule] += int(fields.get("oom-kills", 0))
                except ValueError:
                    pass

    def suggestions(self, overhead_mib: int) -> dict:
        """rule -> mem_mb that gives its jobs their peak plus headroom (at least 512 MiB in
        all), for every rule whose peak is known. Not for rules whose jobs ran out: what was
        seen before that is less than they need."""
        out = {}
        for rule, peaks in self.peaks.items():
            if self.oom_kills.get(rule) or rule in self.groups:
                continue
            want = max(512, math.ceil(max(peaks) * HEADROOM / 100) * 100)  # container memory, MiB
            # the plugin adds overhead_mib for Snakemake itself; mem_mb is in MB
            out[rule] = max(100, math.ceil((want - overhead_mib) * 1.048576 / 100) * 100)
        return out

    def thread_suggestions(self) -> dict:
        """rule -> threads, for rules that never had most of their vCPUs busy at once."""
        out = {}
        for rule, busy in self.busy.items():
            if rule in self.groups:
                continue
            asked, most = self.threads.get(rule, 0), max(busy)
            if asked >= 2 and most <= asked * FEW_THREADS:
                out[rule] = max(1, min(int(asked) - 1, math.ceil(most * THREAD_HEADROOM)))
        return out

    def table(self, overhead_mib: int = 256) -> Optional[str]:
        if not self.rows:
            return None
        width = max(10, *(len(r) for r in self.rows))
        lines = [
            "AWS Batch usage of this run (costs: AWS Billing, by the jobs' tags snakemake-run-id and "
            "snakemake-rule):",
            f"  {'rule':<{width}}  {'jobs':>5}  {'vCPU-h':>8}  {'GiB-h':>8}"
            f"  {'vCPUs':>5}  {'busy':>5}  {'mem MiB':>8}  {'peak':>6}  {'disk GiB':>8}",
        ]
        total = Row()
        # the rules that took the most first
        for rule in sorted(self.rows, key=lambda r: -self.rows[r].vcpu_h):
            row = self.rows[rule]
            n, v, g = row.jobs, row.vcpu_h, row.gib_h
            peak = f"{max(self.peaks[rule]):.0f}" if self.peaks.get(rule) else "-"
            busy = f"{max(self.busy[rule]):.1f}" if self.busy.get(rule) else "-"
            disk = f"{max(self.disk[rule]):.1f}" if self.disk.get(rule) else "-"
            vcpus = f"{self.threads.get(rule, 0):g}"
            lines.append(f"  {rule:<{width}}  {n:>5}  {v:>8.2f}  {g:>8.2f}"
                         f"  {vcpus:>5}  {busy:>5}  {self.asked.get(rule, 0):>8}  {peak:>6}  {disk:>8}")
            total = Row(total.jobs + n, total.vcpu_h + v, total.gib_h + g)
        if len(self.rows) > 1:
            lines.append(f"  {'total':<{width}}  {total.jobs:>5}  {total.vcpu_h:>8.2f}  {total.gib_h:>8.2f}")
        if self.peaks:
            note = f" (from up to {PEAK_SAMPLE} jobs per rule)" if self.sampled else ""
            lines.append(f"vCPUs, mem MiB: what a job had; busy: the most vCPUs in use at once (on EC2 a job can use "
                         "idle vCPUs of its instance, so this can be more than it had); peak: the most "
                         f"memory its processes used (without file cache); disk GiB: the most its working "
                         f"folder took. Each the highest of the rule's jobs{note}; memory sampled every "
                         f"0.5 s (a shorter spike isn't seen), CPUs every 2 s.")
        tips = self.suggestions(overhead_mib)
        if tips:
            lines.append("Memory for each rule (its peak + 30%; mem_mb leaves out the plugin's own "
                         f"{overhead_mib} MiB, added on top), to paste into the project's "
                         "profiles/default/config.yaml (smkflex init makes it):")
            lines.append("set-resources:")
            for rule, mb in sorted(tips.items()):
                had = self.asked.get(rule)
                gets = int(mb / 1.048576) + overhead_mib  # what the job is given for that, MiB
                lines += [f"  {rule}:",
                          f"    mem_mb: {mb:<8}# job gets {gets} MiB; had {had}, peak {max(self.peaks[rule]):.0f}"]
        threads = self.thread_suggestions()
        if threads:
            lines.append("Threads for rules that never had most of theirs busy (the most in use at once "
                         "+ 25%; a burst shorter than 2 s isn't seen, so check the tool first):")
            lines.append("set-threads:")
            for rule, n in sorted(threads.items()):
                lines.append(f"  {rule}: {n:<6}# had {self.threads[rule]:g}, at most {max(self.busy[rule]):.1f} busy")
        for name, (group, rules) in sorted(self.groups.items()):
            if self.peaks.get(name):
                lines.append(f"Note: group {group} (rules {', '.join(rules)}) peaked at {max(self.peaks[name]):.0f} MiB "
                             f"(had {self.asked.get(name)}), its rules running in one job together; set-resources "
                             "takes rules, so give them mem_mb that together cover that, plus 30%.")
        for rule, n in sorted(self.oom_kills.items()):
            if n:
                lines.append(f"Hint: in {rule}'s jobs, Linux killed {n} process(es) for lack of memory (a job "
                             "can still succeed if the tool retried). Its peak is from before that, so it "
                             "needs more: raise its mem_mb.")
        for rule, gib in sorted((r, max(d)) for r, d in self.disk.items() if d and max(d) > BIG_DISK_GIB):
            where = ("Fargate gives a job 20 GiB unless disk_mb asks for more"
                     if self.platform.get(rule) == "fargate" else
                     "on EC2 the jobs on an instance share its disk (root_volume_gib in Terraform)")
            lines.append(f"Hint: {rule}'s jobs used up to {gib:.0f} GiB of disk each; {where}.")
        for rule, secs in sorted(self.seconds.items()):
            if len(secs) >= MANY_JOBS and statistics.median(secs) < SHORT_JOB_SECONDS:
                lines.append(
                    f"Hint: {rule} ran {len(secs)} jobs of {statistics.median(secs):.0f} s each (median). Every "
                    "Batch job adds about 25 s (a minute or more when a new instance has to start), so "
                    "grouping them makes the run faster: "
                    "see \"Many small jobs\" in the plugin's README."
                )
        return "\n".join(lines)
