"""What a run used on Batch, per rule, from Batch's own start and stop times of every attempt
(Spot retries included). Printed when the run ends.

Cost: on EC2, the instances the jobs ran on, at AWS's prices, shared out by vCPU-time
(instances.py); on Fargate, what Fargate charges per job. When the instances can't be looked
up, a rough per-vCPU estimate instead. Storage, data transfer and logs aren't in it."""

import math
import re
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

# USD, on-demand list prices in eu-west-1 / us-east-1 (they're within ~10% of each other).
EC2_PER_VCPU_HOUR = 0.05  # general-purpose instances, memory included
SPOT_FACTOR = 0.35  # Spot is typically 60-70% below on-demand
FARGATE_PER_VCPU_HOUR = 0.0405
FARGATE_PER_GB_HOUR = 0.0045
FARGATE_SPOT_FACTOR = 0.3

# A Batch job costs roughly 30-60 s on top of its command (scheduling, starting the container,
# copying files from and to storage), so many short jobs of one rule are better grouped.
SHORT_JOB_SECONDS = 60
MANY_JOBS = 20

# Peak memory: each job logs it when it ends (bootstrap.peak_memory_snippet). Reading the logs
# costs an API call per job, so big runs read a sample of each rule's jobs.
PEAK_RE = re.compile(r"\[snakemake-batch\] peak-memory-bytes=(\d+)")
PEAK_SAMPLE = 25  # jobs per rule, for runs with more than ALL_PEAKS jobs
ALL_PEAKS = 300
HEADROOM = 1.3  # suggested memory = peak * HEADROOM


def _seconds(d: dict) -> float:
    attempts = d.get("attempts") or []
    spans = [(a.get("startedAt"), a.get("stoppedAt")) for a in attempts] or [(d.get("startedAt"), d.get("stoppedAt"))]
    return sum(max(0, stop - start) / 1000 for start, stop in spans if start is not None and stop is not None)


def estimate(platform: str, spot: bool, vcpu_hours: float, gib_hours: float) -> float:
    if platform == "fargate":
        cost = vcpu_hours * FARGATE_PER_VCPU_HOUR + gib_hours * FARGATE_PER_GB_HOUR
        return cost * (1 - FARGATE_SPOT_FACTOR if spot else 1)
    return vcpu_hours * EC2_PER_VCPU_HOUR * (SPOT_FACTOR if spot else 1)


class Usage:
    def __init__(self):
        self.rows = defaultdict(lambda: [0, 0.0, 0.0, 0.0])  # rule -> jobs, vCPU-h, GiB-h, USD (not EC2 instances)
        self.seconds = defaultdict(list)  # rule -> run time of each job that ran
        self.asked = defaultdict(int)  # rule -> most memory a job asked for (MiB)
        self.logs = defaultdict(list)  # rule -> [(log group, stream)] of jobs that succeeded
        self.peaks = defaultdict(list)  # rule -> peak memory of jobs (MiB) read from their logs
        self.sampled = False
        self.estimated = False  # some EC2 cost is the rough per-vCPU estimate
        self.rough = defaultdict(float)  # rule -> rough estimate for jobs on known instances

    def add(self, rule: str, d: dict, vcpu: float, mem_mib: int, platform: str, spot: bool,
            on_known_instance: bool = False):
        """on_known_instance: an EC2 job whose instances are known, so its cost comes from them."""
        secs = _seconds(d)
        if secs:
            self.seconds[rule].append(secs)
        hours = secs / 3600
        self.asked[rule] = max(self.asked[rule], mem_mib)
        row = self.rows[rule]
        row[0] += 1
        row[1] += vcpu * hours
        row[2] += mem_mib / 1024 * hours
        rough = estimate(platform, spot, vcpu * hours, mem_mib / 1024 * hours)
        if platform == "fargate" or not on_known_instance:
            row[3] += rough
            if platform != "fargate":
                self.estimated = True
        else:
            self.rough[rule] += rough  # used only if the instances can't be priced

    def add_log(self, rule: str, group: str, stream: str):
        self.logs[rule].append((group, stream))

    def fetch_peaks(self, logs_client):
        """Read the peak-memory line from the end of each job's log (a sample per rule on big
        runs). Best effort: jobs whose line can't be read are left out."""
        total = sum(len(v) for v in self.logs.values())
        todo = []
        for rule, streams in self.logs.items():
            if total > ALL_PEAKS and len(streams) > PEAK_SAMPLE:
                self.sampled = True
                streams = streams[-PEAK_SAMPLE:]
            todo += [(rule, g, st) for g, st in streams]

        def one(item):
            rule, group, stream = item
            try:
                events = logs_client.get_log_events(
                    logGroupName=group, logStreamName=stream, limit=20, startFromHead=False
                )["events"]
            except Exception:
                return rule, None
            for e in reversed(events):
                m = PEAK_RE.search(e.get("message", ""))
                if m:
                    return rule, int(m.group(1)) / 1024 / 1024
            return rule, None

        with ThreadPoolExecutor(8) as pool:
            for rule, peak in pool.map(one, todo):
                if peak:
                    self.peaks[rule].append(peak)

    def suggestions(self, overhead_mib: int) -> dict:
        """rule -> mem_mb that gives its jobs their peak plus headroom (at least 512 MiB in
        all), for every rule whose peak is known."""
        out = {}
        for rule, peaks in self.peaks.items():
            want = max(512, math.ceil(max(peaks) * HEADROOM / 100) * 100)  # container memory, MiB
            # the plugin adds overhead_mib for Snakemake itself; mem_mb is in MB
            out[rule] = max(100, math.ceil((want - overhead_mib) * 1.048576 / 100) * 100)
        return out

    def table(self, overhead_mib: int = 256, instance_costs: Optional[dict] = None,
              instance_line: Optional[str] = None) -> Optional[str]:
        if not self.rows:
            return None
        width = max(10, *(len(r) for r in self.rows))
        if instance_costs is not None:
            how = ("cost: the EC2 instances at AWS's prices, shared out by vCPU-time"
                   + (", partly estimated" if self.estimated else "") + "; without storage or data transfer")
        else:
            how = ("cost: rough estimate from the jobs' vCPUs at list prices, without instance idle "
                   "time, storage or data transfer")
        lines = [
            f"AWS Batch usage of this run ({how}):",
            f"  {'rule':<{width}}  {'jobs':>5}  {'vCPU-h':>8}  {'GiB-h':>8}  {'~USD':>8}"
            f"  {'mem MiB':>8}  {'peak MiB':>8}",
        ]
        def usd(c):
            return "<0.01" if 0 < c < 0.005 else f"{c:.2f}"

        total = [0, 0.0, 0.0, 0.0]
        def cost(rule):
            if instance_costs is None:
                return self.rows[rule][3] + self.rough.get(rule, 0.0)
            return self.rows[rule][3] + instance_costs.get(rule, 0.0)

        for rule, (n, v, g, _c) in sorted(self.rows.items(), key=lambda kv: -cost(kv[0])):
            c = cost(rule)
            peak = f"{max(self.peaks[rule]):.0f}" if self.peaks.get(rule) else "-"
            lines.append(f"  {rule:<{width}}  {n:>5}  {v:>8.2f}  {g:>8.2f}  {usd(c):>8}"
                         f"  {self.asked.get(rule, 0):>8}  {peak:>8}")
            total = [total[0] + n, total[1] + v, total[2] + g, total[3] + c]
        if len(self.rows) > 1:
            lines.append(f"  {'total':<{width}}  {total[0]:>5}  {total[1]:>8.2f}  {total[2]:>8.2f}  {usd(total[3]):>8}")
        if instance_line:
            lines.append(instance_line)
        if self.peaks:
            note = f" (from up to {PEAK_SAMPLE} jobs per rule)" if self.sampled else ""
            lines.append(f"mem MiB: what the job had (the most for that rule); peak MiB: the most its "
                         f"processes used{note}, sampled every 2 s, without file cache.")
        tips = self.suggestions(overhead_mib)
        if tips:
            lines.append("Memory for each rule (its peak + 30%), to paste into the project's "
                         "profiles/default/config.yaml (smkflex init makes it):")
            lines.append("set-resources:")
            for rule, mb in sorted(tips.items()):
                had = self.asked.get(rule)
                lines += [f"  {rule}:",
                          f"    mem_mb: {mb:<8}# had {had} MiB, peak {max(self.peaks[rule]):.0f}"]
        for rule, secs in sorted(self.seconds.items()):
            if len(secs) >= MANY_JOBS and statistics.median(secs) < SHORT_JOB_SECONDS:
                lines.append(
                    f"Hint: {rule} ran {len(secs)} jobs of {statistics.median(secs):.0f} s each (median). Every "
                    "Batch job adds about 30-60 s of overhead, so grouping them makes the run faster: "
                    "see \"Many small jobs\" in the plugin's README."
                )
        return "\n".join(lines)
