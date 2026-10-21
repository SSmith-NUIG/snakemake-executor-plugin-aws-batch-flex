"""What a run used on Batch, per rule, from Batch's own start and stop times of every attempt
(Spot retries included). Printed when the run ends.

The cost is a rough estimate at list prices. On EC2 you pay for whole instances, so their idle
time and how well jobs fill them aren't in it; neither are storage, data transfer or logs."""

import statistics
from collections import defaultdict
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
        self.rows = defaultdict(lambda: [0, 0.0, 0.0, 0.0])  # rule -> jobs, vCPU-h, GiB-h, USD
        self.seconds = defaultdict(list)  # rule -> run time of each job that ran

    def add(self, rule: str, d: dict, vcpu: float, mem_mib: int, platform: str, spot: bool):
        secs = _seconds(d)
        if secs:
            self.seconds[rule].append(secs)
        hours = secs / 3600
        row = self.rows[rule]
        row[0] += 1
        row[1] += vcpu * hours
        row[2] += mem_mib / 1024 * hours
        row[3] += estimate(platform, spot, vcpu * hours, mem_mib / 1024 * hours)

    def table(self) -> Optional[str]:
        if not self.rows:
            return None
        width = max(10, *(len(r) for r in self.rows))
        lines = [
            "AWS Batch usage of this run (cost: rough estimate at list prices, without instance "
            "idle time, storage or data transfer):",
            f"  {'rule':<{width}}  {'jobs':>5}  {'vCPU-h':>8}  {'GiB-h':>8}  {'~USD':>8}",
        ]
        def usd(c):
            return "<0.01" if 0 < c < 0.005 else f"{c:.2f}"

        total = [0, 0.0, 0.0, 0.0]
        for rule, (n, v, g, c) in sorted(self.rows.items(), key=lambda kv: -kv[1][3]):
            lines.append(f"  {rule:<{width}}  {n:>5}  {v:>8.2f}  {g:>8.2f}  {usd(c):>8}")
            total = [total[0] + n, total[1] + v, total[2] + g, total[3] + c]
        if len(self.rows) > 1:
            lines.append(f"  {'total':<{width}}  {total[0]:>5}  {total[1]:>8.2f}  {total[2]:>8.2f}  {usd(total[3]):>8}")
        for rule, secs in sorted(self.seconds.items()):
            if len(secs) >= MANY_JOBS and statistics.median(secs) < SHORT_JOB_SECONDS:
                lines.append(
                    f"Hint: {rule} ran {len(secs)} jobs of {statistics.median(secs):.0f} s each (median). Every "
                    "Batch job adds about 30-60 s of overhead, so grouping them makes the run faster: "
                    "see \"Many small jobs\" in the plugin's README."
                )
        return "\n".join(lines)
