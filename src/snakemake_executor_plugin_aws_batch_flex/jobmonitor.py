"""Measure a job while it runs, for the table at the end of the run.

Runs inside the job (in Snakemake's Python, started by the job's launcher). A thread samples
the job's container:

- memory, every 0.5 seconds: anonymous plus shared memory from the cgroup's memory.stat (v2
  anon/shmem, v1 rss/shmem), i.e. without file cache, which the kernel frees before it kills
  anything. A spike between two samples isn't seen, so it's a lower bound; the cgroup's count
  of processes killed for lack of memory (memory.events oom_kill) says when it fell short;
- CPUs, every 2 seconds: how many were busy at once, from the cgroup's CPU time (cpu.stat
  usage_usec, or v1 cpuacct.usage) over each 2-second window;
- disk: the size of the job's working folder (its inputs, outputs and conda downloads),
  every 20 seconds.

When Snakemake exits, one line goes to the job's log:
[snakemake-batch] peak-memory-bytes=... peak-cpus=... avg-cpus=... peak-disk-bytes=... oom-kills=...

At the start, the ID of the image the job runs in (what a tag pointed to at the time) goes there
too, from ECS's container metadata: [snakemake-batch] image ... id=sha256:...

Only the standard library is used."""

import atexit
import json
import os
import sys
import threading
import time

CGROUP = "/sys/fs/cgroup"


def _memory():
    path = os.environ.get("SMK_FLEX_MEMSTAT_FILE", os.path.join(CGROUP, "memory.stat"))
    if not os.path.exists(path):  # cgroup v1 keeps it in the memory controller's folder
        path = os.path.join(CGROUP, "memory", "memory.stat")
    total = 0
    with open(path) as f:
        for line in f:
            key, _, value = line.partition(" ")
            if key in ("anon", "rss", "shmem"):
                total += int(value)
    return total


def _cpu_microseconds():
    path = os.environ.get("SMK_FLEX_CPUSTAT_FILE", os.path.join(CGROUP, "cpu.stat"))
    try:
        with open(path) as f:
            for line in f:
                if line.startswith("usage_usec "):
                    return int(line.split()[1])
    except OSError:
        pass
    try:
        with open(os.path.join(CGROUP, "cpuacct", "cpuacct.usage")) as f:  # cgroup v1, ns
            return int(f.read()) // 1000
    except OSError:
        return None


def _oom_kills():
    """Processes the kernel killed in this container for lack of memory, or None."""
    for path in (os.path.join(CGROUP, "memory.events"), os.path.join(CGROUP, "memory", "memory.oom_control")):
        try:
            with open(path) as f:
                for line in f:
                    if line.startswith("oom_kill "):
                        return int(line.split()[1])
        except (OSError, ValueError):
            pass
    return None


def _folder_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                st = os.lstat(os.path.join(root, name))
                total += st.st_blocks * 512 if hasattr(st, "st_blocks") else st.st_size
            except OSError:
                pass
    return total


def log_image():
    url = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    if not url:
        return
    try:
        from urllib.request import urlopen

        with urlopen(url, timeout=2) as r:
            c = json.load(r)
        print(f"[snakemake-batch] image {c.get('Image')} id={c.get('ImageID')}", file=sys.stderr, flush=True)
    except Exception:
        pass


def monitor(interval=0.5, cpu_every=4, disk_every=40):
    log_image()
    work = os.environ.get("SMK_WORK") or os.getcwd()
    peak = {"mem": 0, "cpus": 0.0, "disk": 0}
    start_cpu, start_t = _cpu_microseconds(), time.monotonic()

    def sample_memory():
        try:
            peak["mem"] = max(peak["mem"], _memory())
        except (OSError, ValueError):
            pass

    def loop():
        last_cpu, last_t, n = start_cpu, start_t, 0
        while True:
            time.sleep(interval)
            n += 1
            try:
                sample_memory()
                if n % cpu_every == 0:
                    cpu, t = _cpu_microseconds(), time.monotonic()
                    if cpu is not None and last_cpu is not None and t > last_t:
                        peak["cpus"] = max(peak["cpus"], (cpu - last_cpu) / 1e6 / (t - last_t))
                    last_cpu, last_t = cpu, t
                if n % disk_every == 1:
                    peak["disk"] = max(peak["disk"], _folder_size(work))
            except Exception:
                pass

    threading.Thread(target=loop, daemon=True).start()

    def report():
        try:
            sample_memory()
            peak["disk"] = max(peak["disk"], _folder_size(work))
            parts = [f"peak-memory-bytes={peak['mem']}", f"peak-cpus={peak['cpus']:.2f}"]
            cpu, t = _cpu_microseconds(), time.monotonic()
            if cpu is not None and start_cpu is not None and t > start_t:
                parts.append(f"avg-cpus={(cpu - start_cpu) / 1e6 / (t - start_t):.2f}")
            parts.append(f"peak-disk-bytes={peak['disk']}")
            kills = _oom_kills()
            if kills is not None:
                parts.append(f"oom-kills={kills}")
            print("[snakemake-batch] " + " ".join(parts), file=sys.stderr, flush=True)
        except Exception:
            pass

    atexit.register(report)
