"""Map Snakemake job resources onto AWS Batch resource requirements."""

import math
from dataclasses import dataclass
from typing import Mapping, Optional

from snakemake_interface_common.exceptions import WorkflowError

DEFAULT_MEM_MIB = 2048
# The Snakemake process inside each job shares the container's memory limit (~100-250 MiB).
DEFAULT_MEM_OVERHEAD_MIB = 256

# Fargate: vCPU -> (min MiB, max MiB, step MiB)
_FARGATE = {
    0.25: (512, 2048, 512),
    0.5: (1024, 4096, 1024),
    1: (2048, 8192, 1024),
    2: (4096, 16384, 1024),
    4: (8192, 30720, 1024),
    8: (16384, 61440, 4096),
    16: (32768, 122880, 8192),
}


@dataclass
class Requirements:
    vcpu: str
    memory_mib: int
    gpu: Optional[int] = None
    timeout_seconds: Optional[int] = None


def fargate_size(vcpu: float, mem_mib: int) -> tuple[str, int]:
    """Smallest valid Fargate (vCPU, MiB) combination that satisfies the request."""
    for cpu, (lo, hi, step) in sorted(_FARGATE.items()):
        if cpu >= vcpu and hi >= mem_mib:
            mem = max(lo, mem_mib)
            mem = lo + math.ceil((mem - lo) / step) * step
            return (str(cpu), mem)
    raise WorkflowError(
        f"No Fargate size fits {vcpu} vCPU / {mem_mib} MiB (max 16 vCPU, 120 GiB). "
        "Use --aws-batch-flex-platform ec2 for larger jobs."
    )


def job_requirements(
    resources: Mapping,
    threads: int,
    platform: str,
    mem_overhead_mib: int = DEFAULT_MEM_OVERHEAD_MIB,
) -> Requirements:
    mem_mib = None
    if resources.get("mem_mib"):
        mem_mib = int(resources["mem_mib"])
    elif resources.get("mem_mb"):
        # Snakemake normally derives mem_mib itself; this is only a fallback (4.6% generous).
        mem_mib = int(resources["mem_mb"])
    # The overhead is only added to explicit requests; the default already leaves room.
    mem_mib = mem_mib + max(0, mem_overhead_mib) if mem_mib else DEFAULT_MEM_MIB
    mem_mib = max(mem_mib, 128)
    vcpu = max(1, int(threads or 1))

    gpu = int(resources["gpu"]) if resources.get("gpu") else None
    if gpu and platform == "fargate":
        raise WorkflowError("GPU resources are not supported on Fargate.")

    timeout = None
    if resources.get("runtime"):  # Snakemake's `runtime` is in minutes
        timeout = max(60, int(resources["runtime"]) * 60)

    if platform == "fargate":
        cpu_s, mem_mib = fargate_size(vcpu, mem_mib)
    else:
        cpu_s = str(vcpu)
    return Requirements(cpu_s, mem_mib, gpu, timeout)
