"""Plain-language hints for job failures with a known cause, from the end of the job's log."""

import re
from typing import List


def known_problem_hints(log_lines: List[str]) -> List[str]:
    """Plain-language explanations for failures with a known cause, from the job's log."""
    text = "\n".join(log_lines)
    hints = []
    bad = " ".join(re.findall(r"unrecognized arguments:(.*)", text))
    plugins = sorted(set(re.findall(r"--storage-([a-z0-9]+)-", bad)))
    for name in plugins:
        hints.append(
            f"  hint: this machine has snakemake-storage-plugin-{name} installed but the job's "
            f"Snakemake (from the AMI, or installed at job start) doesn't, and Snakemake passes "
            f"every installed storage "
            f"plugin's options to its jobs. Uninstall it here (pip uninstall "
            f"snakemake-storage-plugin-{name}) or bake an AMI with --storage-plugin {name}."
        )
    missing = sorted(set(re.findall(r"ModuleNotFoundError: No module named '([A-Za-z0-9_.]+)'", text)))
    for mod in missing:
        hints.append(
            f"  hint: Python couldn't import {mod}. If the Snakefile (or a file it includes) imports "
            f"it, it's needed in every job too, because Snakemake reads the Snakefile there as well: "
            f"add it to the AMI with scripts/update-ami.sh --extra-package {mod.split('.')[0]} (the pip "
            f"name can differ), or to --aws-batch-flex-extra-packages for jobs that install Snakemake "
            f"at start. If a script: imports it, it belongs in the rule's image or conda env."
        )
    if re.search(r"\(403\).*(HeadObject|GetObject|PutObject|ListObjects).*Forbidden|AccessDenied.*(GetObject|PutObject)", text):
        hints.append(
            "  hint: the job wasn't allowed to read or write the bucket. Jobs use the job role "
            "(aws-batch-flex-job-role-arn), not your own login, and the Terraform setup only lets "
            "it use the s3_prefix folder (s3://BUCKET/snakemake/...). Keep default-storage-prefix "
            "inside that folder, or give the role access to the other one."
        )
    if "No space left on device" in text:
        hints.append(
            "  hint: the job ran out of disk. On EC2, all jobs on an instance share its disk "
            "(root_volume_gib in Terraform), and disk_mb isn't enforced there: use a bigger "
            "volume, a queue with bigger disks for this rule (batch_queue), or fewer jobs per "
            "instance. On Fargate, raise the rule's disk_mb (up to 200 GiB)."
        )
    if not plugins and "unrecognized arguments" in text:
        hints.append(
            "  hint: the job's Snakemake didn't understand an option from this machine's "
            "Snakemake; the two versions are probably too far apart (compare "
            "`snakemake --version` here with the job's; 9.5 to 9.27 are known to mix fine)."
        )
    return hints
