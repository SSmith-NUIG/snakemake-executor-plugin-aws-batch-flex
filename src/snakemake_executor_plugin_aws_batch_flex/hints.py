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
    if "pandas of Snakemake's env can't load in this image" in text and "pandas" in text.split("can't load in this image", 1)[1]:
        hints.append(
            "  hint: the image lacks a system library (named above) that the numpy and pandas of the AMI's "
            "Snakemake need, and no conda from the AMI was there to lend one (aws-batch-flex-conda-path, EC2). "
            "Many bioconda images keep theirs inside their own environment. If the Snakefile imports pandas, "
            "set aws-batch-flex-conda-path, or use a Debian- or Ubuntu-based image for this rule."
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
    if re.search(r"fail to locate the index files|Could not retrieve index file|"
                 r"Could not locate a (Bowtie|HISAT2) index|Fasta (dict|index) file .* does not exist", text):
        hints.append(
            "  hint: the tool looked for index files next to an input (genome.fa.bwt, x.bam.bai, "
            "genome.dict...) that the rule doesn't list. A job only gets the inputs its rule names "
            "from storage, so list them too, e.g. input: "
            "multiext(\"genome.fa\", \".amb\", \".ann\", \".bwt\", \".pac\", \".sa\") for bwa, "
            "or bai=\"x.bam.bai\"."
        )
    if "StringDtype.__init__() takes from 1 to 2 positional arguments" in text:
        hints.append(
            "  hint: Snakemake runs with pandas 3, the rule's environment has pandas 2, and pandas 2 "
            "can't load the data Snakemake hands the script (it fails the same way on your own "
            "machine). Use pandas 2 where you run Snakemake (pip install 'pandas<3') and then "
            "scripts/update-ami.sh, so the AMI's matches, or give the rule's environment pandas 3."
        )
    if "No space left on device" in text:
        hints.append(
            "  hint: the job ran out of disk. On EC2, all jobs on an instance share its disk "
            "(root_volume_gib in Terraform), and disk_mb isn't enforced there: use a bigger "
            "volume, a queue with bigger disks for this rule (batch_queue), or fewer jobs per "
            "instance. On Fargate, raise the rule's disk_mb (up to 200 GiB)."
        )
    if re.search(r"SyntaxError: .*(in file /bin/sh\b|source code cannot contain null bytes)", text):
        hints.append(
            "  hint: the image has a Python ENTRYPOINT (such as [\"python3\"]) that the plugin "
            "couldn't read from its registry (see the warning at the start), so Python was handed "
            "/bin/sh as a script to run. Give the rule the entrypoint: "
            "resources: batch_entrypoint='[\"python3\"]'."
        )
    # a script a shell: command runs that wasn't sent: python, Rscript, sh/bash
    scripts = re.findall(r"can't open file '(?:/tmp/snakemake-job-\d+/)?([^']+)'", text)
    scripts += re.findall(r"cannot open file '([^']+\.[Rr])': No such file", text)
    scripts += re.findall(r"(?:^|\s)(\S+\.(?:sh|py|R|pl|rb|jl)): No such file", text, re.M)
    scripts += re.findall(r"Can't open (\S+\.(?:sh|py|R|pl|rb|jl))", text)  # older dash
    if scripts:
        hints.append(
            f"  hint: {', '.join(sorted(set(scripts)))} wasn't sent to the job. Jobs get the workflow folder "
            "snakemake runs in, except what .gitignore excludes and single files over 10 MB. Is it "
            "git-ignored, outside that folder, or made by an earlier rule without being its output?"
        )
    if not plugins and "unrecognized arguments" in text:
        hints.append(
            "  hint: the job's Snakemake didn't understand an option from this machine's "
            "Snakemake; the two versions are probably too far apart (compare "
            "`snakemake --version` here with the job's; 9.5 to 9.27 are known to mix fine)."
        )
    return hints
