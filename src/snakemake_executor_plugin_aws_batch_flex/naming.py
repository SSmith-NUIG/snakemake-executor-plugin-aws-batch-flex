"""AWS Batch job names: up to 128 chars, letters/digits/hyphens/underscores, starting with a letter or digit."""
import hashlib
import re
import string

MAX_LEN = 128


def batch_job_name(name: str) -> str:
    """Make any Snakemake job name valid for Batch; never rejects. Long names keep a hash so they stay distinct."""
    clean = re.sub(r"[^A-Za-z0-9_-]", "_", name)
    if not clean or clean[0] not in string.ascii_letters + string.digits:
        clean = "snk-" + clean
    if len(clean) > MAX_LEN:
        clean = clean[: MAX_LEN - 9] + "-" + hashlib.sha1(name.encode()).hexdigest()[:8]
    return clean
