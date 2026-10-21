"""Runs inside the head job that `smkflex run` submits: restores Snakemake's .snakemake state,
runs Snakemake on the unpacked workflow, then saves the state for the next run.

`smkflex run` copies this file into the workflow bundle, so it only uses the standard library
(and boto3, which comes with the S3 storage plugin)."""

import io
import os
import signal
import subprocess
import sys
import tarfile

# The parts of .snakemake that make a rerun behave like one on the same machine: what was
# made with which code and parameters, and which jobs didn't finish. Conda envs, local
# copies of storage files and the like are left out.
STATE_DIRS = ("metadata", "incomplete", "log")


def _s3(url):
    import boto3

    bucket, _, key = url[len("s3://"):].partition("/")
    return boto3.client("s3"), bucket, key


def get(url):
    """The file at an s3:// URL or local path, or None if there is none."""
    if url.startswith("s3://"):
        s3, bucket, key = _s3(url)
        try:
            return s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except s3.exceptions.NoSuchKey:
            return None
    try:
        with open(url, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None


def put(url, data):
    if url.startswith("s3://"):
        s3, bucket, key = _s3(url)
        s3.put_object(Bucket=bucket, Key=key, Body=data)
    else:
        os.makedirs(os.path.dirname(url) or ".", exist_ok=True)
        with open(url, "wb") as f:
            f.write(data)


def unpack(data, dest):
    with tarfile.open(fileobj=io.BytesIO(data)) as t:
        if hasattr(tarfile, "data_filter"):
            t.extractall(dest, filter="data")
        else:
            t.extractall(dest)


def save_state(url):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for d in STATE_DIRS:
            path = os.path.join(".snakemake", d)
            if os.path.isdir(path):
                t.add(path)
    put(url, buf.getvalue())


GIT_SHIM = """#!{python}
# Stand-in for git, which the image doesn't have: Snakemake runs `git ls-files` to find the
# workflow's source files, and these are exactly the files smkflex sent.
import sys
if sys.argv[1:2] == ["ls-files"]:
    sys.stdout.write(open({files!r}).read())
else:
    sys.exit("git is not installed in the head job's image (smkflex only provides git ls-files)")
"""


def git_shim():
    """Directory with a stand-in `git`, or None when the image has a real one."""
    import shutil

    if shutil.which("git"):
        return None
    d = os.path.abspath(os.path.join(".smkflex", "bin"))
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, "git")
    with open(path, "w") as f:
        f.write(GIT_SHIM.format(python=sys.executable, files=os.path.abspath(".smkflex/files.txt")))
    os.chmod(path, 0o755)
    return d


def main(argv):
    state_url, args = argv[0], argv[1:]
    state = get(state_url)
    if state:
        unpack(state, ".")
        print("[smkflex] restored .snakemake state from the previous run", file=sys.stderr)
    site = os.path.abspath(os.path.join(".smkflex", "site"))
    env = dict(os.environ)
    env["PYTHONPATH"] = site + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    shim = git_shim()
    if shim:
        env["PATH"] = shim + os.pathsep + env.get("PATH", "")
    cmd = [sys.executable, "-m", "snakemake", "--profile", ".smkflex/profile", *args]
    print("[smkflex] " + " ".join(cmd), file=sys.stderr, flush=True)
    proc = subprocess.Popen(cmd, env=env)

    # `smkflex cancel` (Batch's terminate) sends SIGTERM; Snakemake cancels its jobs on SIGINT.
    def stop(_sig, _frame):
        proc.send_signal(signal.SIGINT)

    signal.signal(signal.SIGTERM, stop)
    rc = proc.wait()
    try:
        save_state(state_url)
        print("[smkflex] saved .snakemake state", file=sys.stderr)
    except Exception as e:
        print(f"[smkflex] could not save .snakemake state: {e!r}", file=sys.stderr)
    print(f"[smkflex] snakemake exited with {rc}", file=sys.stderr)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
