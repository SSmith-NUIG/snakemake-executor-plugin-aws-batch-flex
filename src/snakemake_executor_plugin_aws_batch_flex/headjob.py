"""Runs inside the head job that `smkflex run` submits: restores Snakemake's .snakemake state,
runs Snakemake on the unpacked workflow, then saves the state for the next run.

`smkflex run` copies this file into the workflow bundle, so it only uses the standard library
(and boto3, which comes with the S3 storage plugin)."""

import io
import os
import signal
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading

# The parts of .snakemake that make a rerun behave like one on the same machine: what was
# made with which code and parameters, and which jobs didn't finish. Conda envs, local
# copies of storage files and the like are left out.
STATE_DIRS = ("metadata", "incomplete", "log")
# The same records with Snakemake's --persistence-backend db (SQLite at its default place)
DB = os.path.join(".snakemake", "metadata.db")
SAVE_EVERY = 300  # seconds between saves of the state while Snakemake runs (unchanged: skipped)
WRAPPER_PREFIX = "https://github.com/snakemake/snakemake-wrappers/raw/"


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


def delete(url):
    if url.startswith("s3://"):
        s3, bucket, key = _s3(url)
        s3.delete_object(Bucket=bucket, Key=key)
    elif os.path.exists(url):
        os.remove(url)


def unpack(data, dest):
    with tarfile.open(fileobj=io.BytesIO(data)) as t:
        if hasattr(tarfile, "data_filter"):
            t.extractall(dest, filter="data")
        else:
            t.extractall(dest)


def _state_files():
    """[(path, stat)] of the state files now; files removed meanwhile (a job ended) are left out.
    The database's write-ahead log counts too: recent changes can be only there."""
    out = []
    for p in (DB, DB + "-wal"):
        try:
            out.append((p, os.stat(p)))
        except OSError:
            pass
    for d in STATE_DIRS:
        for root, _dirs, names in os.walk(os.path.join(".snakemake", d)):
            for n in names:
                p = os.path.join(root, n)
                try:
                    out.append((p, os.stat(p)))
                except OSError:
                    pass
    return sorted(out, key=lambda f: f[0])


def save_state(url, last=None):
    """Upload the state. Snakemake may still be running, so files can change or go while they're
    read; those are left for the next save. Returns what was saved: pass it back as `last` and
    an unchanged state isn't uploaded again."""
    files = _state_files()
    saved = [(p, st.st_size, st.st_mtime_ns) for p, st in files]
    if saved == last:
        return last
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for p, st in files:
            if p == DB + "-wal":
                continue  # in the snapshot of the database
            try:
                if p == DB:
                    data = _db_snapshot()
                else:
                    with open(p, "rb") as f:
                        data = f.read()
            except (OSError, sqlite3.Error):
                continue
            info = tarfile.TarInfo(p)
            info.size, info.mtime, info.mode = len(data), int(st.st_mtime), st.st_mode & 0o777
            t.addfile(info, io.BytesIO(data))
    put(url, buf.getvalue())
    return saved


def _db_snapshot():
    """The database as one consistent file, with what's in its write-ahead log, although
    Snakemake may be writing to it: copying the file itself could catch it halfway."""
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        src, dst = sqlite3.connect(DB), sqlite3.connect(tmp)
        try:
            src.backup(dst)
        finally:
            src.close()
            dst.close()
        with open(tmp, "rb") as f:
            return f.read()
    finally:
        os.remove(tmp)


class StateSaver:
    """Saves the state every `every` seconds while Snakemake runs, and at once after wake.set(),
    so a head job that dies (out of memory, instance lost) or is killed while stopping keeps
    most of it."""

    def __init__(self, url, every):
        self.url, self.every, self.last = url, every, None
        self.wake, self.done = threading.Event(), threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while True:
            self.wake.wait(self.every)
            self.wake.clear()
            if self.done.is_set():
                return
            self._save()

    def _save(self):
        try:
            self.last = save_state(self.url, self.last)
            return True
        except Exception as e:
            print(f"[smkflex] could not save .snakemake state: {e!r}", file=sys.stderr)
            return False

    def finish(self):
        """Stop the saves in the background, then save the final state. True if it was saved."""
        self.done.set()
        self.wake.set()
        self.thread.join()
        return self._save()


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
    if not any(a == "--wrapper-prefix" or a.startswith("--wrapper-prefix=") for a in args):
        # Without a prefix, Snakemake fetches version-tagged wrappers with git clone, and the
        # head job's image usually has no git; the same files are on GitHub over plain HTTPS.
        cmd += ["--wrapper-prefix", WRAPPER_PREFIX]
    print("[smkflex] " + " ".join(cmd), file=sys.stderr, flush=True)
    proc = subprocess.Popen(cmd, env=env)
    saver = StateSaver(state_url, SAVE_EVERY)

    # `smkflex cancel` (Batch's terminate) sends SIGTERM; Snakemake cancels its jobs on SIGINT.
    # The state is saved at once too, in case the head job is killed before Snakemake is done.
    def stop(_sig, _frame):
        proc.send_signal(signal.SIGINT)
        saver.wake.set()

    signal.signal(signal.SIGTERM, stop)
    rc = proc.wait()
    if saver.finish():
        print("[smkflex] saved .snakemake state", file=sys.stderr)
    bundle = os.environ.get("SMKFLEX_BUNDLE_URL")
    if bundle:  # the uploaded workflow isn't needed any more
        try:
            delete(bundle)
        except Exception as e:
            print(f"[smkflex] could not remove {bundle}: {e!r}", file=sys.stderr)
    print(f"[smkflex] snakemake exited with {rc}", file=sys.stderr)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
