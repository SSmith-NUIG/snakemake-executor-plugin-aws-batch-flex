"""smkflex push and pull: files to and from the workflow's storage, at the paths Snakemake maps
them to, 16 at a time; unchanged ones are skipped, as aws s3 sync does."""

import os
import shutil
import sys
import time

import boto3
from botocore.config import Config as BotoConfig

from .profiles import Fail, _load_profile
from .sources import SKIP_DIRS, workflow_files


def _remote_files(prefix, path, s3):
    """(relative path, size, modification time) of the files under prefix/path, in storage."""
    path = path.strip("/")
    if prefix.startswith("s3://"):
        bucket, _, base = prefix[len("s3://"):].partition("/")
        base = base.strip("/")
        key = f"{base}/{path}" if base else path
        out = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key):
            for o in page.get("Contents", []):
                rel = o["Key"][len(base) + 1:] if base else o["Key"]
                # "results" must not also match "results2/..."
                if rel == path or rel.startswith(path + "/") or not path:
                    out.append((rel, o["Size"], o["LastModified"].timestamp()))
        return out
    root = os.path.join(prefix, path)
    if os.path.isfile(root):
        return [(path, os.path.getsize(root), os.path.getmtime(root))]
    out = []
    for d, _dirs, names in os.walk(root):
        for n in names:
            full = os.path.join(d, n)
            out.append((os.path.relpath(full, prefix), os.path.getsize(full), os.path.getmtime(full)))
    return sorted(out)

def _same(size, mtime, other_size, other_mtime):
    """A copy is up to date if it has the same size and was made after the original last
    changed, as `aws s3 sync` decides (a copy's time is when it was copied)."""
    return other_size == size and other_mtime >= mtime

def _bucket_key(prefix, rel):
    """(bucket, key) of the file at `rel` under an s3:// prefix."""
    bucket, _, base = prefix[len("s3://"):].partition("/")
    base = base.strip("/")
    return bucket, f"{base}/{rel}" if base else rel

TRANSFER_THREADS = 16  # files at once; big files are also split into parallel parts by boto3

def _storage_path(path):
    """Where Snakemake puts a workflow path in storage, relative to the prefix: as it is, or
    for an absolute path without its leading /."""
    norm = os.path.normpath(path)
    if norm == ".." or norm.startswith(".." + os.sep):
        raise Fail(f"{path}: Snakemake can't map paths outside the workflow folder to storage; "
                   "use the path the workflow names (relative, or absolute like /shared/refs)")
    return norm.lstrip("/") if os.path.isabs(norm) else norm

def _transfer(jobs, work, verb):
    """Run work(item) for every item, TRANSFER_THREADS at a time; prints each as it finishes."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    done = total = 0
    start = time.time()
    with ThreadPoolExecutor(TRANSFER_THREADS) as pool:
        futures = {pool.submit(work, item): item for item in jobs}
        for f in as_completed(futures):
            name, size = futures[f][0], futures[f][-1]
            f.result()  # re-raises the first error
            done += 1
            total += size
            print(f"{verb} {name}")
    secs = max(time.time() - start, 0.001)
    return done, total, secs

def _rate(total, secs):
    """How much was sent, and how fast when that means something (1 MB or more, over a second)."""
    if not total:
        return ""
    size = f"{total / 1e6:.1f} MB" if total >= 1e5 else f"{total / 1e3:.1f} kB" if total >= 1e3 else f"{total} bytes"
    return f"{size} in {secs:.0f} s ({total / 1e6 / secs:.1f} MB/s)" if total >= 1e6 and secs >= 1 else size

def cmd_pull(a):
    _dir, profile = _load_profile(a.profile)
    prefix = str(a.prefix or profile.get("default-storage-prefix") or "").rstrip("/")
    if not prefix:
        raise Fail("no default-storage-prefix in the profile")
    region = profile.get("aws-batch-flex-region")
    s3 = boto3.client("s3", config=BotoConfig(region_name=region, max_pool_connections=TRANSFER_THREADS * 2)) \
        if prefix.startswith("s3://") else None
    if not a.paths:
        tops = sorted({rel.split("/")[0] for rel, *_ in _remote_files(prefix, "", s3)
                       if not rel.startswith((".smkflex/", "snakemake-workflow-sources."))})
        raise Fail("say what to download, e.g. `smkflex pull results`. In storage: " + (", ".join(tops) or "nothing"))
    todo, skipped = [], 0
    for path in a.paths:
        files = _remote_files(prefix, _storage_path(path), s3)
        if not files:
            print(f"nothing in storage under {path}", file=sys.stderr)
        for rel, size, mtime in files:
            dest = os.path.join(a.to, rel)
            if not a.force and os.path.isfile(dest) and _same(size, mtime, os.path.getsize(dest), os.path.getmtime(dest)):
                skipped += 1
                continue
            if a.dry_run:
                print(f"would download {rel} ({size} bytes)")
                continue
            todo.append((rel, dest, size))

    def get(item):
        rel, dest, _size = item
        os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
        if s3:
            s3.download_file(*_bucket_key(prefix, rel), dest)
        else:
            shutil.copyfile(os.path.join(prefix, rel), dest)

    if not a.dry_run:
        got, total, secs = _transfer(todo, get, "downloaded")
        rate = _rate(total, secs)
        print(f"{got} downloaded" + (f" ({rate})" if rate else "")
              + (f", {skipped} already here (unchanged; --force downloads them again)" if skipped else ""))
    return 0

def cmd_push(a):
    """Upload local input files to where the workflow's rules look for them in storage: a
    relative path to the same path under the prefix, an absolute one (/shared/refs) to
    prefix/shared/refs, as Snakemake maps them."""
    _dir, profile = _load_profile(a.profile)
    prefix = str(profile.get("default-storage-prefix") or "").rstrip("/")
    if a.prefix:
        prefix = a.prefix.rstrip("/")
    if not prefix:
        raise Fail("no default-storage-prefix in the profile")
    files = {}
    for path in a.paths:
        target = _storage_path(path)
        if not os.path.exists(path):
            raise Fail(f"{path}: no such file or folder")
        if os.path.isfile(path):
            files[target] = path
            continue
        seen = set()
        # reads are often linked in from elsewhere (data/run1 -> /mnt/seq/run1): follow links
        for d, dirs, names in os.walk(path, followlinks=True):
            if os.path.realpath(d) in seen:  # a link back up the tree
                dirs[:] = []
                continue
            seen.add(os.path.realpath(d))
            dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
            for n in names:
                local = os.path.join(d, n)
                files[os.path.normpath(os.path.join(target, os.path.relpath(local, path)))] = local
    s3 = boto3.client("s3", config=BotoConfig(region_name=profile.get("aws-batch-flex-region"),
                                              max_pool_connections=TRANSFER_THREADS * 2)) \
        if prefix.startswith("s3://") else None
    there = {}
    for top in {f.split("/")[0] for f in files}:
        there.update({rel: (size, mtime) for rel, size, mtime in _remote_files(prefix, top, s3)})
    todo, skipped = [], 0
    for rel, local in sorted(files.items()):
        size = os.path.getsize(local)
        if not a.force and rel in there and _same(size, os.path.getmtime(local), *there[rel]):
            skipped += 1
            continue
        if a.dry_run:
            print(f"would upload {rel} ({size} bytes)")
            continue
        todo.append((rel, local, size))

    def put(item):
        rel, local, _size = item
        if s3:
            s3.upload_file(local, *_bucket_key(prefix, rel))
        else:
            os.makedirs(os.path.dirname(os.path.join(prefix, rel)), exist_ok=True)
            shutil.copyfile(local, os.path.join(prefix, rel))

    if not a.dry_run:
        sent, total, secs = _transfer(todo, put, "uploaded")
        rate = _rate(total, secs)
        print(f"{sent} uploaded to {prefix}" + (f" ({rate})" if rate else "")
              + (f", {skipped} already there (unchanged; --force sends them again)" if skipped else ""))
        if sent:  # not again on every push that had nothing new
            _hint_gitignore([p for p in a.paths if not os.path.isabs(p)])
    return 0

def _hint_gitignore(paths):
    """Data uploaded with push reaches the jobs through storage; if smkflex run would also
    pack it with the workflow, say how to leave it out."""
    if not paths:
        return
    sent = set(workflow_files("."))
    tops = sorted({os.path.normpath(p).split(os.sep)[0] for p in paths
                   if any(f == os.path.normpath(p) or f.startswith(os.path.normpath(p) + "/") for f in sent)})
    if tops:
        lines = "".join(f"{t}/\n" if os.path.isdir(t) else f"{t}\n" for t in tops)
        print(f"\nNote: smkflex run would also send {', '.join(tops)} with the workflow (it doesn't need to: the "
              f"jobs read it from storage). To leave it out, add to .gitignore:\n{lines}", end="")
