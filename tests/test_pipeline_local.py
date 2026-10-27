"""pipeline.smk through the plugin + fake Batch (container: ignored locally, tools from the machine).
Skipped unless bwa/samtools/bcftools/fastp are installed."""
import shutil
import pytest
from test_conformance import run_conf

pytestmark = pytest.mark.skipif(not all(shutil.which(t) for t in ("bwa", "samtools", "bcftools", "fastp")),
                                reason="bio tools not installed")


def test_multi_image_pipeline(tmp_path):
    r, calls = run_conf(tmp_path, "pipeline.smk", "-j", "6")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-3000:]
    rows = [l.split("\t") for l in (tmp_path / "store/results/summary.tsv").read_text().splitlines()[1:]]
    assert [x[0] for x in rows] == ["s1", "s2", "s3"] and all(int(x[1]) >= 5 for x in rows), rows
    assert len(list((tmp_path / "store/results/checks").glob("*.txt"))) == 6
    subs = [c for c in calls if c["call"] == "submit_job"]
    assert len({c["jobdef"] for c in subs}) >= 6          # a job definition per image
