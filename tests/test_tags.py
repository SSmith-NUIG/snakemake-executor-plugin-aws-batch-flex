import textwrap
import pytest
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_executor_plugin_aws_batch_flex.tags import job_tags, parse_pairs, parse_resource_string
from test_integration import run


def test_parse_and_validate():
    assert parse_pairs(["a=1", " b = two words "], "x") == {"a": "1", "b": "two words"}
    assert parse_resource_string("stage=align, team=bio") == {"stage": "align", "team": "bio"}
    assert parse_resource_string(None) == {}
    for bad in (["novalue"], ["=v"], ["aws:x=1"]):
        with pytest.raises(WorkflowError):
            parse_pairs(bad, "x")


def test_sanitising_and_precedence():
    t = job_tags(run_id="r1", rule_names=["b", "a"], jobid=7,
                 global_tags={"team": "g", "snakemake-rule": "mine"},
                 rule_tags={"team": "rule"}, auto=True)
    assert t["team"] == "rule"                 # per-rule beats global
    assert t["snakemake-rule"] == "mine"       # user tag beats automatic
    assert t["snakemake-job-id"] == "7" and t["snakemake-run-id"] == "r1"
    t = job_tags(run_id="r", rule_names=["b", "a"], jobid=1, global_tags={}, rule_tags={}, auto=True)
    assert t["snakemake-rule"] == "a+b"        # group jobs: sorted, '+'-joined
    assert parse_pairs(["k=sample#1,x"], "x") == {"k": "sample_1_x"}  # illegal chars replaced


def test_tag_limit():
    with pytest.raises(WorkflowError):
        job_tags(run_id="r", rule_names=["a"], jobid=1,
                 global_tags={f"k{i}": "v" for i in range(60)}, rule_tags={}, auto=True)


SNAKEFILE = textwrap.dedent('''
    rule all:
        input: "a.txt", "b.txt"
    rule align:
        output: "a.txt"
        container: "docker://img:1"
        resources: batch_tags="stage=align,team=bio"
        shell: "echo A > {output}"
    rule plain:
        output: "b.txt"
        container: "docker://img:1"
        shell: "echo B > {output}"
''')


def _subs(calls):
    return [c for c in calls if c["call"] == "submit_job"]


def test_tags_reach_submit_job(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE, "--aws-batch-flex-tags", "team=genomics", "project=rnaseq")
    assert r.returncode == 0, r.stdout + r.stderr
    by_rule = {c["tags"]["snakemake-rule"]: c for c in _subs(calls)}
    a, b = by_rule["align"], by_rule["plain"]
    assert a["tags"]["team"] == "bio" and a["tags"]["stage"] == "align"   # per-rule override
    assert b["tags"]["team"] == "genomics" and "stage" not in b["tags"]
    assert a["tags"]["project"] == b["tags"]["project"] == "rnaseq"       # global on both
    assert a["tags"]["snakemake-run-id"] == b["tags"]["snakemake-run-id"]
    assert a["tags"]["snakemake-job-id"] != b["tags"]["snakemake-job-id"]
    assert a["propagate"] is True


def test_auto_tags_can_be_disabled(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE, "--aws-batch-flex-no-auto-tags")
    assert r.returncode == 0, r.stdout + r.stderr
    for c in _subs(calls):
        assert not any(k.startswith("snakemake-") for k in (c["tags"] or {}))


def test_list_settings_parse_from_cli_and_profile(tmp_path):
    """Regression: list settings used to be declared with a type argparse could not parse."""
    (tmp_path / "prof").mkdir()
    (tmp_path / "prof" / "config.yaml").write_text(
        "aws-batch-flex-tags: [from=profile, other=1]\n")
    r, calls = run(tmp_path, SNAKEFILE, "--profile", str(tmp_path / "prof"),
                   "--aws-batch-flex-volumes", "/scratch:/scratch", "/ref:/ref:ro",
                   "--aws-batch-flex-extra-packages", "pandas")
    assert r.returncode == 0, r.stdout + r.stderr
    reg = next(c for c in calls if c["call"] == "register_job_definition")
    assert {"sourceVolume": "vol1", "containerPath": "/ref", "readOnly": True} in reg["mounts"]
    assert all(c["tags"]["from"] == "profile" for c in _subs(calls))


def test_job_definitions_are_tagged_managed_by(tmp_path):
    r, calls = run(tmp_path, SNAKEFILE, "--aws-batch-flex-tags", "project=x")
    assert r.returncode == 0, r.stdout + r.stderr
    regs = [c for c in calls if c["call"] == "register_job_definition"]
    assert regs and all(c["tags"] == {"project": "x", "ManagedBy": "snakemake-batch-flex"} for c in regs)
