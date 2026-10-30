"""script: rules run with the image's python when it is new enough (see bootstrap._LAUNCHER)."""
import os, subprocess, sys, textwrap
from pathlib import Path
import pytest
from snakemake_executor_plugin_aws_batch_flex.bootstrap import image_python_snippet

SNAKEFILE = textwrap.dedent('''
    rule all:
        input: "out.txt"
    rule s:
        output: "out.txt"
        params: word="hi"
        script: "s.py"
''')
SCRIPT = 'import sys\nopen(snakemake.output[0], "w").write(f"{snakemake.params.word} {sys.executable}")\n'


def _python(min_ok):
    """A python other than the one running the tests, new enough (min_ok) or too old."""
    cands = ["/usr/bin/python3"] + sorted(str(p) for p in Path.home().glob("*conda*/envs/*/bin/python3.*")
                                          if p.name[7:].isdigit() or p.name[7:].replace(".", "").isdigit())
    for c in cands:
        if not os.path.exists(c) or os.path.realpath(c) == os.path.realpath(sys.executable):
            continue
        r = subprocess.run([c, "-c", "import sys; print(sys.version_info >= (3, 10))"], capture_output=True, text=True)
        if r.returncode == 0 and (r.stdout.strip() == "True") == min_ok:
            return c
    return None


def run_job(tmp_path, image_python, mode="image", snakefile=SNAKEFILE, script=SCRIPT):
    (tmp_path / "Snakefile").write_text(snakefile)
    (tmp_path / "s.py").write_text(script)
    img = tmp_path / "imagebin"; img.mkdir()
    os.symlink(image_python, img / "python")
    work = tmp_path / "work"; work.mkdir()
    snippet = image_python_snippet()
    job = (f'set -eu\nexport SMK_FLEX_SCRIPT_PYTHON={mode}\nSMK_WORK={work}\nSMK_PY={sys.executable}\npython() {{ "$SMK_PY" "$@"; }}\n'
           f'{snippet}\ncd {tmp_path}\npython -m snakemake -c1 -q\n')
    env = dict(os.environ, PATH=f"{img}:/usr/bin:/bin")
    r = subprocess.run(["sh", "-c", job], env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stdout + r.stderr
    return (tmp_path / "out.txt").read_text()


@pytest.mark.skipif(not _python(True), reason="needs a second python >= 3.10")
def test_script_uses_image_python(tmp_path):
    py = _python(True)
    assert run_job(tmp_path, py) == f"hi {tmp_path / 'imagebin' / 'python'}"


@pytest.mark.skipif(not _python(False), reason="needs a python < 3.10")
def test_old_image_python_falls_back_to_snakemakes(tmp_path):
    assert run_job(tmp_path, _python(False)) == f"hi {sys.executable}"


@pytest.mark.skipif(not _python(True), reason="needs a second python >= 3.10")
def test_host_setting_keeps_snakemakes_python(tmp_path):
    assert run_job(tmp_path, _python(True), mode="host") == f"hi {sys.executable}"


def _lacks_pandas(py):
    return py and subprocess.run([py, "-c", "import pandas"], capture_output=True).returncode != 0


@pytest.mark.skipif(not _lacks_pandas(_python(True)), reason="needs a second python >= 3.10 without pandas")
def test_script_with_pandas_params_uses_snakemakes_python_when_the_image_has_no_pandas(tmp_path):
    # rna-seq-kallisto-sleuth: a script handed pandas tables as params, in continuumio/miniconda3
    # (no pandas); locally it runs in Snakemake's python, which has pandas
    snakefile = SNAKEFILE.replace('params: word="hi"', 'params: word="hi", table=__import__("pandas").DataFrame({"a": [1]})')
    out = run_job(tmp_path, _python(True), snakefile=snakefile)
    assert out == f"hi {sys.executable}"


def test_pandas_3_arrays_reach_scripts_as_numpy_arrays(tmp_path):
    # rna-seq-kallisto-sleuth: a text column's .values is a pandas StringArray in pandas 3 (numpy
    # in pandas 2), which a script environment with pandas 2 can't load; it goes as numpy
    import pandas as pd
    if not isinstance(pd.Series(["a"], dtype="str").values, pd.api.extensions.ExtensionArray):
        pytest.skip("needs pandas 3")
    pd_ = '__import__("pandas")'
    snakefile = SNAKEFILE.replace('params: word="hi"', f'params: word="hi", samples={pd_}.Series(["a", "b"], dtype="str").values, '
                                  f'groups={pd_}.Categorical(["x"])')
    script = ('p = snakemake.params\n'
              'open(snakemake.output[0], "w").write(f"{p.word} {type(p.samples).__name__} {p.samples.tolist()} '
              '{type(p.groups).__name__}")\n')
    # a Categorical was a pandas object in pandas 2 too: unchanged
    out = run_job(tmp_path, sys.executable, mode="host", snakefile=snakefile, script=script)
    assert out == "hi ndarray ['a', 'b'] Categorical"
