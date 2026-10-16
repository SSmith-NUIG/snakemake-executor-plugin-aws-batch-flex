# Python and bash script: rules in debian:12, which has no Python.
container: "docker://debian:12"        # no python in the image; Python scripts run in the host env's Python

rule all:
    input: "results/py_script.txt", "results/sh_script.txt"

rule py_script:                          # Python script (stdlib only: the image has no python)
    output: "results/py_script.txt"
    params: word="pyscript"
    threads: 2
    resources: mem_mb=512
    script: "scripts/py_script.py"

rule sh_script:                          # bash script (uses the image's bash)
    output: "results/sh_script.txt"
    params: word="shscript"
    resources: mem_mb=512
    script: "scripts/sh_script.sh"
