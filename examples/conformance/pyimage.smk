# Python script: in an image that has pandas; the script should use the image's python.
container: "docker://quay.io/biocontainers/pandas:2.2.1"

rule all:
    input: "results/pyimg/out.txt"

rule pandas_script:
    output: "results/pyimg/out.txt"
    params: n=4
    resources: mem_mb=1024
    script: "scripts/pandas_script.py"
