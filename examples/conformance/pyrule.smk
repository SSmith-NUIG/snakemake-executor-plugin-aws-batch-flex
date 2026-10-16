# Same image for both rules; one of them asks for Snakemake's python instead of the image's.
container: "docker://quay.io/biocontainers/pandas:2.2.1"

rule all:
    input: "results/pyrule/image.txt", "results/pyrule/host.txt"

rule image_python:
    output: "results/pyrule/image.txt"
    resources: mem_mb=1024
    script: "scripts/which_python.py"

rule host_python:
    output: "results/pyrule/host.txt"
    resources: mem_mb=1024, batch_script_python="host"
    script: "scripts/which_python.py"
