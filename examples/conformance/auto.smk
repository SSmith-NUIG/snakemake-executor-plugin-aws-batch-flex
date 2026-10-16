rule all:
    input: "results/auto_py.txt", "results/auto_bare.txt"
rule auto_py:
    output: "results/auto_py.txt"
    container: "docker://python:3.12-slim"
    resources: mem_mb=1500
    shell: "python --version > {output} 2>&1"
rule auto_bare:
    output: "results/auto_bare.txt"
    container: "docker://debian:12"
    resources: mem_mb=1500
    shell: "echo hi > {output}"
