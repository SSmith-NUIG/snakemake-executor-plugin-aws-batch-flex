rule all:
    input: "results/fg_bare.txt", "results/fg_py.txt"
rule fg_bare:
    output: "results/fg_bare.txt"
    container: "docker://debian:12"
    resources: mem_mb=1500
    shell: "echo fargate-bare > {output}"
rule fg_py:
    output: "results/fg_py.txt"
    container: "docker://python:3.12-slim"
    resources: mem_mb=1500
    shell: "python --version > {output} 2>&1"
