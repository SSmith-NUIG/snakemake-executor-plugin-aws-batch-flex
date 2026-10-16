# pipe() between two rules in one group.
container: "docker://debian:12"
rule all:
    input: "results/pipe/out.txt"
rule produce:
    output: pipe("results/pipe/stream")
    resources: mem_mb=256
    shell: "printf 'a\\nb\\nc\\n' > {output}"
rule consume:
    input: "results/pipe/stream"
    output: "results/pipe/out.txt"
    resources: mem_mb=256
    shell: "wc -l < {input} > {output}"
