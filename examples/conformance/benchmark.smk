# benchmark: and log: files go to storage next to the outputs.
container: "docker://debian:12"
rule all:
    input: "results/bm/out.txt"
rule timed:
    output: "results/bm/out.txt"
    benchmark: "results/bm/bench.tsv"
    log: "results/bm/log.txt"
    resources: mem_mb=256
    shell: "echo logline > {log}; echo done > {output}"
