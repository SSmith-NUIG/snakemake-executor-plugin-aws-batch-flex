# Two grouped rules, which should go to Batch as a single job.
container: "docker://debian:12"
rule all:
    input: "results/grp/final.txt"
rule first:
    output: temp("results/grp/mid.txt")
    group: "g"
    resources: mem_mb=256
    shell: "echo mid > {output}"
rule second:
    input: "results/grp/mid.txt"
    output: "results/grp/final.txt"
    group: "g"
    resources: mem_mb=256
    shell: "cat {input} > {output}; echo second >> {output}"
