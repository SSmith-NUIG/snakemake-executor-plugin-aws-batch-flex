# A checkpoint whose downstream job count is only known after it runs.
container: "docker://debian:12"
rule all:
    input: "results/cp/total.txt"
checkpoint make_n:
    output: "results/cp/n.txt"
    resources: mem_mb=256
    shell: "echo 4 > {output}"
def units(wc):
    with checkpoints.make_n.get().output[0].open() as f:
        n = int(f.read())
    return expand("results/cp/u{i}.txt", i=range(n))
rule unit:
    output: "results/cp/u{i}.txt"
    resources: mem_mb=256
    shell: "echo {wildcards.i} > {output}"
rule total:
    input: units
    output: "results/cp/total.txt"
    resources: mem_mb=256
    shell: "cat {input} | awk '{{s+=$1}} END {{print s}}' > {output}"
