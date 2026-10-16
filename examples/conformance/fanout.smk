# Fan-out: n_units jobs plus a gather, e.g. --config n_units=200 -j 200
N = int(config.get("n_units", 100))
container: "docker://debian:12"

rule all:
    input: "results/total.txt"

rule unit:
    output: "results/units/{i}.txt"
    resources: mem_mb=256
    shell: "echo {wildcards.i} > {output}"

rule gather:
    input: expand("results/units/{i}.txt", i=range(N))
    output: "results/total.txt"
    resources: mem_mb=256
    shell: "cat {input} | awk '{{s+=$1}} END {{print s}}' > {output}"
