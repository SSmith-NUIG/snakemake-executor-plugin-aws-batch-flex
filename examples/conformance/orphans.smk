# Slow jobs for orphan-test.sh: they need to still be active when Snakemake is restarted.
container: "docker://debian:12"

rule all:
    input: expand("results/orph/{i}.txt", i=range(3))

rule slow:
    output: "results/orph/{i}.txt"
    resources: mem_mb=256
    shell: "sleep 90; echo {wildcards.i} > {output}"
