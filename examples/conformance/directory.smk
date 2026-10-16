# directory() outputs on S3 (Snakemake marks this experimental).
container: "docker://debian:12"
rule all:
    input: "results/dr/total.txt"
rule mk:
    output: directory("results/dr/files")
    resources: mem_mb=256
    shell: "mkdir -p {output}; echo 1 > {output}/a.txt; echo 2 > {output}/b.txt"
rule sumup:
    input: "results/dr/files"
    output: "results/dr/total.txt"
    resources: mem_mb=256
    shell: "cat {input}/*.txt | awk '{{s+=$1}} END {{print s}}' > {output}"
