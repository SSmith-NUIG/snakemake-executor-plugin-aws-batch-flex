# The common case: the Snakefile reads its sample sheet with pandas, and a rule runs in a bioconda
# image (no libstdc++ where Linux looks). Every job reads the Snakefile again, so the AMI's pandas
# has to load in that image.
import pandas as pd

SAMPLES = pd.read_csv("config/pdsheet.tsv", sep="\t")["sample"].tolist()

rule all:
    input: expand("results/pdsheet/{s}.txt", s=SAMPLES)

rule tool:
    output: "results/pdsheet/{s}.txt"
    container: "docker://quay.io/biocontainers/samtools:1.21--h50ea8bc_0"
    resources: mem_mb=1024
    shell: "samtools --version | head -1 > {output}; echo {wildcards.s} >> {output}"
