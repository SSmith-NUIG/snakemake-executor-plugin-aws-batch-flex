# wrapper: needs --sdm conda (wrappers import snakemake_wrapper_utils from their env).
# Here conda comes from the AMI mount, so the image itself has no conda.
container: "docker://debian:12"

rule all:
    input: "results/wr/ref.fa.fai"

rule make_fasta:
    output: "results/wr/ref.fa"
    resources: mem_mb=512
    shell: "printf '>chr1\\nACGTACGTACGT\\n>chr2\\nGGGGCCCC\\n' > {output}"

rule faidx:
    input: "results/wr/ref.fa"
    output: "results/wr/ref.fa.fai"
    log: "results/wr/faidx.log"
    resources: mem_mb=2048
    wrapper: "v9.19.1/bio/samtools/faidx"
