# R script: in a plain R image.
container: "docker://rocker/r-ver:4.4.2"

rule all:
    input: "results/r/out.txt"

rule r_script:
    output: "results/r/out.txt"
    params: word="rscript"
    threads: 2
    resources: mem_mb=1024
    script: "scripts/r_script.R"
