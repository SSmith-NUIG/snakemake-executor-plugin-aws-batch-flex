# temp() output should be deleted from storage once it's used.
container: "docker://debian:12"
rule all:
    input: "results/td/final.txt"
rule make:
    output: temp("results/td/tmp.txt")
    resources: mem_mb=256
    shell: "echo x > {output}"
rule use:
    input: "results/td/tmp.txt"
    output: "results/td/final.txt"
    resources: mem_mb=256
    shell: "cat {input} > {output}"
