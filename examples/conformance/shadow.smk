# shadow: rule; scratch files shouldn't end up in storage.
container: "docker://debian:12"
rule all:
    input: "results/shadow/out.txt"
rule shadowed:
    output: "results/shadow/out.txt"
    shadow: "minimal"
    resources: mem_mb=256
    shell: "echo scratch > scratch.tmp; echo kept > {output}"
