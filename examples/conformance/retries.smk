# Fails on the first attempt and passes on the second (retries: 2).
container: "docker://debian:12"
rule all:
    input: "results/rt/out.txt"
rule flaky:
    output: "results/rt/out.txt"
    retries: 2
    resources: mem_mb=256, attempt_no=lambda wildcards, attempt: attempt
    shell: "if [ {resources.attempt_no} -lt 2 ]; then echo failing >&2; exit 1; fi; echo ok attempt={resources.attempt_no} > {output}"
