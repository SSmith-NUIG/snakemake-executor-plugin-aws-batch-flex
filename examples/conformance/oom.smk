# Runs out of memory on the first attempt (needs ~700 MiB, asks for ~540 with the 256 MiB
# headroom); with --retries 1 the plugin retries with twice the memory, and it fits.
container: "docker://debian:12"

rule all:
    input: "results/oom/out.txt"

rule needs_more:
    output: "results/oom/out.txt"
    resources: mem_mb=300
    # tail keeps everything after the last newline, and there is none: ~700 MiB in memory
    shell: "head -c 700M /dev/zero | tail -n 1 | wc -c > /dev/null; echo ok attempt={resources.mem_mb} > {output}"
