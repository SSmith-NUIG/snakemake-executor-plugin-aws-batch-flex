# Fargate disk size from disk_mb: one rule asks for 60000 MB (gets 75 GiB), the other keeps
# Fargate's default 20 GiB. run.sh passes the Fargate flags.
container: "docker://debian:12"

rule all:
    input: "results/disk/check.txt"

rule big:
    output: "results/disk/big.txt"
    resources: mem_mb=512, disk_mb=60000, batch_queue=config["fargate_queue"]
    shell: "df -BG --output=size / | tail -1 | tr -dc 0-9 > {output}"

rule default:
    output: "results/disk/default.txt"
    resources: mem_mb=512, batch_queue=config["fargate_queue"]
    shell: "df -BG --output=size / | tail -1 | tr -dc 0-9 > {output}"

rule check:
    input: big="results/disk/big.txt", default="results/disk/default.txt"
    output: "results/disk/check.txt"
    resources: mem_mb=512
    shell: """
        big=$(cat {input.big}); def=$(cat {input.default})
        if [ "$big" -gt $((def + 40)) ]; then r=ok; else r=bad; fi
        echo "disk $r big=${{big}}G default=${{def}}G" > {output}
    """
