# /dev/shm size: one rule asks for 1024 MiB, the other keeps Docker's default (64 MB).
container: "docker://debian:12"

rule all:
    input: "results/shm/big.txt", "results/shm/default.txt"

rule big:
    output: "results/shm/big.txt"
    resources: mem_mb=2048, batch_shm_mb=1024
    shell: "df -m /dev/shm | awk 'NR==2 {{print $2}}' > {output}"

rule default:
    output: "results/shm/default.txt"
    resources: mem_mb=512
    shell: "df -m /dev/shm | awk 'NR==2 {{print $2}}' > {output}"
