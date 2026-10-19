# A rule whose image is in a private ECR repository; run.sh ecr creates and removes the repo.
rule all:
    input: "results/ecr/os.txt"

rule from_ecr:
    output: "results/ecr/os.txt"
    container: config["ecr_image"]
    resources: mem_mb=512
    shell: "grep PRETTY_NAME /etc/os-release > {output}"
