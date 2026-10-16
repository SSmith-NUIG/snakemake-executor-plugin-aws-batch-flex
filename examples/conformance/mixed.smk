# One rule on the Fargate queue, the rest on EC2. run.sh passes the Fargate flags.
container: "docker://debian:12"

rule all:
    input: "results/mixed/both.txt"

rule on_ec2:                             # main queue (EC2, host-mounted env)
    output: "results/mixed/ec2.txt"
    resources: mem_mb=512
    shell: "echo ${{AWS_EXECUTION_ENV:-unset}} > {output}"

rule on_fargate:                         # Fargate queue: installs Snakemake at job start
    output: "results/mixed/fargate.txt"
    resources: mem_mb=512, batch_queue=config["fargate_queue"]
    shell: "echo ${{AWS_EXECUTION_ENV:-unset}} > {output}"

rule both:                               # back on EC2, reads the Fargate job's output
    input: "results/mixed/ec2.txt", "results/mixed/fargate.txt"
    output: "results/mixed/both.txt"
    resources: mem_mb=512
    shell: "cat {input} > {output}"
