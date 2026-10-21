# Setup

You need a Batch queue whose instances have the Snakemake environment. In the repository,
`bash scripts/setup.sh --region eu-west-1` builds the AMI, creates the queue, compute
environment, roles and S3 bucket with Terraform, and writes a profile; the [README](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex#getting-started)
walks through it step by step, from an empty AWS account to a finished test run.

To try the plugin on an existing queue without building an AMI, set
`aws-batch-flex-bootstrap: auto`: Snakemake is then installed into each container when its
job starts (about 10-15 s per job; needs internet and `curl`, `wget` or `pip` in the image).

# Example profile

```yaml
executor: aws-batch-flex
default-storage-provider: s3
default-storage-prefix: s3://my-bucket/snakemake
aws-batch-flex-queue: my-queue
aws-batch-flex-region: eu-west-1
aws-batch-flex-job-role-arn: arn:aws:iam::123456789012:role/snakemake-job
aws-batch-flex-bootstrap: host
aws-batch-flex-default-image: debian:12
default-resources: [mem_mb=1000]
jobs: 50
```

Terraform's `snakemake_profile` output prints one filled in for your account.

# Rules

```python
rule align:
    input: "data/{sample}.fq"
    output: "results/{sample}.sam"
    container: "docker://quay.io/biocontainers/bwa:0.7.17--h7132678_9"
    threads: 8
    resources: mem_mb=16000, runtime=120, batch_queue="big-queue", batch_tags="stage=align"
    shell: "bwa mem -t {threads} ref.fa {input} > {output}"
```

- `threads` -> vCPUs, `mem_mb` -> memory (plus 256 MiB for the Snakemake process in the job),
  `runtime` (minutes) -> timeout, `gpu` -> GPUs.
- `disk_mb` sets the job's disk on Fargate (up to 200 GiB). On EC2, jobs share the
  instance's disk and `disk_mb` isn't enforced.
- `batch_shm_mb` sets the size of `/dev/shm` (EC2 only); `batch_spot_retries` and
  `batch_priority` set Spot retries and fair-share priority per rule.
- `batch_job_definition` (or `aws-batch-flex-job-definition` for all rules) uses an existing job
  definition instead of registering one.
- `batch_queue` sends a rule to another queue; EC2 and Fargate queues can be mixed in one run.
- `batch_tags` adds tags to that rule's jobs; every job is also tagged with the run, workflow,
  rule and job ID.
- Python `script:` rules run with the image's own Python (3.10 or newer), so they can use its
  packages; `batch_script_python="host"` uses Snakemake's instead.
- `conda:` and `wrapper:` work with `--sdm conda` (conda from the image, or baked into the AMI).
- If `snakemake` is killed, jobs it left running are cancelled when you restart it.
- Private images: ECR works through the IAM roles; other registries through Secrets Manager
  (`aws-batch-flex-registry-credentials`).

# Limits

- Host mode needs glibc images with `/bin/sh` and bash (no Alpine).
- `pipe()` doesn't work; Singularity/Apptainer images can't be used.
- All rules in a group must use the same image.

The [README](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex) has the
full list of options, what has been tested on AWS, and the IAM permissions needed.
