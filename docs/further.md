# Setup

You need a Batch queue whose instances have the Snakemake environment. In the repository,
`bash scripts/setup.sh --region eu-west-1` builds the AMI (with Snakemake, conda and pandas),
creates the queue, compute environment, roles and S3 bucket with Terraform, and installs a
profile called `smkflex`, so `snakemake --profile smkflex` works in any folder. The
[README](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex#getting-started)
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
jobs: 100
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
  `runtime` (minutes) -> timeout, `gpu` -> GPUs. A job that runs out of memory is retried with
  twice as much when Snakemake retries it (`--retries`).
- `disk_mb` sets the job's disk on Fargate (up to 200 GiB). On EC2, jobs share the
  instance's disk and `disk_mb` isn't enforced.
- `batch_shm_mb` sets the size of `/dev/shm` (EC2 only); `batch_spot_retries` and
  `batch_priority` set Spot retries and fair-share priority per rule.
- `batch_job_definition` (or `aws-batch-flex-job-definition` for all rules) uses an existing job
  definition instead of registering one.
- `batch_queue` sends a rule to another queue; EC2, Spot and Fargate queues can be mixed in one
  run.
- `batch_tags` adds tags to that rule's jobs; every job is also tagged with the run, workflow,
  rule and job ID.
- Python `script:` rules run with the image's own Python (3.10 or newer), so they can use its
  packages; `batch_script_python="host"` uses Snakemake's instead.
- `conda:` and `wrapper:` work with `--sdm conda` (conda from the image, or baked into the AMI).
  Built envs are reused instead of built in every job: by the jobs on the same EC2 machine, or
  from the bucket for later jobs and runs (`aws-batch-flex-conda-cache`).
- If `snakemake` is killed, jobs it left running are cancelled when you restart it.
- Private images: ECR works through the IAM roles; other registries through Secrets Manager
  (`aws-batch-flex-registry-credentials`).
- Images with an `ENTRYPOINT` (e.g. `ENTRYPOINT ["python3"]`) work: the plugin reads the image
  config and launches the job through the entrypoint. Override the guess with `batch_entrypoint`
  or `aws-batch-flex-container-entrypoint` (`none`, `shell`, `sh-c`, `python`, or a JSON list).
  Entrypoints that can't run `/bin/sh` are reported before submission; wrap the image with
  `ENTRYPOINT []` to run it.

At the end of a run the plugin prints what each rule used and cost. The cost comes from the EC2
instances the run used (their type, on-demand or Spot price, and disks), shared out by each
rule's vCPU-time. The table also shows each rule's peak memory, how many vCPUs were busy at once
and how much disk its jobs took, with `set-resources:` and `set-threads:` blocks to paste into
the project's settings.

# Published pipelines and long runs

Pipelines from the workflow catalog run unchanged: set them up with `snakedeploy`, then
`smkflex init` (the project's own folder in the bucket) and `smkflex push data` (the inputs).
`smkflex run` runs Snakemake itself as a Batch job, so nothing has to stay running on your
machine; `smkflex logs`, `status` and `cancel` follow and stop it.

# Limits

- Host mode needs glibc images with `/bin/sh` and bash (no Alpine).
- `pipe()` doesn't work; Singularity/Apptainer images can't be used.
- All rules in a group must use the same image.

The [README](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex) has the
full list of options, what has been tested on AWS, and the IAM permissions needed.
