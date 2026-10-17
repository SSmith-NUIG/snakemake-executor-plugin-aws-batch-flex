# snakemake-executor-plugin-aws-batch-flex

A Snakemake executor for AWS Batch where each rule runs in whatever image its `container:`
directive names. The images don't need Snakemake (or Python) inside them.

On EC2, a Snakemake environment is baked into the AMI at `/opt/snakemake-env` and mounted
read-only into every job, the same trick Nextflow uses for the AWS CLI. On Fargate, where
there's no host to mount from, Snakemake is installed into the container when the job starts.

## Quick start

```bash
pip install snakemake-executor-plugin-aws-batch-flex snakemake-storage-plugin-s3
```

You need a Batch queue whose instances have the Snakemake env. `terraform/` sets up a
compute environment, queue, roles and bucket, and `scripts/bake-ami.sh` builds the AMI; see
[terraform/README.md](terraform/README.md). To try it on an existing queue first, use
`aws-batch-flex-bootstrap: auto` instead of `host` (installs at job start, ~10-15 s per job,
needs internet and `curl`, `wget` or `pip` in the image).

Profile (`profile/config.yaml`):

```yaml
executor: aws-batch-flex
default-storage-provider: s3
default-storage-prefix: s3://my-bucket/snakemake
aws-batch-flex-queue: my-queue
aws-batch-flex-bootstrap: host
aws-batch-flex-job-role-arn: arn:aws:iam::123456789012:role/snakemake-job
aws-batch-flex-default-image: debian:12
default-resources: [mem_mb=1000]
jobs: 50
```

Snakefile:

```python
rule align:
    input: "reads.fq"
    output: "aligned.bam"
    container: "docker://staphb/bwa:0.7.19"
    threads: 8
    resources: mem_mb=16000, runtime=120
    shell: "bwa mem -t {threads} ref.fa {input} > {output}"
```

Then `snakemake --profile profile`. All settings: `snakemake --help | grep aws-batch-flex`.

## How rules map to Batch

- `container:` is the image (`docker://` stripped). One job definition is registered per
  image and platform and reused across runs.
- `threads` -> vCPU, `mem_mb`/`mem_mib` -> memory, `gpu` -> GPU, `runtime` (minutes) -> timeout.
- Memory is a hard limit and the Snakemake process in the job uses some of it, so 256 MiB is
  added to explicit memory requests (`--aws-batch-flex-mem-overhead-mib`, 0 to turn off).
  Rules with no memory request get 2048 MiB.
- `batch_queue="other-queue"` sends one rule to another queue. EC2 and Fargate queues can be
  mixed in a run; the platform is read from each queue's compute environment (or set per rule
  with `batch_platform`). Fargate jobs need `--aws-batch-flex-execution-role-arn` and get no
  host mounts.
- Every job is tagged with `snakemake-run-id`, `snakemake-rule` and `snakemake-job-id`. Add
  your own with `aws-batch-flex-tags: [project=x]` or per rule with
  `resources: batch_tags="stage=align"`. Tags propagate to the ECS tasks.

## Private images

ECR images need nothing extra: the instance role (EC2) or execution role (Fargate) pulls them,
and the roles from `terraform/` already have ECR read access. For another account's ECR, that
repository's policy has to allow those roles.

For registries outside AWS (Docker Hub, GitHub, Quay...), store `{"username": ..., "password": ...}`
in Secrets Manager and map the registry to it:

```yaml
aws-batch-flex-registry-credentials: [docker.io=arn:aws:secretsmanager:eu-west-1:123456789012:secret:dockerhub]
aws-batch-flex-execution-role-arn: arn:aws:iam::123456789012:role/my-exec-role
```

Only images from that registry get the credentials. ECS reads the secret with the execution
role, so it's needed on EC2 too, and it needs `secretsmanager:GetSecretValue` on the secret.

## Conda and wrappers

Run with `--sdm conda`. Jobs need `conda`: either the image has it, or bake Miniforge into
the AMI (`bake-ami.sh --conda-env env.yaml`) and set `aws-batch-flex-conda-path: /opt/miniforge3`.
Only `condabin` is added to PATH, so an image's own conda still wins. Envs baked with
`--conda-env` can be used by name (`conda: "rnaseq"`); YAML files are built inside the job.
The machine you run `snakemake` on needs `conda` on its PATH as well.

Wrappers work the same way. The plugin passes jobs an HTTP `--wrapper-prefix`, so images don't
need git.

`scripts/update-ami.sh --conda-env new.yaml` bakes a new AMI on top of the current one; a
`terraform apply` then replaces the compute environment but keeps the queue.

## What's been tested

Run on real AWS (eu-west-1):
`shell:`, `run:`, Python/bash/R `script:`, `wrapper:`, conda (YAML in a conda image, YAML and
named envs in conda-less images), a different image per rule, a 6-image variant-calling
pipeline, EC2 and Fargate (separately and mixed in one run), checkpoints, groups, shadow,
benchmark/log, retries, `temp()`, `directory()`, resources and tags, OOM and timeout
reporting, cancelling with Ctrl-C, startup checks, fan-out to 100 jobs, install-at-start on a
bare `debian:12`.

Only tested against a fake Batch: `--aws-batch-flex-volumes`, `update-ami.sh`, a single Ctrl-C.
Not tested yet: `notebook:`, Julia, GPU, Spot, the `terraform/launcher` module.

## Limits

- Host mode needs a glibc image with `/bin/sh` and bash. Alpine/musl images exit with 127 and
  say why. Build the AMI on the same CPU architecture as your instances.
- Python `script:` rules use the image's `python` if it's 3.10 or newer, so they see the
  image's packages. Otherwise (no Python, or an older one) and for `run:` blocks, the code runs
  in Snakemake's own Python from the host env; add libraries there with `--extra-package` when
  building it, or call the script from `shell:`. `--aws-batch-flex-script-python host` (or
  `resources: batch_script_python="host"` on one rule) uses Snakemake's Python instead.
- `pipe()` doesn't work. `--sdm apptainer` isn't supported.
- Images with an ENTRYPOINT that doesn't pass the command through will break.
- All rules in a group must use the same image.
- Install-at-start fails on very old images (e.g. glibc 2.23).
- If the machine running `snakemake` dies mid-run, a restart can resubmit jobs that were
  still running.

## IAM

The identity running `snakemake` needs `batch:SubmitJob`, `DescribeJobs`, `TerminateJob`,
`RegisterJobDefinition`, `DescribeJobDefinitions`, `DescribeJobQueues`,
`DescribeComputeEnvironments` and `TagResource`, plus `logs:GetLogEvents`, `iam:PassRole` on
the job role(s), and read/write on the S3 prefix. The job role needs read/write on the prefix.

## License

MIT, see [LICENSE](LICENSE).
