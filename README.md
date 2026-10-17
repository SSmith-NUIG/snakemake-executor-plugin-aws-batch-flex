# snakemake-executor-plugin-aws-batch-flex

Run [Snakemake](https://snakemake.readthedocs.io) workflows on [AWS Batch](https://aws.amazon.com/batch/),
with each step running in whatever container image you choose: a stock image from Docker Hub,
quay.io/biocontainers, your own ECR repository, and so on. The images don't need Snakemake or
Python inside them.

In plain terms: you write a workflow on your own computer, and this plugin sends each step to
AWS, where machines are started for you, run the step in its container, save the results to S3
(Amazon's file storage) and shut down again when there's nothing left to do. You pay only while
machines are running.

How it works, briefly: a copy of Snakemake is built into the machine image (an "AMI") that the
AWS machines start from, and every job gets read access to it. That's why your containers don't
need it. On Fargate (AWS's serverless option, no machines to manage) Snakemake is installed into
the container when the job starts instead.

## Getting started

These steps take you from nothing to a finished test run, in about an hour. They assume you're
comfortable typing commands into a terminal, but not that you know AWS or Snakemake.

It has been tested on Linux. On Windows, use [WSL](https://learn.microsoft.com/windows/wsl/install)
and run everything inside it. macOS should work but hasn't been tried.

### 1. What you need

- An AWS account. Setting this up costs very little: building the machine image runs one small
  machine for about 10 minutes, and keeping the image costs well under $1 a month. The test run
  in step 8 costs a few cents.
- Somewhere to run commands: your laptop is fine. It has to stay on (and online) while a
  workflow runs, because it's what tells AWS what to do next.

### 2. Install the tools

Install [Miniforge](https://github.com/conda-forge/miniforge#install) if you don't already have
`conda`. Then get this repository and create an environment with everything you need (Python,
Snakemake, Terraform and the AWS command-line tool):

```bash
git clone https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex.git
cd snakemake-executor-plugin-aws-batch-flex
conda env create -f environment.yml
conda activate smk-aws
pip install -e .
```

Run `conda activate smk-aws` again in every new terminal before the steps below.

Check it worked: `snakemake --version` should print a version number (9.x), and
`snakemake --help | grep aws-batch-flex` should list this plugin's settings.

### 3. Connect to your AWS account

The command-line tools need an access key. In the [AWS console](https://console.aws.amazon.com/):

1. Go to **IAM** > **Users** > **Create user**. Give it a name (e.g. `snakemake-admin`).
2. On the permissions page choose **Attach policies directly** and tick **AdministratorAccess**.
   (Setup creates machines, roles, a queue and a bucket, which needs broad permissions. Don't
   use your account's root login for this.)
3. Open the new user > **Security credentials** > **Create access key** > **Command Line
   Interface (CLI)**. Copy the access key ID and secret access key.

Then in your terminal:

```bash
aws configure
```

Paste the two keys when asked. For the region, pick the one closest to you, e.g. `eu-west-1`
(Ireland) or `us-east-1` (N. Virginia), and use the same one everywhere below. Leave the output
format empty.

Check it worked: `aws sts get-caller-identity` should print your account number.

### 4. Find your subnets

AWS machines start inside a "subnet", a slice of your account's network. New accounts come
with default subnets that have internet access, which is what's needed here (to download
container images). List them:

```bash
aws ec2 describe-subnets --filters Name=default-for-az,Values=true --query 'Subnets[].SubnetId' --output text
```

You'll get two or more IDs like `subnet-0a1b2c3d`. Note them down. If the list is empty, your
account has no default network; create one with `aws ec2 create-default-vpc` and run the
command again.

### 5. Build the machine image

This starts a temporary machine from Amazon's standard image for Batch, installs Snakemake on
it, saves the result as your own image, and removes the temporary machine. First look up
Amazon's current standard image for your region:

```bash
aws ssm get-parameter --name /aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id --query Parameter.Value --output text
```

Then build (use that image ID, one of your subnets and your region):

```bash
bash scripts/bake-ami.sh --base-ami ami-XXXXXXXX --subnet subnet-XXXXXXXX --region eu-west-1
```

It takes 10 to 20 minutes and ends with:

```
DONE. New standalone AMI: ami-0123456789abcdef0 (snakemake-host-env-...)
Put this in terraform.tfvars:   ami_id = "ami-0123456789abcdef0"
```

Note that AMI ID.

### 6. Create the AWS resources

[Terraform](https://www.terraform.io) creates everything else from the files in `terraform/`:
the queue jobs are sent to, the "compute environment" that starts and stops machines for that
queue, the permissions (IAM roles) the machines and jobs need, and an S3 bucket for your files.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Open `terraform.tfvars` in a text editor and fill in the region, your subnet IDs from step 4
and the AMI ID from step 5. Then:

```bash
terraform init
terraform plan
```

`plan` only shows what would be created. Read the last line: it should say something like
`Plan: 12 to add, 0 to change, 0 to destroy`. If it says anything will be changed or destroyed,
stop and check you're in the right folder and account. Otherwise create it all (type `yes` when
asked):

```bash
terraform apply
cd ..
```

The machines aren't started yet. The compute environment starts them when jobs arrive and
stops them when the queue is empty, so nothing is running (or costing money) at this point.

### 7. Make a profile

A Snakemake "profile" is a settings file that tells Snakemake to use this plugin, your queue and
your bucket. Terraform prints one for you. For the test workflow:

```bash
cd examples/smoke-test
mkdir -p profile
terraform -chdir=../../terraform output -raw snakemake_profile > profile/config.yaml
printf 'default-resources: [mem_mb=512]\njobs: 5\n' >> profile/config.yaml
cat profile/config.yaml
```

You should see lines like `executor: aws-batch-flex` and `aws-batch-flex-queue: smkflex-queue`.

### 8. Run the test workflow

```bash
snakemake --profile profile
```

What happens: Snakemake uploads the workflow to S3 and submits two jobs. The first time, AWS
has to start a machine, so the jobs show as waiting for a few minutes. Then each job runs in
its own container (`debian:12`, which has no Python) and writes its result to S3. When it's
finished you'll see `3 of 3 steps (100%) done`.

Look at the results (the bucket name is printed by `terraform -chdir=../../terraform output -raw s3_bucket`):

```bash
aws s3 cp s3://YOUR-BUCKET/snakemake/results/hello.txt -
aws s3 cp s3://YOUR-BUCKET/snakemake/results/sum.txt -
```

The first says hello from Debian and `image has NO python3`; the second is `45`. A few minutes
later the machine shuts itself down.

### 9. Your own workflow

A Snakemake workflow is a file called `Snakefile` made of rules. Each rule says what files it
makes (`output`), what it needs (`input`), which container to use, and the command to run:

```python
rule count_lines:
    input: "data/reads.txt"
    output: "results/count.txt"
    container: "docker://debian:12"
    resources: mem_mb=1000
    shell: "wc -l < {input} > {output}"
```

Snakemake works out the order from the file names, and with this plugin each rule runs as a
Batch job. File paths are inside your bucket's `snakemake/` folder, so upload input files
there first:

```bash
aws s3 cp data/reads.txt s3://YOUR-BUCKET/snakemake/data/reads.txt
```

Put the `Snakefile` in a new folder, make a `profile/config.yaml` there as in step 7, and run
`snakemake --profile profile`. Results appear under `s3://YOUR-BUCKET/snakemake/results/`.
The [Snakemake tutorial](https://snakemake.readthedocs.io/en/stable/tutorial/tutorial.html)
explains rules, wildcards and the rest; everything in it works here, with the differences listed
under [Limits](#limits).

`examples/conformance/` has one small workflow per feature (scripts, conda, checkpoints,
Fargate, ...) if you want to see more.

### 10. Costs and removing everything

While idle, the only costs are the files in your bucket and the stored machine image (both
cents per month for small amounts). Machines only run while jobs do. To check nothing is
running, the AWS console's **EC2** > **Instances** page should show no running
`snakemake`-related instances a few minutes after a workflow ends.

To remove everything:

```bash
aws s3 rm s3://YOUR-BUCKET --recursive          # empty the bucket (deletes your results!)
cd terraform && terraform destroy && cd ..       # remove queue, roles, bucket, ...
rm terraform/terraform.tfvars
bash scripts/cleanup-ami.sh --region eu-west-1 ami-XXXXXXXX   # remove the machine image
```

`terraform destroy` refuses to delete a bucket that still has files in it, hence the first
line. `cleanup-ami.sh` refuses to delete the AMI that `terraform.tfvars` still uses, hence the
`rm`; it also only deletes images that `bake-ami.sh` made, and asks before deleting.

## Troubleshooting

- **Jobs stay "RUNNABLE" for more than ~10 minutes.** No machine could start. Check the
  compute environment in the AWS console (**Batch** > **Compute environments**) for an error,
  and that your subnets have internet access. New accounts sometimes have a low limit on
  machines; Snakemake also prints a warning when a job waits too long.
- **A job fails.** Snakemake prints the end of the job's log. Full logs are in the AWS console
  under **CloudWatch** > **Log groups** > `/aws/batch/job`.
- **"Startup check failed".** The plugin checks the queue before submitting anything; the
  message says what's wrong (usually a typo in the queue name, or the wrong region).
- **"IncompleteFilesException" after Snakemake was stopped.** Run again with
  `--rerun-incomplete`. Jobs left running by the stopped run are cancelled automatically.

## Reference

### How rules map to Batch

- `container:` is the image (`docker://` is optional). One job definition is registered per
  image and platform and reused across runs.
- `threads` -> vCPUs, `mem_mb`/`mem_mib` -> memory, `gpu` -> GPUs, `runtime` (minutes) -> timeout.
- Memory is a hard limit and the Snakemake process in the job uses some of it, so 256 MiB is
  added to explicit memory requests (`--aws-batch-flex-mem-overhead-mib`, 0 to turn off).
  Rules with no memory request get 2048 MiB.
- `resources: batch_queue="other-queue"` sends one rule to another queue. EC2 and Fargate
  queues can be mixed in a run; the platform is read from each queue's compute environment (or
  set per rule with `batch_platform`). Fargate jobs need `--aws-batch-flex-execution-role-arn`
  and get no host mounts.
- Every job is tagged with `snakemake-run-id`, `snakemake-workflow`, `snakemake-rule` and
  `snakemake-job-id`. Add your own with `aws-batch-flex-tags: [project=x]` or per rule with
  `resources: batch_tags="stage=align"`. Tags propagate to the ECS tasks.
- All settings: `snakemake --help | grep aws-batch-flex`.

### Interrupted runs

If `snakemake` is killed or loses its connection, its Batch jobs keep running, and a restart
would submit them again. Jobs are tagged with `snakemake-workflow` (a hash of the working
directory, Snakefile and storage prefix), so on restart the plugin finds jobs from the earlier
run that are still active and cancels them before submitting anything. Use
`--aws-batch-flex-orphan-jobs warn` to only list them (the default with `--nolock`), or
`ignore` to skip the check.

### Private images

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

### Conda and wrappers

Run with `--sdm conda`. Jobs need `conda`: either the image has it, or bake Miniforge into
the AMI (`bake-ami.sh --conda-env env.yaml`) and set `aws-batch-flex-conda-path: /opt/miniforge3`.
Only `condabin` is added to PATH, so an image's own conda still wins. Envs baked with
`--conda-env` can be used by name (`conda: "rnaseq"`); YAML files are built inside the job.
The machine you run `snakemake` on needs `conda` on its PATH as well.

Wrappers work the same way. The plugin passes jobs an HTTP `--wrapper-prefix`, so images don't
need git.

`scripts/update-ami.sh --conda-env new.yaml` bakes a new AMI on top of the current one; a
`terraform apply` then replaces the compute environment but keeps the queue.

### Without the machine image

To try the plugin on an existing Batch queue without building an AMI, set
`aws-batch-flex-bootstrap: auto` in the profile instead of `host`. Snakemake is then installed
into each container when its job starts (about 10-15 s per job), which needs internet access
and `curl`, `wget` or `pip` in the image (`--aws-batch-flex-install-prereqs` installs `curl`
first if missing).

### What's been tested

Run on real AWS (eu-west-1):
`shell:`, `run:`, Python/bash/R `script:` (Python scripts in the image's own Python),
`wrapper:`, conda (YAML in a conda image, YAML and named envs in conda-less images), a
different image per rule, a 6-image variant-calling pipeline, EC2 and Fargate (separately and
mixed in one run), checkpoints, groups, shadow, benchmark/log, retries, `temp()`,
`directory()`, resources and tags, OOM and timeout reporting, cancelling with Ctrl-C, startup
checks, fan-out to 100 jobs, install-at-start on a bare `debian:12`, cancelling jobs left over
from an interrupted run.

Only tested against a fake Batch: private registry credentials, ECR pulls,
`--aws-batch-flex-volumes`, `update-ami.sh`, a single Ctrl-C.
Not tested yet: `notebook:`, Julia, GPU, Spot, the `terraform/launcher` module.

### Limits

- Host mode needs a glibc image with `/bin/sh` and bash. Alpine/musl images exit with 127 and
  say why. Build the AMI on the same CPU architecture as your instances.
- Python `script:` rules use the image's `python` if it's 3.10 or newer, so they see the
  image's packages. Otherwise (no Python, or an older one) and for `run:` blocks, the code runs
  in Snakemake's own Python from the host env; add libraries there with `--extra-package` when
  building it, or call the script from `shell:`. `--aws-batch-flex-script-python host` (or
  `resources: batch_script_python="host"` on one rule) uses Snakemake's Python instead.
- `pipe()` doesn't work. `--sdm apptainer` isn't supported, and Singularity images (`.sif`)
  can't be used.
- Images with an ENTRYPOINT that doesn't pass the command through will break.
- All rules in a group must use the same image.
- Install-at-start fails on very old images (e.g. glibc 2.23).

### IAM

The identity running `snakemake` needs `batch:SubmitJob`, `DescribeJobs`, `TerminateJob`,
`RegisterJobDefinition`, `DescribeJobDefinitions`, `DescribeJobQueues`, `ListJobs`,
`DescribeComputeEnvironments` and `TagResource`, plus `logs:GetLogEvents`, `iam:PassRole` on
the job role(s), and read/write on the S3 prefix. `terraform output launcher_policy_json`
prints a policy with exactly this. The job role needs read/write on the prefix.

## License

MIT, see [LICENSE](LICENSE).
