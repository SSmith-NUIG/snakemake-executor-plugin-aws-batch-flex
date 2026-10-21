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

While idle, the only costs are the files in your bucket, the job logs and the stored machine
image (all cents per month for small amounts). Job logs are deleted after 30 days
(`log_retention_days` in `terraform.tfvars`). Files in the bucket stay until you delete them;
set `s3_expire_days` to have files under the prefix deleted automatically that many days after
they were written (only do this if you copy results you want to keep elsewhere). Uploads that
were interrupted, e.g. by a killed job, are cleared after 7 days. Machines only run while jobs do. To check nothing is
running, the AWS console's **EC2** > **Instances** page should show no running
`snakemake`-related instances a few minutes after a workflow ends.

To remove everything:

```bash
aws s3 rm s3://YOUR-BUCKET --recursive          # empty the bucket (deletes your results!)
cd terraform && terraform destroy && cd ..       # remove queue, roles, bucket, ...
bash scripts/cleanup-ami.sh --region eu-west-1 --no-tfvars ami-XXXXXXXX   # remove the machine image
bash scripts/cleanup-jobdefs.sh --region eu-west-1                       # remove the plugin's job definitions
```

`cleanup-jobdefs.sh` is also handy on its own now and then: job definitions for images you no longer
use stay in AWS (they cost nothing, but pile up). It only removes the plugin's own definitions,
never one an active job uses, and the plugin simply registers them again if a later run needs one.
Add `--dry-run` to see the list first.

`terraform destroy` refuses to delete a bucket that still has files in it, hence the first
line. `cleanup-ami.sh` only deletes images that `bake-ami.sh` made, asks before deleting, and
refuses any image that's still in use by a compute environment, launch template or machine.
Normally it also refuses the image named in `terraform.tfvars`; `--no-tfvars` skips that one
check, since after `terraform destroy` nothing uses it any more.

## Troubleshooting

- **Jobs stay "RUNNABLE" for more than ~10 minutes.** No machine could start. Check the
  compute environment in the AWS console (**Batch** > **Compute environments**) for an error,
  and that your subnets have internet access. New accounts sometimes have a low limit on
  machines; Snakemake also prints a warning when a job waits too long.
- **A job fails.** Snakemake prints the end of the job's log. Full logs are in the AWS console
  under **CloudWatch** > **Log groups** > `/aws/batch/smkflex` (`/aws/batch/` plus the `name` from
  `terraform.tfvars`; `/aws/batch/job` without the Terraform setup).
- **"Startup check failed".** The plugin checks the queue before submitting anything; the
  message says what's wrong (usually a typo in the queue name, or the wrong region).
- **"IncompleteFilesException" after Snakemake was stopped.** Run again with
  `--rerun-incomplete`. Jobs left running by the stopped run are cancelled automatically.

## Reference

### Rule options

Everything about a job is set on its rule. An example with most of the options:

```python
rule align:
    input: "data/{sample}.fq"
    output: "results/{sample}.sam"
    container: "docker://quay.io/biocontainers/bwa:0.7.17--h7132678_9"
    threads: 8
    resources:
        mem_mb=16000,
        runtime=120,
        batch_queue="smkflex-big-queue",
        batch_tags="stage=align,project=rnaseq",
    shell: "bwa mem -t {threads} ref.fa {input} > {output}"
```

| On the rule | What it does on AWS |
|---|---|
| `container: "docker://..."` | The image the job runs in. Any registry works (Docker Hub, quay.io, ghcr.io, ECR...); `docker://` is optional. Rules without one use `aws-batch-flex-default-image`. |
| `threads: 8` | Number of vCPUs requested. |
| `mem_mb=16000` (or `mem_mib`) | Memory requested. It's a hard limit, and Snakemake itself runs inside the job too, so 256 MiB is added (see `mem-overhead-mib`). No request: 2048 MiB. |
| `runtime=120` | Minutes before Batch stops the job (timeout). To give every rule a timeout, put `runtime` in the profile's `default-resources`. |
| `batch_shm_mb=4096` | Size of `/dev/shm` (Docker's default is 64 MB), for tools that use shared memory, e.g. bwa-mem2 or PyTorch data loaders. What's stored there counts towards `mem_mb`. EC2 only. `shared_memory_size_mb` also works. |
| `disk_mb=100000` | Fargate: scratch disk for the job (default 20 GiB, up to 200), rounded up to 25 GiB steps. Snakemake sets it to twice the input size by default, which is used too. EC2: ignored; see [Limits](#limits). |
| `gpu=1` | Number of GPUs (needs a queue with GPU instances; untested). |
| `batch_queue="other-queue"` | Send this rule to a different queue than the profile's. EC2 and Fargate queues can be mixed in one workflow; the plugin works out which is which. |
| `batch_platform="fargate"` | Force the platform instead of detecting it from the queue (only needed if the plugin isn't allowed to look the queue up). |
| `batch_tags="k=v,k2=v2"` | Extra tags on this rule's jobs, e.g. for cost reports. Overrides profile tags with the same key. |
| `batch_spot_retries=0` | How often Batch retries this rule's job if its Spot instance is taken away (0-9; default from the profile, 2). `aws_batch_spot_attempts` (retries + 1) also works. |
| `batch_priority=500` | Scheduling priority, 0-9999, higher first. Only on queues with a fair-share policy. `aws_batch_scheduling_priority` also works. |
| `batch_job_definition="name"` | Use an existing job definition for this rule (see the `job-definition` setting below). |
| `batch_script_python="host"` | For Python `script:` rules: use Snakemake's own Python instead of the image's. |

Resources can also be set from the profile, without editing the Snakefile, using Snakemake's
`set-resources`:

```yaml
set-resources:
  align:
    batch_queue: smkflex-big-queue
    mem_mb: 32000
```

Snakemake features that work as usual (each has a small example in `examples/conformance/`):

- `shell:` commands, and `script:` files in Python, R or bash. Python scripts run with the
  image's own Python (3.10 or newer) and can use its packages; otherwise with Snakemake's.
- `run:` blocks (these always use Snakemake's own Python, not the image's).
- `conda:` environments and `wrapper:`s, with `--sdm conda` (see below).
- `retries: 2`, and resources that grow with each attempt, e.g.
  `mem_mb=lambda wildcards, attempt: 4000 * attempt`.
- `group:` (grouped rules run as one Batch job, so they must share an image), `checkpoint`,
  `temp()`, `directory()`, `shadow:`, `log:` and `benchmark:`.

### Profile settings

Settings go in `profile/config.yaml` (as `aws-batch-flex-queue: my-queue`) or on the command
line (as `--aws-batch-flex-queue my-queue`). Only the queue is required; Terraform's
`snakemake_profile` output fills in the usual ones.

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `queue` | (required) | The Batch queue jobs go to, unless a rule sets `batch_queue`. |
| `region` | your AWS config | AWS region. |
| `job-role-arn` | instance role | Role the jobs run as; needs read/write on the S3 bucket. |
| `execution-role-arn` | none | Needed for Fargate queues and for private registries. |
| `platform` | detected | `ec2` or `fargate` for the main queue. Normally read from the queue. |
| `default-image` | none | Image for rules without `container:`. |
| `bootstrap` | `auto` | How jobs get Snakemake: `host` (from the AMI, recommended on EC2), `auto`/`always` (install when the job starts), `never` (the image has it). |
| `host-env-path` | `/opt/snakemake-env` | Where the AMI has Snakemake (`host` mode). |
| `host-env-wait-seconds` | 120 | How long a job waits for that env if the instance is still installing it. 0 on a baked AMI. |
| `install-prereqs` | off | Install-at-start only: install `curl` first in images that have no way to download. |
| `script-python` | `image` | `image`: Python `script:` rules use the image's Python if it's 3.10+. `host`: always Snakemake's. |
| `mem-overhead-mib` | 256 | Memory added to each rule's request for the Snakemake process in the job. |
| `conda-path` | none | Where the AMI has Miniforge, for `conda:` rules in images without conda (EC2). |
| `volumes` | none | Extra folders from the instance to mount into jobs, `HOST:CONTAINER[:ro]` (EC2). |
| `extra-packages` | none | Extra pip packages for install-at-start (e.g. other Snakemake plugins). |
| `python-version` | yours | Python version for install-at-start. |
| `spot-retries` | 2 | How often Batch retries a job whose Spot instance was taken away (0 = never). |
| `scheduling-priority` | none | Priority for every job (0-9999), on queues with a fair-share policy; others ignore it with a warning. |
| `registry-credentials` | none | `REGISTRY=SECRET_ARN` for private registries outside AWS (see below). |
| `orphan-jobs` | `cancel` | Jobs left running by an interrupted run: `cancel`, `warn` or `ignore` (see below). |
| `tags` | none | `KEY=VALUE` tags on every job, e.g. `[project=rnaseq, team=genomics]`. |
| `no-auto-tags` | off | Turn off the automatic tags (and the leftover-job check, which uses them). |
| `runnable-warn-minutes` | 15 | Warn when a job has waited this long for a machine. |
| `skip-preflight` | off | Skip the startup checks of the queue and roles. |
| `log-group` | `/aws/batch/job` | CloudWatch log group for job logs. Terraform creates one that deletes logs after 30 days and puts it in the profile; Batch's default group keeps them forever. The group must already exist. |
| `job-definition` | none | Use an existing job definition (name, `name:revision` or ARN) instead of letting the plugin register its own, e.g. where you aren't allowed to register them. It decides the image, job role, mounts and `/dev/shm`; vCPUs, memory, timeout and tags still come from the rules. If it doesn't mount the AMI's Snakemake, jobs install Snakemake at start. |
| `job-definition-prefix` | `snakemake` | Name prefix for the job definitions the plugin registers. |
| `assign-public-ip` | `ENABLED` | Fargate: give jobs a public IP (needed for internet without a NAT gateway). |
| `ephemeral-storage-gib` | Fargate's 20 | Fargate: minimum scratch disk size for every job, 21-200 GiB (a rule's `disk_mb` can raise it). |

Jobs are tagged automatically with `snakemake-run-id`, `snakemake-workflow`, `snakemake-rule`
and `snakemake-job-id`, and tags propagate to the ECS tasks.

### Interrupted runs

If `snakemake` is killed or loses its connection, its Batch jobs keep running, and a restart
would submit them again. Jobs are tagged with `snakemake-workflow` (a hash of the working
directory, Snakefile and storage prefix), so on restart the plugin finds jobs from the earlier
run that are still active and cancels them before submitting anything. Use
`--aws-batch-flex-orphan-jobs warn` to only list them (the default with `--nolock`), or
`ignore` to skip the check.

### Long runs: `smkflex`

`snakemake` has to keep running until the workflow is finished, so a laptop that sleeps or
loses its connection stops the run (the jobs already on Batch carry on, and are cancelled when
you restart). For long workflows, `smkflex` runs Snakemake itself on Batch instead. It's
optional: running `snakemake` from your own machine works as before, and both can be used on
the same workflow.

```bash
cd my-workflow
smkflex run                      # uses ./profile; returns once the run is submitted
smkflex logs JOB_ID --follow     # Snakemake's output, live
smkflex status                   # recent runs
smkflex cancel JOB_ID            # stops the run; Snakemake cancels its jobs
```

`smkflex run` packs the workflow folder (the files git would track, or everything except
`.snakemake/` outside a git checkout) together with the profile, uploads it next to your
workflow's files in the bucket, and submits one "head job" that runs `snakemake` there. Your
machine can be switched off once it returns. Anything after the options goes to Snakemake,
e.g. `smkflex run -- --forceall all`; `--wait` stays attached and prints the log until the run
ends.

- The head job uses 1 vCPU and 2 GiB (`--vcpus`, `--mem-mb`) for as long as the workflow
  runs, on the profile's queue (`--queue` for another one). Don't use a Spot queue for it.
- It needs remote storage (`default-storage-prefix`) and the profile setting
  `aws-batch-flex-head-role-arn`; the profile Terraform prints has both.
- Snakemake's `.snakemake` records (what was made with which code, unfinished jobs, logs) are
  saved to the bucket at the end of every run and restored at the start of the next, so reruns
  behave as they do on one machine. Runs with the same name (`--name`, default: the folder's
  name) share them, and a second run of a workflow that's still running is refused.
- The head job runs the plugin version installed where you run `smkflex`, and the AMI's
  Snakemake (in `host` mode) or one installed at start.

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

`scripts/update-ami.sh --conda-env new.yaml` bakes a new AMI on top of the current one and
points `terraform.tfvars` at it; `terraform apply` then updates the compute environment's image
in place (new instances start from the new AMI) and leaves the queue alone. Apply it while no
jobs are running.

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
from an interrupted run, `/dev/shm` size per rule, Fargate disk size from `disk_mb`, updating
the AMI with `update-ami.sh`, using an existing job definition, a private ECR image, job logs in
Terraform's own log group (EC2 and Fargate), a whole workflow run as a `smkflex` head job, and cancelling one.

Only tested against a fake Batch: private registry credentials (outside AWS),
`--aws-batch-flex-volumes`, a single Ctrl-C.
Not tested yet: `notebook:`, Julia, GPU, Spot.

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
- On EC2, all jobs on an instance share its disk (`root_volume_gib` in Terraform, 100 GiB in
  the example), and `disk_mb` is not enforced: Batch only places jobs by CPU and memory. Several
  jobs with large files on one instance can fill it, and the jobs then fail with "No space left
  on device" (the failure message says so). Use a bigger volume, or a second queue with bigger
  disks for the heavy rules (`batch_queue`).
- Install-at-start fails on very old images (e.g. glibc 2.23).
- Your Snakemake and the AMI's don't have to be the same version: 9.5 up to 9.27 mix fine in
  both directions (tested against the fake Batch; 9.0.x doesn't work). But every storage plugin
  installed on your machine must also be in the AMI, because Snakemake passes all of their
  options to the jobs; a job that fails this way says which plugin it is.

### IAM

The identity running `snakemake` needs `batch:SubmitJob`, `DescribeJobs`, `TerminateJob`,
`RegisterJobDefinition`, `DescribeJobDefinitions`, `DescribeJobQueues`, `ListJobs`,
`DescribeComputeEnvironments` and `TagResource`, plus `logs:GetLogEvents`, `iam:PassRole` on
the job role(s), and read/write on the S3 prefix. Optionally `ec2:DescribeImages` and
`ec2:DescribeLaunchTemplateVersions`, so the plugin can warn when the AMI's Snakemake version is
far from yours. `terraform output launcher_policy_json` prints a policy with all of this. The job role needs read/write on the prefix.
For `smkflex`, the head job's role needs the same as the identity running `snakemake`, and
that identity also needs `iam:PassRole` on the head role (both are in the Terraform setup).

## License

MIT, see [LICENSE](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/blob/main/LICENSE).
