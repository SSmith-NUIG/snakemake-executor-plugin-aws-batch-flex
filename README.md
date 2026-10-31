# snakemake-executor-plugin-aws-batch-flex

A Snakemake executor plugin that runs each job on [AWS Batch](https://aws.amazon.com/batch/) in
the image named by its rule's `container:` directive: a stock image from Docker Hub,
quay.io/biocontainers, your own ECR repository, and so on. The images don't need Snakemake or
Python inside them.

**How it works.** On EC2, a Snakemake environment is baked into the machine image (AMI) that
Batch starts instances from, and mounted read-only into every job's container; this is how
Nextflow brings its AWS CLI to jobs. On Fargate, which has no instances to customise, Snakemake
is installed into the container when the job starts. Inputs and outputs go through S3 with
Snakemake's S3 storage plugin. Instances start when jobs arrive and stop when the queue is
empty, so you pay only while jobs run.

**How it differs from the aws-batch plugin.** The other AWS Batch executor in the plugin
catalog,
[snakemake-executor-plugin-aws-batch](https://github.com/snakemake/snakemake-executor-plugin-aws-batch),
runs `snakemake` inside each job's container, so every image a rule uses needs Snakemake and the
S3 storage plugin installed; biocontainers and other stock images can't be used as they are.
This plugin brings Snakemake to the job instead, so each rule can use any image, and conda envs
work in images without conda. It also:

- sets up the AWS side (AMI, queue, roles, bucket, profile) with one command;
- mixes EC2, Spot and Fargate queues in one run;
- can run Snakemake itself on AWS (`smkflex run`), so your machine can be switched off;
- retries jobs that ran out of memory with more memory;
- reports what each rule used at the end of a run, with `set-resources:` and `set-threads:`
  suggestions to paste into your profile.

This page goes from an empty AWS account to a finished test run
([Getting started](#getting-started)), then covers running your own workflows and the per-rule
resources. The rest is in three more pages:

- [Guide](docs/guide.md): published pipelines, moving from a cluster, long runs with `smkflex`,
  conda, the usage table, Spot and Fargate, private images, and troubleshooting;
- [Reference](docs/reference.md): every profile setting and `smkflex` command, IAM, what has been
  tested, and the limits;
- [Setup and upkeep](docs/setup.md): university and company accounts, updating the machine image,
  setting up by hand, and removing everything.

## Getting started

These steps take under an hour. They assume you know Snakemake but not AWS.

The plugin needs Snakemake 9.5 or newer and Python 3.11 to 3.13. It has been tested on Linux.
On Windows, use [WSL](https://learn.microsoft.com/windows/wsl/install) and run everything inside
it. macOS should work but hasn't been tried.

### 1. What you need

- An AWS account. Setting this up costs very little: building the machine image runs one small
  machine for about 10 minutes, and keeping the image costs well under $1 a month. The test run
  in step 5 costs a few cents. If your account comes from your university or company, read
  [University and company accounts](docs/setup.md#university-and-company-accounts) first.
- Somewhere to run commands: a laptop is fine. As with any remote executor, the machine running
  `snakemake` has to stay on and online until the workflow finishes; `smkflex run` runs
  Snakemake on AWS instead (see [Long runs](docs/guide.md#long-runs-smkflex)).

### 2. Install the tools

Install [Miniforge](https://github.com/conda-forge/miniforge#install) if you don't already have
`conda`. Then get this repository and create an environment with everything you need (Python,
Snakemake, Terraform, the AWS command-line tool and `snakedeploy`):

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

Check it worked: `aws sts get-caller-identity` should print your account number. (If you log in
through a university or company page instead, follow
[University and company accounts](docs/setup.md#university-and-company-accounts) for this step.)

### 4. Set up AWS

One script does the rest of the setup. It finds your account's default subnets, builds an AMI
with Snakemake on it (plus conda and pandas, which most published pipelines need), creates the
queue, compute environment, roles, log group and bucket with Terraform, and installs a global
Snakemake profile, `~/.config/snakemake/smkflex/config.yaml`, that sets the executor, your queue
and the S3 storage prefix. `snakemake --profile smkflex` then works in any folder. Use the
region you picked in step 3:

```bash
bash scripts/setup.sh --region eu-west-1
```

It asks twice before creating anything: before building the AMI, which takes 10 to 20 minutes,
and after showing the Terraform plan (about 20 new resources, nothing changed or removed). At
the end it checks the whole setup and prints `All good.`. If it stops partway, fix what it
reports and run the same command again; it continues where it stopped.

Options: `--fargate` also creates a Fargate queue and `--spot` a Spot queue (see
[Spot and Fargate queues](docs/guide.md#spot-and-fargate-queues)), `--extra-package NAME` adds a
Python package to the AMI, and `--no-conda` leaves conda out. `bash scripts/setup.sh --help`
lists the rest. To do each step by hand instead, see
[Setting up by hand](docs/setup.md#setting-up-by-hand).

Nothing is running, or costing money, yet: instances start when jobs arrive and stop when the
queue is empty.

### 5. Run the test workflow

```bash
cd examples/smoke-test
snakemake --profile smkflex
```

Snakemake submits two jobs. On the first run Batch has to start an instance, so the jobs wait
in `RUNNABLE` for a few minutes. Each job then runs in `debian:12`, which has no Python, and
writes its output to S3. The run ends with `3 of 3 steps (100%) done` and a table of what it
used.

Download the results from the bucket:

```bash
smkflex pull results --profile smkflex
cat results/hello.txt results/sum.txt
```

The first says hello from Debian and `image has NO python3`; the second is `45`. A few minutes
later the instance shuts down.

### 6. Set a budget alert

Before running real pipelines, set a budget alert, so that a mistake (a huge job, a forgotten
setting) costs an email rather than a surprise bill. In the AWS console, go to **Billing and
Cost Management** > **Budgets** > **Create budget** > **Use a template** > **Monthly cost
budget**, and enter an amount (e.g. $50) and your email address. AWS then emails you at 85% and
100% of it, and when the month's forecast goes over. It costs nothing.

As a rough guide, compute costs about $0.05 per vCPU-hour on on-demand instances (about a third
of that on Spot), so a job using 4 vCPUs for an hour costs about $0.20. The published RNA-seq
pipeline in [Running a published pipeline](docs/guide.md#running-a-published-pipeline) ran its
53 test jobs for about $0.10; a real run with 20 samples of 30 million reads should cost a few
dollars (an estimate, not measured). S3 storage is about $0.02 per GB per month. The table at
the end of every run shows what that run used (see
[What a run used](docs/guide.md#what-a-run-used)).

That completes the setup. Next, depending on what you're running:

- a workflow of your own: [Your own workflow](#your-own-workflow);
- a pipeline from the Snakemake workflow catalog: [Running a published pipeline](docs/guide.md#running-a-published-pipeline);
- a pipeline you've been running on a cluster: read [Your own workflow](#your-own-workflow),
  then [Coming from a cluster](docs/guide.md#coming-from-a-cluster).

## Running your workflows

### Your own workflow

Each rule runs as one Batch job, in the image its `container:` directive names:

```python
rule count_lines:
    input: "data/reads.txt"
    output: "results/count.txt"
    container: "docker://debian:12"
    resources: mem_mb=1000
    shell: "wc -l < {input} > {output}"
```

The workflow runs with S3 as its default storage provider, so jobs read their inputs from the
bucket, never from your machine, and write their outputs there. In a folder with your
`Snakefile`:

```bash
smkflex init                     # gives this project its own prefix in the bucket
smkflex push data                # uploads data/reads.txt there
snakemake --profile smkflex      # runs the workflow
smkflex pull results             # downloads results/
```

`smkflex init` writes a project profile, `profiles/default/config.yaml`, whose
`default-storage-prefix` is the project's own prefix (`s3://YOUR-BUCKET/snakemake/<folder name>`,
so each project's `results/` stay apart), and adds `data/`, `results/` and the like to the
folder's `.gitignore`. Snakemake loads that profile automatically on top of `--profile smkflex`,
so it's the place for this workflow's settings (`default-resources`, `set-resources`,
`software-deployment-method`, `retries`; see [Tuning jobs](docs/guide.md#tuning-jobs)), while
the global `smkflex` profile holds what every workflow shares (queue, bucket, roles).

Push whatever files and folders your rules and sample sheets name (`smkflex push reads
samples.tsv`), and pull whichever outputs you want (`smkflex pull results/qc`). Both skip files
that are already up to date (`--dry-run` lists what would be transferred) and also take absolute
paths (see [Coming from a cluster](docs/guide.md#coming-from-a-cluster)). Paths that leave the
project folder (`../raw`) can't be mapped to storage; use an absolute path for those. If
something you push isn't git-ignored, `push` warns you, since `smkflex run` would otherwise send
it along with the workflow.

`export SNAKEMAKE_PROFILE=smkflex` saves typing `--profile` every time. Everything in the
[Snakemake docs](https://snakemake.readthedocs.io) applies, with the exceptions listed under
[Limits](docs/reference.md#limits). `examples/conformance/` has one small workflow per feature
(scripts, conda, checkpoints, Fargate, ...).

### Resources per rule

Each job's size, queue and other Batch settings come from its rule. This example sets every
option in the table below; a real rule only needs the few it uses:

```python
rule align:
    input: "data/{sample}.fq"
    output: "results/{sample}.sam"
    container: "docker://quay.io/biocontainers/bwa:0.7.17--h7132678_9"
    threads: 8
    retries: 4
    resources:
        mem_mb=16000,
        runtime=120,                          # minutes
        batch_shm_mb=4096,                    # EC2 only
        disk_mb=100000,                       # Fargate only
        gpu=1,                                # needs a queue with GPU instances
        batch_queue="smkflex-big-queue",
        batch_platform="ec2",                 # only if the queue can't be looked up
        batch_tags="stage=align,project=rnaseq",
        batch_spot_retries=3,                 # Spot queues
        batch_priority=500,                   # fair-share queues
        batch_script_python="host",           # script: rules only
        # batch_entrypoint='["python3"]',     # only if the image's registry can't be read
        # batch_job_definition="my-jobdef",   # instead of the plugin's own; it then sets the image
    shell: "bwa mem -t {threads} ref.fa {input} > {output}"
```

| On the rule | Value | Default | What it does on AWS |
|---|---|---|---|
| `container:` | image, e.g. `"docker://quay.io/biocontainers/bwa:0.7.17--h7132678_9"` | *the `default-image` setting* | The image the job runs in. Any registry works (Docker Hub, quay.io, ghcr.io, ECR, ...); `docker://` is optional. Pin a version tag rather than `latest`. Images with an `ENTRYPOINT` work too: Batch can't override it, so the plugin reads it from the registry and builds a command it will run. |
| `threads:` | whole number of vCPUs | `1` | vCPUs requested. On EC2, a job bigger than the largest instance type the queue allows waits forever (the plugin warns after `runnable-warn-minutes`). On Fargate, vCPUs and memory are rounded up to the nearest size Fargate offers (at most 16 vCPUs and 120 GiB). |
| `mem_mb` or `mem_mib` (or `mem`) | whole MB or MiB, e.g. `mem_mb=16000`; or `mem="16G"` | *`default-resources`, else 2048 MiB* | Memory requested, a hard limit. 256 MiB is added for the Snakemake process in the job (`mem-overhead-mib`). A job that exceeds it is killed; with `retries` set, it's retried with twice the memory, then four times (`oom-memory-factor`). The end-of-run table shows what each rule really used. |
| `retries:` (a rule directive) | whole number | *Snakemake's `retries`, else 0* | Snakemake's usual retries (or `retries: 2` in the profile for every rule). A job that ran out of memory gets more on each retry (`oom-memory-factor`), unless the rule scales memory itself with `attempt`, e.g. `mem_mb=lambda wildcards, attempt: 4000 * attempt`; then its own figure is used as is. Jobs that lost their Spot instance are restarted by Batch instead (`batch_spot_retries`) and don't use these up. |
| `runtime` | minutes, or a string such as `"2h"` or `"90m"` | *no timeout* | Batch stops the job after this long (at least 1 minute). To give every rule a timeout, put `runtime` in `default-resources`. |
| `batch_shm_mb` | MiB, more than 0 | *Docker's 64 MB* | EC2 only: the size of `/dev/shm`, for tools that use shared memory, e.g. bwa-mem2 or PyTorch data loaders. What's stored there counts towards `mem_mb`. `shared_memory_size_mb` (the aws-batch plugin's name) also works. |
| `disk_mb` or `disk_mib` (or `disk`) | whole MB or MiB, e.g. `disk_mb=100000`; or `disk="100G"` | *twice the job's input size (Snakemake's default)* | Fargate only: scratch disk for the job, rounded up to 25 GiB steps, at most 200 GiB; jobs get at least 20 GiB (or `ephemeral-storage-gib`). Ignored on EC2, where jobs share their instance's disk (see [Limits](docs/reference.md#limits)). |
| `gpu` | whole number of GPUs | *none* | GPUs requested. Needs a queue with GPU instances; not on Fargate. Untested. |
| `batch_queue` | queue name | *the `queue` setting* | Send this rule to another queue. EC2, Spot and Fargate queues can be mixed in one workflow; the plugin detects which is which. |
| `batch_platform` | `"ec2"` or `"fargate"` | *detected from the queue* | Force the platform; only needed if the plugin isn't allowed to describe the queue. |
| `batch_tags` | `"KEY=VALUE,KEY2=VALUE2"` | *none* | Extra tags on this rule's jobs, e.g. for cost reports. They override profile `tags` with the same key. |
| `batch_spot_retries` | whole number, `0` to `9` | *the `spot-retries` setting (2)* | How many times Batch restarts this rule's job if its Spot instance is reclaimed. `aws_batch_spot_attempts` (retries + 1, `1` to `10`; the aws-batch plugin's name) also works. |
| `batch_priority` | whole number, `0` to `9999` | *the `scheduling-priority` setting* | Scheduling priority, higher first. Only on queues with a fair-share policy. `aws_batch_scheduling_priority` also works. |
| `batch_job_definition` | job definition name, `name:revision` or ARN | *the `job-definition` setting* | Use an existing job definition for this rule; it decides the image (see the `job-definition` setting). |
| `batch_script_python` | `"image"` or `"host"` | *the `script-python` setting (`image`)* | For Python `script:` rules: `"host"` uses Snakemake's own Python instead of the image's. |
| `batch_entrypoint` | the image's `ENTRYPOINT` as a JSON list, e.g. `'["python3"]'`; `'[]'` for none | *read from the registry* | Only for when the plugin can't read the entrypoint from the registry (it then warns at the start): a private registry without `registry-credentials`, or no network access to it. |

To change resources without editing the Snakefile (for a published pipeline, say), use
`set-resources`, `set-threads` and `default-resources` in the project profile:

```yaml
default-resources: [mem_mb=4000]   # rules that set no memory
max-threads: 8                     # caps rules written for big cluster nodes
set-resources:
  align:
    batch_queue: smkflex-big-queue
    mem_mb: 32000
```

These work as usual, and each has a small example in `examples/conformance/`: `shell:`;
`script:` in Python, R or bash; `run:`; `conda:` and `wrapper:`; `retries` and resources that
scale with `attempt`; `group:` (a group runs as one Batch job, so its rules must share an
image); checkpoints, `temp()`, `directory()`, `shadow:`, `log:` and `benchmark:`.

### Most-used settings

Plugin settings go in a profile as `aws-batch-flex-NAME: value`, or on the command line as
`--aws-batch-flex-NAME value`; `setup.sh` writes the usual ones. The ones most often changed:

| Setting (`aws-batch-flex-...`) | Value | What it's for |
|---|---|---|
| `queue` | queue name | The Batch queue jobs go to. |
| `default-image` | image | The image for rules without `container:`. |
| `conda-cache` | `instance` (default), `none` or an `s3://` URL | Reuse built conda envs: shared per instance by default; an S3 URL also keeps a copy in S3 for later runs. |
| `oom-memory-factor` | number, `1` or more (default `2`) | How much more memory a job gets when it's retried after running out. |
| `spot-retries` | `0` to `9` (default `2`) | How many times Batch restarts a job whose Spot instance was reclaimed. |
| `tags` | list of `KEY=VALUE` | Extra tags on every job, e.g. to break costs down by project. |

All of them, with their defaults, are in the [Reference](docs/reference.md#profile-settings).

## AI assistance

This plugin was developed with an AI coding assistant (Claude Code, Anthropic), which wrote
code, tests and documentation under my direction. I designed the approach, reviewed the
changes, and ran the workflows listed under
[What's been tested](docs/reference.md#whats-been-tested) on real AWS Batch. I maintain the
plugin and am responsible for it; please report problems as
[issues](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/issues).

## License

MIT, see [LICENSE](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/blob/main/LICENSE).
