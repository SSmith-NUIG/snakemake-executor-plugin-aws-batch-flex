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

**How it differs from the aws-batch plugin.** The other AWS Batch executor in the Snakemake
plugin catalog,
[snakemake-executor-plugin-aws-batch](https://github.com/snakemake/snakemake-executor-plugin-aws-batch), runs
`snakemake` inside each job's container, so every image a rule uses needs Snakemake and the S3
storage plugin installed; the usual biocontainers and stock images can't be used as they are.
This plugin brings Snakemake to the job instead, so each rule can use any image, and conda envs
work in images without conda. It also sets up the AWS side in one command, mixes EC2, Spot and
Fargate queues in one run, can run Snakemake itself on AWS (`smkflex run`), retries jobs that
ran out of memory with more, and reports what each rule used, with the memory and threads to give it.

This page takes you from nothing to a finished test run ([Getting started](#getting-started)),
then shows how to run your own workflow and what you can set on each rule. The rest is in three
more pages:

- [Guide](docs/guide.md): published pipelines, moving from a cluster, long runs with `smkflex`,
  conda, the usage table, Spot and Fargate, private images, and troubleshooting;
- [Reference](docs/reference.md): every profile setting and `smkflex` command, IAM, what has been
  tested, and the limits;
- [Setup and upkeep](docs/setup.md): university and company accounts, updating the machine image,
  setting up by hand, and removing everything.

## Getting started

These steps take you from nothing to a finished test run, in under an hour. They assume you're
comfortable typing commands into a terminal, but not that you know AWS or Snakemake.

It has been tested on Linux. On Windows, use [WSL](https://learn.microsoft.com/windows/wsl/install)
and run everything inside it. macOS should work but hasn't been tried.

### 1. What you need

- An AWS account. Setting this up costs very little: building the machine image runs one small
  machine for about 10 minutes, and keeping the image costs well under $1 a month. The test run
  in step 5 costs a few cents. If your account comes from your university or company, read
  [University and company accounts](docs/setup.md#university-and-company-accounts) first.
- Somewhere to run commands: your laptop is fine. When you run `snakemake` it has to stay on
  (and online) until the workflow finishes, because it's what tells AWS what to do next;
  `smkflex run` hands that to AWS instead (see [Long runs](docs/guide.md#long-runs-smkflex)).

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

One script does the rest of the setup. It finds your account's network, builds a machine image
with Snakemake on it (plus conda and pandas, which most published pipelines need), creates the
queue, roles and bucket with Terraform, and installs a Snakemake profile called `smkflex` in
`~/.config/snakemake/smkflex/`. A profile is a settings file: it tells Snakemake to use this
plugin, your queue and your bucket, and because it's installed by name, `snakemake --profile
smkflex` works in any folder. Use the region you picked in step 3:

```bash
bash scripts/setup.sh --region eu-west-1
```

It asks twice before creating anything. The first time is before building the machine image,
which takes 10 to 20 minutes. The second is after showing what Terraform will create (about 20
new resources, nothing changed or removed). At the end it checks the whole setup and says
`All good.`. If it stops halfway, fix what it says and run the same command again: it carries on
where it stopped.

Options: `--fargate` also creates a Fargate queue and `--spot` a cheaper Spot queue (see
[Spot and Fargate queues](docs/guide.md#spot-and-fargate-queues)), `--extra-package NAME` adds a Python
package to the image, and `--no-conda` leaves conda out. `bash scripts/setup.sh --help` lists the
rest. To do each step by hand instead, see [Setting up by hand](docs/setup.md#setting-up-by-hand).

Nothing is running (or costing money) yet: machines start when jobs arrive and stop when the
queue is empty.

### 5. Run the test workflow

```bash
cd examples/smoke-test
snakemake --profile smkflex
```

What happens: Snakemake uploads the workflow to S3 and submits two jobs. The first time, AWS has
to start a machine, so the jobs show as waiting for a few minutes. Then each job runs in its own
container (`debian:12`, which has no Python) and writes its result to S3. When it's finished
you'll see `3 of 3 steps (100%) done`, followed by a table of what the run used.

Download the results from the bucket and look at them:

```bash
smkflex pull results --profile smkflex
cat results/hello.txt results/sum.txt
```

The first says hello from Debian and `image has NO python3`; the second is `45`. A few minutes
later the machine shuts itself down.

### 6. Set a budget alert

Before running real pipelines, set a budget alert, so a mistake (a huge job, or a forgotten
setting) costs an email rather than a surprise bill. In the AWS console, **Billing and Cost
Management** > **Budgets** > **Create budget** > **Use a template** > **Monthly cost budget**:
enter an amount (e.g. $50) and your email address. AWS then mails you at 85% and 100% of it, and
when the month's forecast goes over. It takes two minutes and costs nothing.

As a rough guide: compute costs about $0.05 per vCPU-hour on normal instances (a third of that
on Spot), so a job using 4 vCPUs for an hour costs about $0.20. The published RNA-seq pipeline
in [Running a published pipeline](docs/guide.md#running-a-published-pipeline) ran its 53 test jobs for about
$0.10; a real run with 20 samples of 30 million reads should be a few dollars (an estimate, not
measured). S3 storage is about $0.02 per GB per month. The table at the end of every run shows
what that run used (see [What a run used](docs/guide.md#what-a-run-used)).

That's the setup done. Next, depending on what you're running:

- a workflow of your own: [Your own workflow](#your-own-workflow);
- a pipeline from the Snakemake workflow catalog: [Running a published pipeline](docs/guide.md#running-a-published-pipeline);
- a pipeline you've been running on a cluster: read [Your own workflow](#your-own-workflow),
  then [Coming from a cluster](docs/guide.md#coming-from-a-cluster).

## Running your workflows

### Your own workflow

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
Batch job. The [Snakemake tutorial](https://snakemake.readthedocs.io/en/stable/tutorial/tutorial.html)
explains rules, wildcards and the rest; everything in it works here, with the differences listed
under [Limits](docs/reference.md#limits).

Each workflow lives in its own project folder, and its files live in your S3 bucket: jobs read
their inputs from there, never from your computer, so upload them first, and write their outputs
there. Put the `Snakefile` in a new folder and,
in that folder:

```bash
smkflex init                     # gives this project its own folder in the bucket
smkflex push data                # uploads data/reads.txt to it
snakemake --profile smkflex      # runs the workflow
smkflex pull results             # downloads results/ from it
```

`smkflex init` writes the project's own settings file, `profiles/default/config.yaml`, with the
project's folder in the bucket (`s3://YOUR-BUCKET/snakemake/<folder name>`, so each project's
`results/` stay apart), and adds `data/`, `results/` and the like to the folder's `.gitignore`.
Snakemake loads that file by itself, on top of the `smkflex` profile, so it's also the place for
anything this workflow needs (memory, conda, retries, see [Tuning jobs](docs/guide.md#tuning-jobs)); the
`smkflex` profile keeps what's the same for every workflow (your queue, bucket and roles).

`data` is only this example's folder: push whatever files and folders your rules and sample
sheets name (`smkflex push reads samples.tsv`), and pull whichever outputs you want
(`smkflex pull results/qc`). `push` and `pull` skip files that are already there with the same
size (`--dry-run` only lists them) and also take absolute paths (see
[Coming from a cluster](docs/guide.md#coming-from-a-cluster)); paths that go up out of the project folder
(`../raw`) can't be mapped to storage, so use an absolute path for those. If something you push
isn't in `.gitignore`, `push` says so, since `smkflex run` would otherwise send it along with
the workflow.
`export SNAKEMAKE_PROFILE=smkflex` saves typing `--profile` every time. `examples/conformance/`
has one small workflow per feature (scripts, conda, checkpoints, Fargate, ...) if you want to
see more.

### Resources per rule

Everything about a job is set on its rule. An example with every option in the table below (a real
rule only needs the few it uses):

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
        gpu=1,                                # needs a queue with GPU machines
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

| On the rule | What it does on AWS |
|---|---|
| `container: "docker://..."` | The image the job runs in. Any registry works (Docker Hub, quay.io, ghcr.io, ECR...); `docker://` is optional. Rules without one use `aws-batch-flex-default-image`. Images with an `ENTRYPOINT` work too: Batch can't change it, so the plugin reads it from the registry and gives the jobs a command it runs. |
| `threads: 8` | Number of vCPUs requested. On EC2, a job bigger than the largest machine the queue may start waits forever (the plugin warns after `runnable-warn-minutes`). On Fargate, vCPUs and memory are rounded up to the nearest size Fargate offers (at most 16 vCPUs and 120 GiB). |
| `mem_mb=16000` (or `mem_mib`) | Memory requested, a hard limit. Snakemake itself runs inside the job too, so 256 MiB is added (`mem-overhead-mib`). Without it (many published pipelines don't set it), `default-resources` applies, or else 2048 MiB. A job that goes over it is stopped; with `--retries 2` (or `retries: 2` in the profile) it's retried with twice the memory, then four times (`oom-memory-factor`). The end-of-run table shows what each rule really used. |
| `retries: 2` | Not a resource but a rule setting (or `retries: 2` in the profile for every rule): how many times Snakemake runs a failed job again. A job that ran out of memory gets more on each retry (`oom-memory-factor`), and `attempt` lets a rule raise anything itself, e.g. `mem_mb=lambda wildcards, attempt: 4000 * attempt` (then the rule's own figure is used, not doubled on top). Jobs that lost their Spot machine are restarted by Batch instead (`batch_spot_retries`) and don't use these up. |
| `runtime=120` | Minutes before Batch stops the job (timeout); `"2h"` also works. To give every rule a timeout, put `runtime` in `default-resources`. |
| `batch_shm_mb=4096` | Size of `/dev/shm` (Docker's default is 64 MB), for tools that use shared memory, e.g. bwa-mem2 or PyTorch data loaders. What's stored there counts towards `mem_mb`. EC2 only. `shared_memory_size_mb` also works. |
| `disk_mb=100000` (or `disk_mib`) | Fargate only: disk space for the job, up to 200 GiB, rounded up to 25 GiB steps. Without it, Snakemake asks for twice the size of the job's input files, and jobs get at least 20 GiB (or `ephemeral-storage-gib`). On EC2 it's ignored: jobs share their machine's disk (see [Limits](docs/reference.md#limits)). |
| `gpu=1` | Number of GPUs (needs a queue with GPU instances; untested). |
| `batch_queue="other-queue"` | Send this rule to a different queue than the profile's. EC2, Spot and Fargate queues can be mixed in one workflow; the plugin works out which is which. |
| `batch_platform="fargate"` | Force the platform instead of detecting it from the queue (only needed if the plugin isn't allowed to look the queue up). |
| `batch_tags="k=v,k2=v2"` | Extra tags on this rule's jobs, e.g. for cost reports. Overrides profile tags with the same key. |
| `batch_spot_retries=0` | How often Batch retries this rule's job if its Spot instance is taken away (0-9; default from the profile, 2). `aws_batch_spot_attempts` (retries + 1) also works. |
| `batch_priority=500` | Scheduling priority, 0-9999, higher first. Only on queues with a fair-share policy. `aws_batch_scheduling_priority` also works. |
| `batch_job_definition="name"` | Use an existing job definition for this rule (see the `job-definition` setting). |
| `batch_script_python="host"` | For Python `script:` rules: use Snakemake's own Python instead of the image's. |
| `batch_entrypoint='["python3"]'` | The image's `ENTRYPOINT`, for when the plugin can't read it from the registry (it then warns at the start): a private registry without `registry-credentials`, or no network to it. `'[]'` for none. |

To change resources without editing the Snakefile (for a published pipeline, say), use
Snakemake's `set-resources`, `set-threads` and `default-resources` in the project's
`profiles/default/config.yaml`:

```yaml
default-resources: [mem_mb=4000]   # rules that set no memory
max-threads: 8                     # caps rules written for big cluster nodes
set-resources:
  align:
    batch_queue: smkflex-big-queue
    mem_mb: 32000
```

Snakemake features that work as usual (each has a small example in `examples/conformance/`):
`shell:` commands; `script:` files in Python, R or bash; `run:` blocks; `conda:` environments and
wrappers (`wrapper:`); `retries: 2`, and resources that grow with each attempt, e.g.
`mem_mb=lambda wildcards, attempt: 4000 * attempt`; `group:` (grouped rules run as one Batch job,
so they must share an image), `checkpoint`, `temp()`, `directory()`, `shadow:`, `log:` and
`benchmark:`.

### Most-used settings

Plugin settings go in a profile as `aws-batch-flex-NAME: value` (or on the command line as
`--aws-batch-flex-NAME value`); `setup.sh` writes the usual ones. The ones people change most:

| Setting (`aws-batch-flex-...`) | What it's for |
|---|---|
| `queue` | The Batch queue jobs go to. |
| `default-image` | The image for rules that have no `container:`. |
| `conda-cache` | Reuse built conda envs: per machine (default), or kept in S3 for later runs. |
| `oom-memory-factor` | How much more memory a job gets when it's retried after running out (default 2×). |
| `spot-retries` | How many times Batch restarts a job whose Spot machine was taken back. |
| `tags` | Extra tags on every job, e.g. to see costs by project. |

All of them, with their defaults, are in the [Reference](docs/reference.md#profile-settings).

## License

MIT, see [LICENSE](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/blob/main/LICENSE).
