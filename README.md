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
ran out of memory with more, and reports what each rule used and cost.

This README starts with [Getting started](#getting-started), from nothing to a finished test
run. [Running your workflows](#running-your-workflows) then covers your own workflows, published
pipelines and pipelines moved from a cluster; [Tuning jobs](#tuning-jobs) covers memory,
software, costs and queues; and the [Reference](#reference) has every setting.

## Getting started

These steps take you from nothing to a finished test run, in under an hour. They assume you're
comfortable typing commands into a terminal, but not that you know AWS or Snakemake.

It has been tested on Linux. On Windows, use [WSL](https://learn.microsoft.com/windows/wsl/install)
and run everything inside it. macOS should work but hasn't been tried.

### 1. What you need

- An AWS account. Setting this up costs very little: building the machine image runs one small
  machine for about 10 minutes, and keeping the image costs well under $1 a month. The test run
  in step 5 costs a few cents. If your account comes from your university or company, read
  [University and company accounts](#university-and-company-accounts) first.
- Somewhere to run commands: your laptop is fine. When you run `snakemake` it has to stay on
  (and online) until the workflow finishes, because it's what tells AWS what to do next;
  `smkflex run` hands that to AWS instead (see [Long runs](#long-runs-smkflex)).

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
[University and company accounts](#university-and-company-accounts) for this step.)

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
[Spot and Fargate queues](#spot-and-fargate-queues)), `--extra-package NAME` adds a Python
package to the image, and `--no-conda` leaves conda out. `bash scripts/setup.sh --help` lists the
rest. To do each step by hand instead, see [Setting up by hand](#setting-up-by-hand).

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
you'll see `3 of 3 steps (100%) done`, followed by a table of what the run used and cost.

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
in [Running a published pipeline](#running-a-published-pipeline) ran its 53 test jobs for about
$0.10; a real run with 20 samples of 30 million reads should be a few dollars (an estimate, not
measured). S3 storage is about $0.02 per GB per month. The table at the end of every run shows
what that run used (see [What a run used](#what-a-run-used)).

That's the setup done. Next, depending on what you're running:

- a workflow of your own: [Your own workflow](#your-own-workflow);
- a pipeline from the Snakemake workflow catalog: [Running a published pipeline](#running-a-published-pipeline);
- a pipeline you've been running on a cluster: read [Your own workflow](#your-own-workflow),
  then [Coming from a cluster](#coming-from-a-cluster).

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
under [Limits](#limits).

Each workflow lives in its own project folder, and its files live in your S3 bucket: jobs read
their inputs from there and write their outputs there. Put the `Snakefile` in a new folder and,
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
anything this workflow needs (memory, conda, retries, see [Tuning jobs](#tuning-jobs)); the
`smkflex` profile keeps what's the same for every workflow (your queue, bucket and roles).

`data` is only this example's folder: push whatever files and folders your rules and sample
sheets name (`smkflex push reads samples.tsv`), and pull whichever outputs you want
(`smkflex pull results/qc`). `push` and `pull` skip files that are already there with the same
size (`--dry-run` only lists them) and also take absolute paths (see
[Coming from a cluster](#coming-from-a-cluster)); paths that go up out of the project folder
(`../raw`) can't be mapped to storage, so use an absolute path for those. If something you push
isn't in `.gitignore`, `push` says so, since `smkflex run` would otherwise send it along with
the workflow.
`export SNAKEMAKE_PROFILE=smkflex` saves typing `--profile` every time. `examples/conformance/`
has one small workflow per feature (scripts, conda, checkpoints, Fargate, ...) if you want to
see more.

### Running a published pipeline

Pipelines from the [Snakemake workflow catalog](https://snakemake.github.io/snakemake-workflow-catalog/)
run unchanged. Set one up the way the catalog suggests, with
[snakedeploy](https://snakedeploy.readthedocs.io) (it's in the `smk-aws` environment): it makes a
project folder with your own copy of the pipeline's config, and a short `workflow/Snakefile` that
fetches the pipeline's code from GitHub at the release you name. From there it's the same as your
own workflow:

```bash
snakedeploy deploy-workflow https://github.com/snakemake-workflows/rna-seq-star-deseq2 my-rnaseq --tag v3.1.2
cd my-rnaseq
smkflex init                       # this project's own folder in the bucket, and a .gitignore
# edit config/config.yaml and the sample sheets (config/samples.tsv, config/units.tsv), as the
# pipeline's docs say, and add what this pipeline needs to the project's settings:
cat >> profiles/default/config.yaml <<'EOF'
software-deployment-method: conda
default-resources: [mem_mb=4000]
max-threads: 8
retries: 2
EOF
smkflex push data                  # the input files, at the paths the sample sheets use
snakemake --profile smkflex        # or: smkflex run --profile smkflex
```

The four settings are what this pipeline needs, the same as it would on any cluster: its rules
use conda; most don't set their memory, so `default-resources` applies (otherwise the plugin's
2 GB); STAR asks for 24 threads, which `max-threads` caps; and `retries` covers jobs that run out
of memory (retried with twice the memory) and flaky downloads. Other pipelines need other values:
their docs say which, and after a first run the table at the end suggests memory for each rule.

Tested with [rna-seq-star-deseq2](https://github.com/snakemake-workflows/rna-seq-star-deseq2) on
its yeast test data, set up as above (v3.1.2): 53 jobs, 13 minutes, about $0.10 of compute.
Without the thread cap it was $0.28, because STAR's 24-thread jobs needed big instances and ran
out of memory once.

- **Packages a pipeline's Snakefile imports**, other than pandas, have to be in the machine image
  (see [Containers and Python](#containers-and-python)).
- **If the pipeline brings its own settings file** (`workflow/profiles/default/`), `smkflex init`
  adds to that one, since Snakemake reads only one.
- **A pipeline's own tests** (`.test/` in its repository) are run with `--directory .test`. That
  doesn't work with remote storage, because Snakemake only sends the jobs the code inside the
  folder it starts in. Copy the test config into `config/` instead.
- **git:** Snakemake keeps a git clone of a pipeline loaded from GitHub, so `git` has to be
  wherever the Snakefile is read: on your machine, in the `smkflex` head job (its image has it),
  and in every job. On EC2, jobs whose image has no git use the one in the machine image (made
  with conda, so images built before this need `scripts/update-ami.sh`); on Fargate, the rules'
  images need their own.
- **Private pipeline repositories** need `GITHUB_TOKEN` set where Snakemake runs and in the jobs
  (`--envvars GITHUB_TOKEN`).

`smkflex run --git URL --add config/` does the same without `snakedeploy`: the code comes from a
clone of the repository and your `config/` is laid over it (see [Long runs](#long-runs-smkflex)).

### Coming from a cluster

On a cluster, every job sees the same filesystem. On AWS, jobs run on machines that come and go,
and files reach them through the S3 bucket: each job downloads its inputs and uploads its
outputs. Most of a pipeline carries over unchanged; these are the parts that don't.

**Paths and data.** Your files have to be in the bucket, under the project's folder there (which
`smkflex init` sets up). Relative paths map straight onto it: `data/S1.fq` is
`s3://BUCKET/snakemake/my-project/data/S1.fq`. Absolute paths do too. Say your config points at a
reference genome in a shared folder on the cluster, e.g. `genome: /shared/refs/hg38.fa` (use
whatever paths your config has): that file is looked for at
`s3://BUCKET/snakemake/my-project/shared/refs/hg38.fa`. So a config full of cluster paths can stay
as it is, and `smkflex push` uploads each path to where Snakemake will look for it:

```bash
smkflex push data             # a relative path, from the project folder
smkflex push /shared/refs     # an absolute one (your own), e.g. run on the cluster, where the data is
```

It sends 16 files at a time (and big files in parallel parts), as fast as `aws s3 sync`, and
skips files that are already there with the same size, so an interrupted upload just continues.

Data that's already in S3, e.g. from a sequencing provider, doesn't need copying: in the rule,
refer to it as `storage("s3://their-bucket/run1/S1.fq")`. The job role then needs read access to
that bucket, which the Terraform setup doesn't give it (add a policy with `s3:GetObject` and
`s3:ListBucket` for it to the job role).

**Profile settings.** A SLURM profile's per-rule settings mostly have a direct counterpart; put
them in the project's `profiles/default/config.yaml`:

| On the cluster | Here |
|---|---|
| `threads`, `cpus_per_task` | `threads` (vCPUs) |
| `mem_mb` | `mem_mb`; `mem_mb_per_cpu` becomes `mem_mb` = per-CPU value × threads |
| `runtime` | `runtime` (minutes; the job is stopped after it) |
| `slurm_partition` | `batch_queue` (e.g. a Spot or Fargate queue) |
| `slurm_account`, `qos` | nothing to set; `batch_tags` can label jobs for cost reports |
| `gpu`, `gres` | `gpu` (needs a GPU queue; untested) |
| `constraint` (node types) | instance types are set in Terraform (`instance_types`) |
| `--jobs` | `jobs` (how many Batch jobs at once) |
| local scratch, `tmpdir` | the instance's disk, shared by its jobs (100 GiB by default); `disk_mb` on Fargate |

**Software.** `envmodules:` and `module load` don't exist here, and neither do tools installed on
the cluster (`/apps/...`): use a container image per rule (e.g. from biocontainers) or a `conda:`
env instead. Conda envs are built by the jobs; the first job on each machine builds an env, the
others reuse it, and with the bucket cache on later runs reuse it too, much like a cluster's
shared `.snakemake/conda` (see [Conda and wrappers](#conda-and-wrappers)).

**Big intermediate files.** Since there's no shared filesystem, a 30 GB BAM that ten rules read is
downloaded ten times (within AWS, so it's fast and free, but it takes time). Grouping rules that
use the same big files into one job (`group:`, see [Many small jobs](#many-small-jobs)) avoids it,
and so do `temp()` files that never need to leave a job.

**Where Snakemake runs.** On a cluster you'd start Snakemake on the login node and log out;
`smkflex run` does that here (next section). `localrules` run where Snakemake runs: your machine,
or the `smkflex` head job.

**Logs.** Snakemake prints the end of a failed job's log; rule `log:` files are in the bucket
(`smkflex pull logs`), and the full job output in CloudWatch (see
[Troubleshooting](#troubleshooting)).

### Long runs: `smkflex`

`snakemake` has to keep running until the workflow is finished, so a laptop that sleeps or loses
its connection stops the run (the jobs already on Batch carry on, and are cancelled when you
restart). For long workflows, `smkflex run` runs Snakemake itself on Batch instead, in a "head
job". It's optional: running `snakemake` from your own machine works as before, and both can be
used on the same workflow.

```bash
cd my-workflow
smkflex run --profile smkflex    # returns once the run is submitted
smkflex logs JOB_ID --follow     # Snakemake's output, live
smkflex status                   # recent runs
smkflex cancel JOB_ID            # stops the run; Snakemake cancels its jobs
```

`smkflex run` packs the workflow folder (what git would track, or else everything a
`.gitignore` there doesn't exclude) together with the profile, uploads it next to your
workflow's files in the bucket (the head job removes the upload again when it's done), and
submits the head job, which runs `snakemake` there. Your machine can be switched off once it
returns. Anything after the options goes to Snakemake, e.g. `smkflex run -- --forceall all`;
`--wait` stays attached and prints the log until the run ends.

- `smkflex run --git https://github.com/lab/rnaseq --rev v2.1` runs a workflow straight from a
  git repository instead of the current folder: `--rev` is a branch, tag or commit (default:
  the default branch), `--path` a folder to run from instead of the top (only what's inside it is
  sent; repositories in Snakemake's standard layout, with `workflow/Snakefile`, need no
  `--path`). The repository is cloned into a temporary folder on your machine (your current
  folder isn't touched), so private repositories work with your usual git login; the profile
  still comes from here (`--profile`). The run's name defaults to the repository's name.
- `--add PATH` sends a local file or folder as well, at the same path, e.g. `--add config/` for
  your own config and sample sheets on top of a `--git` repository (files with the same name
  replace the repository's; with `--path`, paths are relative to that folder). It also sends
  files a local folder's `.gitignore` would leave out. Repeatable.
- Each run records the commit it ran (for a local folder in git, with `-modified` if it had
  uncommitted changes); `smkflex status` shows it.
- Snakemake's `.snakemake` records (what was made with which code, unfinished jobs, logs) are
  saved to the bucket at the end of every run and restored at the start of the next, so reruns
  behave as they do on one machine. Runs with the same name (`--name`, default: the folder's
  name) share them, and a second run of a workflow that's still running is refused.
- The head job runs in `buildpack-deps:bookworm-scm` (Debian 12 with git; `--image` for
  another) and uses 1 vCPU and 2 GiB (`--vcpus`, `--mem-mb`) for as long as the workflow runs,
  on the profile's queue (`--queue` for another one). Don't use a Spot queue for it.
- It needs remote storage (`default-storage-prefix`) and the profile setting
  `aws-batch-flex-head-role-arn`; the profile from `setup.sh` has both.
- The head job runs the plugin version installed where you run `smkflex`, and the AMI's
  Snakemake (in `host` mode) or one installed at start.

All `smkflex` commands are listed under [The smkflex command](#the-smkflex-command).

### Interrupted runs

If `snakemake` is killed or loses its connection, its Batch jobs keep running, and a restart
would submit them again. Jobs are tagged with `snakemake-workflow` (a hash of the working
directory, Snakefile and storage prefix), so on restart the plugin finds jobs from the earlier
run that are still active and cancels them before submitting anything. Use
`--aws-batch-flex-orphan-jobs warn` to only list them (the default with `--nolock`), or `ignore`
to skip the check. If Snakemake then says "IncompleteFilesException", run again with
`--rerun-incomplete`.

## Tuning jobs

### Resources per rule

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
| `mem_mb=16000` (or `mem_mib`) | Memory requested, a hard limit. Snakemake itself runs inside the job too, so 256 MiB is added (`mem-overhead-mib`). Without it (many published pipelines don't set it), `default-resources` applies, or else 2048 MiB. A job that goes over it is stopped; with `--retries 2` (or `retries: 2` in the profile) it's retried with twice the memory, then four times (`oom-memory-factor`). The end-of-run table shows what each rule really used. |
| `runtime=120` | Minutes before Batch stops the job (timeout). To give every rule a timeout, put `runtime` in `default-resources`. |
| `batch_shm_mb=4096` | Size of `/dev/shm` (Docker's default is 64 MB), for tools that use shared memory, e.g. bwa-mem2 or PyTorch data loaders. What's stored there counts towards `mem_mb`. EC2 only. `shared_memory_size_mb` also works. |
| `disk_mb=100000` | Fargate: scratch disk for the job (default 20 GiB, up to 200), rounded up to 25 GiB steps. Snakemake sets it to twice the input size by default, which is used too. EC2: ignored; see [Limits](#limits). |
| `gpu=1` | Number of GPUs (needs a queue with GPU instances; untested). |
| `batch_queue="other-queue"` | Send this rule to a different queue than the profile's. EC2, Spot and Fargate queues can be mixed in one workflow; the plugin works out which is which. |
| `batch_platform="fargate"` | Force the platform instead of detecting it from the queue (only needed if the plugin isn't allowed to look the queue up). |
| `batch_tags="k=v,k2=v2"` | Extra tags on this rule's jobs, e.g. for cost reports. Overrides profile tags with the same key. |
| `batch_spot_retries=0` | How often Batch retries this rule's job if its Spot instance is taken away (0-9; default from the profile, 2). `aws_batch_spot_attempts` (retries + 1) also works. |
| `batch_priority=500` | Scheduling priority, 0-9999, higher first. Only on queues with a fair-share policy. `aws_batch_scheduling_priority` also works. |
| `batch_job_definition="name"` | Use an existing job definition for this rule (see the `job-definition` setting). |
| `batch_script_python="host"` | For Python `script:` rules: use Snakemake's own Python instead of the image's. |

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

### Containers and Python

Each rule runs in its own image, and Snakemake comes from the machine image, so images need
nothing of their own (but see [Limits](#limits): a glibc-based image with `/bin/sh` and bash).

**Python `script:` rules** run with the image's own `python` if it's 3.10 or newer, so they can
use the image's packages. Otherwise (no Python in the image, or an older one) and for `run:`
blocks, the code runs in Snakemake's own Python from the machine image, which only has
Snakemake's packages; call such scripts from `shell:` instead, or add their packages to the
image as below. `batch_script_python="host"` on a rule (or `aws-batch-flex-script-python: host`
for all) always uses Snakemake's Python.

**Python packages your Snakefile imports.** Snakemake reads the Snakefile not only where you start
it but again inside every job, so a package the Snakefile (or a file it includes with `include:`) imports at
the top, like `import pandas`, is needed in the jobs as well. Many published pipelines do this.
`setup.sh` puts pandas into the machine image; add others with `--extra-package NAME` when
setting up, or later with [Updating the machine image](#updating-the-machine-image). Jobs that
install Snakemake at start (Fargate, `bootstrap: auto`) take
`aws-batch-flex-extra-packages: [pandas]` in the profile instead. The package has to be installed
where you run `snakemake`, too. A job that fails this way says which package it was.

### Conda and wrappers

Set `software-deployment-method: conda` (or run with `--sdm conda`). Jobs need `conda`: either
the image has it, or it's in the machine image (`setup.sh` puts Miniforge there by default) and
the profile has `aws-batch-flex-conda-path: /opt/miniforge3`. Only `condabin` is added to the
job's PATH, so an image's own conda still wins. The machine you run `snakemake` on needs `conda`
on its PATH as well. Wrappers work the same way; the plugin passes jobs an HTTPS
`--wrapper-prefix`, so images don't need git.

Each job builds the envs its rule needs, which takes a minute or more per env. So that this
happens once rather than in every job, built envs are reused (`aws-batch-flex-conda-cache`):

- **On the machine** (the default, `instance`): jobs on the same EC2 instance share a folder for
  them, so the first job of a rule builds the env and the others on that machine use it (a job
  that needs an env another job is building waits for it). It's gone when the instance shuts
  down.
- **In the bucket** (`aws-batch-flex-conda-cache: s3://BUCKET/snakemake/.smkflex/conda-cache`): a
  job that builds an env also uploads it there, packed, and later jobs on any machine, later
  runs, other projects and Fargate jobs unpack it in seconds instead. `setup.sh` puts this line,
  commented out, in the profile. An env is typically 0.1-2 GB, a few cents a month each.

On rna-seq-star-deseq2's test data, a run took 18-21 minutes with every job building its envs,
11 minutes with the per-machine cache (12 builds instead of 52), and 7.5 minutes on a second run
with the S3 cache (no builds at all).

An env is only reused for the same definition (the YAML file, its pinned versions, the wrapper
version) and for images with the same C library version (glibc), which is what conda chooses
its packages for, so a cached env never differs from one the job would have built. Changing an
env's file gives it a new name, and old ones are simply not used any more; delete
`.smkflex/conda-cache/` in the bucket to clear the cache. Anyone who can write to the bucket
could put a different env there, the same as for the workflow's files. `none` turns it off.

Envs can also be baked into the machine image and used by name (`conda: "rnaseq"`), see
[Updating the machine image](#updating-the-machine-image).

### What a run used

At the end of a run the plugin prints what each rule used, from Batch's own start and stop times
(Spot retries included), what it cost, and how much memory, CPU and disk its jobs really needed:

```
AWS Batch usage of this run (cost: the EC2 instances at AWS's prices, shared out by vCPU-time; without storage or data transfer):
  rule         jobs    vCPU-h     GiB-h      ~USD  vCPUs   busy   mem MiB    peak  disk GiB
  star_align      4      0.96      0.32      0.21      8    7.6      4071    1840       3.2
  fastp_pe        4      0.08      0.32      0.02      4    1.4      4071     310       0.4
  total           8      1.04      0.64      0.23
EC2: 1 × m6i.2xlarge (on-demand, 32 min in all): ~$0.23 at AWS's prices (disks included: $0.01); the jobs used 24% of these instances' vCPU-time.
vCPUs, mem MiB: what a job had; busy: the most vCPUs in use at once; peak: the most memory its processes used (without file cache); disk GiB: the most its working folder took. Each the highest of the rule's jobs, sampled every 2 s.
Memory for each rule (its peak + 30%), to paste into the project's profiles/default/config.yaml (smkflex init makes it):
set-resources:
  fastp_pe:
    mem_mb: 300     # had 4071 MiB, peak 310
  star_align:
    mem_mb: 2300    # had 4071 MiB, peak 1840
Threads for rules that never had most of theirs busy (the most in use at once + 25%; a burst shorter than 2 s isn't seen, so check the tool first):
set-threads:
  fastp_pe: 2     # had 4, at most 1.4 busy
```

- **Cost:** on EC2 you pay for whole instances, so that's what's counted: the plugin notes which
  instances the jobs ran on, how long each ran during the run (idle time included) and its type,
  and prices it, with its disk, at AWS's on-demand list price or the Spot price of its zone at
  the time. Each
  instance's cost is shared out to the rules by their jobs' vCPU-time on it. The `EC2:` line says
  which instances they were and how much of them the jobs used: a low share means jobs left
  instances half empty, often because one rule's memory needs a big instance. Fargate is priced
  per job, which is how Fargate bills. Instances still running at the end (Batch keeps them a few
  minutes) are counted until then, and an instance that other runs used at the same time is all
  counted here. If the instances can't be looked up (see [IAM](#iam)), the table falls back to a
  rough estimate from the jobs' vCPUs and says so. AWS Cost Explorer has the exact figures, a day
  later: the instances are tagged `ManagedBy=snakemake-batch-flex`, which you can filter by once
  it's switched on under **Billing** > **Cost allocation tags**.
- **Memory, CPU and disk:** each job samples its container every 2 seconds and logs the most it
  saw: its processes' memory (from Linux's cgroup statistics, without file cache, which Linux
  frees before it kills anything), how many vCPUs were busy at once, and how big its working
  folder (inputs, outputs, conda downloads) got. The plugin reads those lines from the job logs
  (on runs with more than 300 jobs, from 25 per rule). A spike shorter than 2 seconds can be missed, which the retry with more memory
  covers. For a job that builds its conda environment, that build counts too (often 1-1.5 GB).
  The `set-resources:` block has every rule whose peak is known, at its peak plus 30% (at least
  512 MiB per job): paste it into the project's settings, and the next run neither wastes room
  on the instances nor runs out. On rna-seq-star-deseq2's test data the rules used 0.3-2.7 GB of
  the 4 GB they asked for, and ran fine with the suggestions.
- **Threads:** rules that asked for 2 or more vCPUs but never had more than 60% of them busy get
  a `set-threads:` suggestion (the most in use at once plus 25%). Pipelines written for big
  cluster nodes often ask for more threads than their tools use, and on Batch that means bigger,
  half-idle instances. A tool that uses its threads only in bursts shorter than 2 seconds can
  look idle, so check before lowering.
- **Disk:** a rule whose jobs used more than 20 GiB gets a hint, since on EC2 the jobs on one
  instance share its disk, and on Fargate a job has 20 GiB unless `disk_mb` asks for more.

Turn the table off with `no-cost-summary`.

### Many small jobs

Every Batch job costs about 30-60 seconds on top of its command: Batch has to schedule it, start
its container and copy its files from and to the bucket. For a rule with thousands of jobs that
take a few seconds each, that overhead is most of the run. Snakemake can put many of them into
one Batch job:

```python
rule index_reads:
    input: "reads/{sample}.fq"
    output: "index/{sample}.idx"
    group: "index"
    shell: "..."
```

and then run with `--group-components index=50` (or `group-components: [index=50]` in the
project's settings): each Batch job then does 50 of them. The jobs in one group run in one
container, so the rules in a group must use the same image; how Snakemake adds up their CPU and
memory is in its [grouping docs](https://snakemake.readthedocs.io/en/stable/executing/grouping.html).
When a rule ran at least 20 jobs with a median of under a minute, the table at the end of the run
suggests this.

### Spot and Fargate queues

**Spot instances** are AWS's spare capacity: the same machines, usually 60-90% cheaper, but AWS
can take one back with two minutes' warning, which kills the job on it. Batch then starts the job
again on another machine (`spot-retries`, 2 by default; real failures aren't retried this way).
`setup.sh --spot` (or `enable_spot_queue = true` in `terraform.tfvars`) creates a second queue,
`<name>-spot-queue`, next to the normal one; send rules to it:

```python
rule call_variants:
    resources: batch_queue="smkflex-spot-queue"
```

or make it the profile's `aws-batch-flex-queue` and send the rest to the normal queue. Keep very
long jobs (a restart begins from scratch) and `smkflex` head jobs (losing one stops the run) on
the normal queue.

**Fargate** runs each job without machines to manage, and bills per job; `setup.sh --fargate`
creates a Fargate queue. Jobs there install Snakemake when they start (10-15 s), get their scratch
disk from `disk_mb`, need `conda` in the image for `conda:` rules (the S3 conda cache saves them building envs) and
`git` for pipelines loaded from GitHub, and
are limited to 16 vCPUs and 120 GiB. EC2 and Fargate queues can be mixed in one run with
`batch_queue`.

### Private images

ECR images need nothing extra: the instance role (EC2) or execution role (Fargate) pulls them,
and the roles from `terraform/` already have ECR read access. For another account's ECR, that
repository's policy has to allow those roles.

For registries outside AWS (Docker Hub, GitHub, Quay...), store `{"username": ..., "password": ...}`
in Secrets Manager and map the registry to it in the profile:

```yaml
aws-batch-flex-registry-credentials: [docker.io=arn:aws:secretsmanager:eu-west-1:123456789012:secret:dockerhub]
aws-batch-flex-execution-role-arn: arn:aws:iam::123456789012:role/my-exec-role
```

Only images from that registry get the credentials. ECS reads the secret with the execution role,
so it's needed on EC2 too, and it needs `secretsmanager:GetSecretValue` on the secret.

## Troubleshooting

- **Not sure what's wrong?** Run `smkflex check --profile smkflex` in your workflow folder. It
  checks your AWS login, the queue, the roles, that it can write to the bucket, and the
  Snakemake versions, and says what to fix.
- **"Startup check failed".** The plugin checks the queue before submitting anything; the
  message says what's wrong (usually a typo in the queue name, or the wrong region).
- **Jobs stay "RUNNABLE" for more than ~10 minutes.** No machine could start. Check the compute
  environment in the AWS console (**Batch** > **Compute environments**) for an error, and that
  your subnets have internet access. New accounts sometimes have a low limit on machines;
  Snakemake also prints a warning when a job waits too long.
- **A job fails.** Snakemake prints the end of the job's log, and for causes the plugin recognises
  (out of memory, a full disk, a missing package or storage plugin, S3 permissions) a hint on what
  to do. Full logs are in the AWS console under **CloudWatch** > **Log groups** >
  `/aws/batch/smkflex` (`/aws/batch/` plus the `name` from `terraform.tfvars`; `/aws/batch/job`
  without the Terraform setup); the rule's own `log:` files are in the bucket (`smkflex pull logs`).
- **A job fails with "403" or "Forbidden" from S3.** Jobs use the job role, not your login, and
  the Terraform setup only lets it use `s3://BUCKET/snakemake/`. Keep the project's folder inside
  it (which `smkflex init` does).
- **"Ignoring source file ../workflow/...".** Snakemake only sends the jobs code that's inside the
  folder you start it from, so start it from the top of the pipeline (not with `--directory`
  pointing at a subfolder). Published pipelines' tests often use `--directory .test`; copy the
  test config into `config/` and run from the top instead.
- **A dry run (`-n`) lists jobs for a finished workflow** with "software environment definition
  has changed". That's a Snakemake quirk with `conda:` rules: a dry run treats the jobs as local
  and works out their environments, which a real run (where the jobs build them on AWS) doesn't
  record. The real run says "Nothing to be done". `-n --rerun-triggers mtime params input code`
  shows what the real run will do.
- **"IncompleteFilesException" after Snakemake was stopped.** Run again with
  `--rerun-incomplete`. Jobs left running by the stopped run are cancelled automatically.

## Reference

### Profile settings

Settings go in the `smkflex` profile, `~/.config/snakemake/smkflex/config.yaml`, in a project's
`profiles/default/config.yaml`, or on the command line. In a profile, each is a line with its
full name and a value; on the command line, the same name with `--` in front:

```yaml
aws-batch-flex-ephemeral-storage-gib: 50     # on the command line: --aws-batch-flex-ephemeral-storage-gib 50
aws-batch-flex-spot-retries: 0
aws-batch-flex-skip-preflight: true          # on the command line: just --aws-batch-flex-skip-preflight
```

Only the queue is required, and `setup.sh` writes the usual ones for you. The settings are
grouped below; most people never need more than the first group. In the tables, a value in
`code` is the default, and one in *italics* says what happens when the setting isn't given.

**Basics**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `queue` | *required* | The Batch queue jobs go to. A rule can send its jobs elsewhere with `batch_queue`. |
| `region` | *your AWS config's region* | The AWS region of the queue. |
| `job-role-arn` | *the instances' own role (EC2)* | The IAM role jobs run as. It needs to read and write the S3 bucket. |
| `execution-role-arn` | *none* | The IAM role Batch uses to start a job: to pull its image and send its logs. Needed for Fargate queues and private registries. |
| `head-role-arn` | *none* | The IAM role of the head job, the Batch job in which `smkflex run` runs Snakemake (see [Long runs](#long-runs-smkflex)). Only `smkflex` uses it. |
| `default-image` | *none* | The image for rules that have no `container:`. |
| `tags` | *none* | Extra `KEY=VALUE` tags on every job, e.g. `[project=rnaseq, team=genomics]`, for example to see costs by project. |

**How jobs get Snakemake**

Jobs run in each rule's own image, which normally has no Snakemake in it. On EC2, Snakemake comes
from the machine image (`host`); otherwise it's installed when each job starts, which takes
10-15 s per job.

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `bootstrap` | `auto` | `host`: use Snakemake from the machine image (what `setup.sh` sets; EC2 only). `auto`: use the image's own Snakemake if it has the same version as yours, otherwise install it when the job starts. `always`: always install it. `never`: the image has it. |
| `host-env-path` | `/opt/snakemake-env` | Where the machine image has Snakemake. |
| `host-env-wait-seconds` | `120` | How long a job waits for that Snakemake if its machine is still setting it up when the job starts. A machine image made by `setup.sh` has it from the start, so 0 is fine there. |
| `extra-packages` | *none* | Extra pip packages to install along with Snakemake when a job starts (e.g. `[pandas]`, or other Snakemake plugins). |
| `python-version` | *the one you run Snakemake with* | The Python version to install Snakemake with when a job starts. |
| `install-prereqs` | `false` | When installing at start: first install `curl` into images that have none of `curl`, `wget` or `pip`. |
| `script-python` | `image` | Which Python runs Python `script:` rules. `image`: the image's own, if it's 3.10 or newer, so the script can use the image's packages. `host`: always Snakemake's own. |

**Memory, disk and Spot**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `mem-overhead-mib` | `256` | Memory added to each job on top of the rule's `mem_mb`, for the Snakemake process that runs in the job. |
| `oom-memory-factor` | `2` | When a job runs out of memory and Snakemake retries it (`retries:`), the retry gets this many times the memory, and again on each further retry (on Fargate, up to 120 GiB). `1` retries with the same memory. |
| `spot-retries` | `2` | How many times Batch restarts a job when AWS takes its Spot machine back. `0`: never. |
| `ephemeral-storage-gib` | *none: jobs get 20 GiB* | Fargate only: disk space for every job, in GiB: a whole number from 21 to 200, e.g. `50`. Without it, each job has the 20 GiB Fargate always gives. A rule's `disk_mb` can ask for more for its own jobs. (On EC2, jobs share their machine's disk; its size is set in Terraform.) |
| `assign-public-ip` | `ENABLED` | Fargate only: `ENABLED` gives jobs a public IP address, which they need to reach the internet (images, S3) unless the subnet has a NAT gateway. `DISABLED` for subnets that do. |

**Conda**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `conda-path` | *none* | Where the machine image has conda (`setup.sh` sets it), so `conda:` rules also work in images without conda. EC2 only. |
| `conda-cache` | `instance` | Reuse conda environments instead of building them in every job. `instance`: jobs on the same machine share them. An S3 URL, e.g. `s3://BUCKET/snakemake/.smkflex/conda-cache`: they're also kept there for later jobs and runs. `none`: build in every job. See [Conda and wrappers](#conda-and-wrappers). |

**Logs, checks and leftovers**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `log-group` | `/aws/batch/job` | The CloudWatch log group for job logs. It must already exist. Terraform creates one that deletes logs after 30 days and puts it in the profile; Batch's default group keeps them forever. |
| `no-cost-summary` | `false` | Don't print the table of what the run used and cost at the end (see [What a run used](#what-a-run-used)). |
| `runnable-warn-minutes` | `15` | Warn when a job has waited this long for a machine to run on. |
| `skip-preflight` | `false` | Skip the checks of the queue and roles at the start of a run. |
| `orphan-jobs` | `cancel` | What to do with jobs still running from an earlier run that was killed: `cancel`, `warn` or `ignore` (see [Interrupted runs](#interrupted-runs)). |
| `no-auto-tags` | `false` | Don't tag jobs with their run, rule and so on. Without those tags, jobs left over from a killed run can't be found. |

**Less common**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `platform` | *read from the queue* | `ec2` or `fargate`. Only needed if it can't be read from the queue. |
| `volumes` | *none* | Folders on the EC2 machine to make available in every job, as `MACHINE_PATH:JOB_PATH`, with `:ro` for read-only, e.g. `[/data/refs:/refs:ro]`. |
| `scheduling-priority` | *none* | Priority for every job (0-9999), for queues with a fair-share scheduling policy. Other queues ignore it, with a warning. |
| `registry-credentials` | *none* | `REGISTRY=SECRET_ARN` for private registries outside AWS (see [Private images](#private-images)). |
| `job-definition` | *none* | Use an existing Batch job definition (name, `name:revision` or ARN) instead of the ones the plugin makes, e.g. where you aren't allowed to make them. It decides the image, job role, mounted folders and `/dev/shm`; vCPUs, memory, timeout and tags still come from the rules. If it doesn't mount the machine image's Snakemake, jobs install it when they start. |
| `job-definition-prefix` | `snakemake` | How the job definitions the plugin makes are named. |

Every job is also tagged with `snakemake-run-id`, `snakemake-workflow`, `snakemake-rule` and
`snakemake-job-id`, so you can find a run's jobs, or a rule's, in the Batch console and in billing.

### The smkflex command

`smkflex` comes with the plugin. Without `--profile`, it uses the only profile for this plugin in
`~/.config/snakemake/`; `smkflex COMMAND --help` lists each command's options.

| Command | What it does |
|---|---|
| `smkflex check` | Checks the setup: AWS login, queue, roles, bucket, and that the versions fit together. |
| `smkflex init` | Sets up the current folder as a project: its own folder in the bucket, and a `.gitignore` (see [Your own workflow](#your-own-workflow)). |
| `smkflex push PATH...` | Uploads input files or folders to where the rules look for them, e.g. `smkflex push data /shared/refs`. |
| `smkflex pull PATH...` | Downloads outputs from the bucket into the current folder, e.g. `smkflex pull results logs` (`--to` puts them elsewhere). |
| `smkflex run [-- OPTIONS]` | Runs the workflow on AWS in a head job, so your machine can be switched off (see [Long runs](#long-runs-smkflex)). Snakemake options go after `--`, e.g. `smkflex run -- --forceall`. `--git URL` runs a workflow from a git repository; `--wait` waits for the run to finish. It prints the run's JOB_ID. |
| `smkflex status` | Lists recent runs started with `smkflex run`: their JOB_ID, state and the commit each one ran. |
| `smkflex logs JOB_ID` | Prints a run's Snakemake output; `--follow` keeps watching. |
| `smkflex cancel JOB_ID` | Stops a run. Snakemake cancels its jobs first. |

### University and company accounts

Organisations often manage AWS access centrally: you log in through a web page (single sign-on),
can't create users, and don't have administrator rights. That works, with these adjustments.

**Logging in with single sign-on.** Instead of access keys (step 3), run `aws configure sso` once
(your IT department can tell you the start URL and region) and give the profile a name, e.g.
`lab`. Then, in each new terminal:

```bash
aws sso login --profile lab
export AWS_PROFILE=lab
```

Everything here, `setup.sh`, Terraform, Snakemake and `smkflex`, uses that login.

**Permissions.** `setup.sh` creates IAM roles, a Batch queue and compute environment, an S3
bucket, a log group and a machine image. If you can't, ask IT to run `setup.sh` once for the lab,
or to give you `PowerUserAccess` plus the right to create IAM roles and instance profiles named
`smkflex-*`. Once it's set up, people who only run pipelines need much less: the policy printed
by `terraform -chdir=terraform output -raw launcher_policy_json` (see [IAM](#iam)), which IT can
attach to your login.

**No default network.** Some accounts have no default subnets, so `setup.sh` stops and says so.
Ask IT which subnets to use and pass them with `--subnets subnet-a,subnet-b`. The machines need
outbound internet (to pull images and install conda packages): a public subnet, or a private one
with a NAT gateway (for Fargate jobs there, also set `aws-batch-flex-assign-public-ip: DISABLED`
in the profile).

### Updating the machine image

To add Python packages or named conda envs to the machine image, or to get a newer Snakemake into
it, bake a new image on top of the current one:

```bash
bash scripts/update-ami.sh --subnet subnet-XXXX --region eu-west-1 --extra-package scikit-learn
bash scripts/update-ami.sh --subnet subnet-XXXX --region eu-west-1 --conda-env envs/rnaseq.yaml
```

It points `terraform/terraform.tfvars` at the new image and shows the Terraform plan; `terraform
apply` then updates the compute environment's image in place (new instances start from it) and
leaves the queue alone. Apply it while no jobs are running. Envs baked with `--conda-env` can be
used by name (`conda: "rnaseq"`). Old images can be removed with `scripts/cleanup-ami.sh` (see
[Costs and removing everything](#costs-and-removing-everything)).

### Setting up by hand

What `scripts/setup.sh` does, step by step.

#### Find your subnets

AWS machines start inside a "subnet", a slice of your account's network. New accounts come with
default subnets that have internet access, which is what's needed here (to download container
images). List them:

```bash
aws ec2 describe-subnets --filters Name=default-for-az,Values=true --query 'Subnets[].SubnetId' --output text
```

You'll get two or more IDs like `subnet-0a1b2c3d`. Note them down. If the list is empty, your
account has no default network; create one with `aws ec2 create-default-vpc` and run the command
again.

#### Build the machine image

This starts a temporary machine from Amazon's standard image for Batch, installs Snakemake on it,
saves the result as your own image, and removes the temporary machine. First look up Amazon's
current standard image for your region:

```bash
aws ssm get-parameter --name /aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id --query Parameter.Value --output text
```

Then build (use that image ID, one of your subnets and your region; `--conda` and
`--extra-package pandas` are what `setup.sh` adds):

```bash
bash scripts/bake-ami.sh --base-ami ami-XXXXXXXX --subnet subnet-XXXXXXXX --region eu-west-1 --conda --extra-package pandas
```

It takes 10 to 20 minutes and ends with:

```
DONE. New standalone AMI: ami-0123456789abcdef0 (snakemake-host-env-...)
Put this in terraform.tfvars:   ami_id = "ami-0123456789abcdef0"
```

Note that AMI ID.

#### Create the AWS resources

[Terraform](https://www.terraform.io) creates everything else from the files in `terraform/`: the
queue jobs are sent to, the "compute environment" that starts and stops machines for that queue,
the permissions (IAM roles) the machines and jobs need, and an S3 bucket for your files.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Open `terraform.tfvars` in a text editor and fill in the region, your subnet IDs and the AMI ID
from above. Then:

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

The machines aren't started yet. The compute environment starts them when jobs arrive and stops
them when the queue is empty, so nothing is running (or costing money) at this point.

#### Make a profile

Terraform prints a profile for you; install it under the name `smkflex`:

```bash
mkdir -p ~/.config/snakemake/smkflex
terraform -chdir=terraform output -raw snakemake_profile > ~/.config/snakemake/smkflex/config.yaml
echo "jobs: 100" >> ~/.config/snakemake/smkflex/config.yaml
cat ~/.config/snakemake/smkflex/config.yaml
```

(With conda in the image, also add `aws-batch-flex-conda-path: /opt/miniforge3`.) You should see
lines like `executor: aws-batch-flex` and `aws-batch-flex-queue: smkflex-queue`.

### Without the machine image

To try the plugin on an existing Batch queue without building an AMI, set
`aws-batch-flex-bootstrap: auto` in the profile instead of `host`. Snakemake is then installed into
each container when its job starts (about 10-15 s per job), which needs internet access and
`curl`, `wget` or `pip` in the image (`--aws-batch-flex-install-prereqs` installs `curl` first if
missing).

### Costs and removing everything

While idle, the only costs are the files in your bucket, the job logs and the stored machine image
(all cents per month for small amounts). Job logs are deleted after 30 days
(`log_retention_days` in `terraform.tfvars`). Files in the bucket stay until you delete them; set
`s3_expire_days` to have files under the prefix deleted automatically that many days after they
were written (only do this if you copy results you want to keep elsewhere). Uploads that were
interrupted, e.g. by a killed job, are cleared after 7 days. Machines only run while jobs do: the
AWS console's **EC2** > **Instances** page should show no running `snakemake`-related instances a
few minutes after a workflow ends.

To remove everything:

```bash
aws s3 rm s3://YOUR-BUCKET --recursive          # empty the bucket (deletes your results!)
cd terraform && terraform destroy && cd ..       # remove queue, roles, bucket, ...
bash scripts/cleanup-ami.sh --region eu-west-1 --no-tfvars ami-XXXXXXXX   # the machine image (ID in terraform/terraform.tfvars)
bash scripts/cleanup-jobdefs.sh --region eu-west-1                       # the plugin's job definitions
rm -r ~/.config/snakemake/smkflex                                        # the profile
```

`terraform destroy` refuses to delete a bucket that still has files in it, hence the first line.
`cleanup-ami.sh` only deletes images that `bake-ami.sh` made, asks before deleting, and refuses
any image that's still in use by a compute environment, launch template or machine. Normally it
also refuses the image named in `terraform.tfvars`; `--no-tfvars` skips that one check, since
after `terraform destroy` nothing uses it any more.

`cleanup-jobdefs.sh` is also handy on its own now and then: job definitions for images you no
longer use stay in AWS (they cost nothing, but pile up). It only removes the plugin's own
definitions, never one an active job uses, and the plugin simply registers them again if a later
run needs one. Add `--dry-run` to see the list first.

### IAM

The identity running `snakemake` needs `batch:SubmitJob`, `DescribeJobs`, `TerminateJob`,
`RegisterJobDefinition`, `DescribeJobDefinitions`, `DescribeJobQueues`, `ListJobs`,
`DescribeComputeEnvironments` and `TagResource`, plus `logs:GetLogEvents`, `iam:PassRole` on the
job role(s), and read/write on the S3 prefix. Optionally `ec2:DescribeImages` and
`ec2:DescribeLaunchTemplateVersions`, so the plugin can warn when the AMI's Snakemake version is
far from yours, and `ecs:DescribeContainerInstances`, `ec2:DescribeInstances`,
`ec2:DescribeVolumes`, `ec2:DescribeSpotPriceHistory` and `pricing:GetProducts`, so it can price
the instances (and their disks) a run used. `terraform output launcher_policy_json` prints a
policy with all of this. The job role needs read/write on the prefix. For `smkflex`, the head
job's role needs the same as the identity running `snakemake`, and that identity also needs
`iam:PassRole` on the head role (both are in the Terraform setup).

### What's been tested

Run on real AWS (eu-west-1): `shell:`, `run:`, Python/bash/R `script:` (Python scripts in the
image's own Python), `wrapper:`, conda (YAML in a conda image, YAML and named envs in conda-less
images), a different image per rule, a 6-image variant-calling pipeline, EC2 and Fargate
(separately and mixed in one run), checkpoints, groups, shadow, benchmark/log, retries, `temp()`,
`directory()`, resources and tags, OOM and timeout reporting, cancelling with Ctrl-C, startup
checks, fan-out to 100 jobs, install-at-start on a bare `debian:12`, cancelling jobs left over
from an interrupted run, pricing a run by the EC2 instances it used, `/dev/shm` size per rule, Fargate disk size from `disk_mb`, updating the
AMI with `update-ami.sh`, using an existing job definition, a private ECR image, job logs in
Terraform's own log group (EC2 and Fargate), a whole workflow run as a `smkflex` head job (from a
folder and with `--git`) and cancelling one, jobs on the Spot queue, the usage table (with each
rule's peak memory and the hint about many short jobs), retrying a job that ran out of memory with
more, the whole setup with `scripts/setup.sh` (twice, as a second, separate setup in the same
account; with conda and pandas in the image, and `smkflex init`/`push`/`pull`), the published
rna-seq-star-deseq2 pipeline on its test data (with `snakemake`, as a `smkflex` head job, and set
up with `snakedeploy`), and the conda env cache on it (per machine: 12 builds instead of 52, 11
instead of 18-21 minutes; from S3 on a second run: no builds, 7.5 minutes).

Only tested against a fake Batch: the CPU and disk figures and disk cost, private registry
credentials (outside AWS), `--aws-batch-flex-volumes`, a single Ctrl-C, a Spot instance being
taken back mid-job.

Not tested yet: `notebook:`, Julia, GPU.

### Limits

- Host mode needs a glibc image with `/bin/sh` and bash. Alpine/musl images exit with 127 and say
  why. Build the AMI on the same CPU architecture as your instances.
- `pipe()` doesn't work. `--sdm apptainer` isn't supported, and Singularity images (`.sif`) can't
  be used.
- Images with an ENTRYPOINT that doesn't pass the command through will break.
- All rules in a group must use the same image.
- On EC2, all jobs on an instance share its disk (`root_volume_gib` in Terraform, 100 GiB in the
  example), and `disk_mb` is not enforced: Batch only places jobs by CPU and memory. Several jobs
  with large files on one instance can fill it, and the jobs then fail with "No space left on
  device" (the failure message says so). Use a bigger volume, or a second queue with bigger disks
  for the heavy rules (`batch_queue`).
- Install-at-start fails on very old images (e.g. glibc 2.23).
- Your Snakemake and the AMI's don't have to be the same version: 9.5 up to 9.27 mix fine in both
  directions (tested against the fake Batch; 9.0.x doesn't work). But every storage plugin
  installed on your machine must also be in the AMI, because Snakemake passes all of their
  options to the jobs; a job that fails this way says which plugin it is.

## License

MIT, see [LICENSE](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/blob/main/LICENSE).
