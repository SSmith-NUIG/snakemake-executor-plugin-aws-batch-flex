# Guide

Running workflows with the plugin, and tuning them. Setting up is in the
[README](../README.md#getting-started); every setting is in the [Reference](reference.md).

- [Running workflows](#running-workflows): [published pipelines](#running-a-published-pipeline),
  [coming from a cluster](#coming-from-a-cluster),
  [what works differently from a local run](#what-works-differently-from-a-local-run),
  [long runs with `smkflex`](#long-runs-smkflex),
  [interrupted runs](#interrupted-runs)
- [Tuning jobs](#tuning-jobs): [containers and Python](#containers-and-python),
  [conda](#conda-and-wrappers), [what a run used](#what-a-run-used), [many small
  jobs](#many-small-jobs), [Spot and Fargate](#spot-and-fargate-queues), [private
  images](#private-images)
- [Troubleshooting](#troubleshooting)

## Running workflows

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
skips files that are already there and unchanged since (same size, not edited after they were
uploaded, as `aws s3 sync` decides), so an interrupted upload just continues.

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

### What works differently from a local run

Jobs run on other machines and only share the bucket, so a few things that work when Snakemake
runs everything on your machine need a change:

- **Input data has to be in the bucket.** Upload it once with `smkflex push data` (see
  [Paths and data](#coming-from-a-cluster)); a "Missing input files … (in storage)" error says
  which files are only on your machine.
- **Results are in the bucket, not on your machine.** Code that runs where Snakemake runs and
  opens an output, such as an `onsuccess:` handler that prints a summary, finds no file. Do it in
  a rule that has the file as its input (a `localrules:` rule if it should run on your machine):
  Snakemake then fetches it. Or download the results after the run: `smkflex pull results`.
- **Jobs get the workflow folder,** except `data/`, `results/`, `logs/`, `benchmarks/`, what
  `.gitignore` excludes (but `config/` always goes), files over 10 MB and anything outside the
  folder. A file a rule reads that isn't one of its inputs has to be in there (see
  [Troubleshooting](#troubleshooting)).
- **A job only gets the inputs its rule names.** Files a tool finds next to an input locally,
  such as a genome's index (`genome.fa.bwt`, `genome.fa.fai`, `genome.dict`) or a BAM's `.bai`,
  aren't fetched unless the rule lists them: `input: multiext("genome.fa", ".amb", ".ann",
  ".bwt", ".pac", ".sa")` for bwa, or `bai="x.bam.bai"`. The failed job's error says so when the
  tool's message is a known one.
- **No `workdir:`.** The jobs need the Snakefile inside the folder Snakemake works in, so the run
  stops at the start and says so. Outputs go under the storage prefix anyway: remove it.
- **`threads: workflow.cores`** needs `--cores N`, as on a cluster (Snakemake asks for it). Each
  such job then asks for N vCPUs, so N has to fit on one machine of the queue.
- **`envmodules:`** is ignored (there's no module system), so the tool it would load is missing:
  use a `container:` or `conda:` (see Software above).

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
`.gitignore` there doesn't exclude, and `config/`, but never `data/`, `results/`, `logs/` or
`benchmarks/`) together with the profile, uploads it next to your
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
  uncommitted changes or new files that aren't git-ignored, and `+added` with `--add`);
  `smkflex status` shows it.
- Snakemake's `.snakemake` records (what was made with which code, unfinished jobs, logs; also
  with `--persistence-backend db`) are saved to the bucket at the end of every run and restored at the start of the next, so reruns
  behave as they do on one machine. As with Snakemake's lock on a folder, a project (its
  storage prefix) runs once at a time: a second `smkflex run` while one is going is refused.
  For more samples, add them to the sample sheet and run again once the first run is done or
  cancelled (finished outputs are kept); for another set of samples, `smkflex init` a new folder.
- The head job runs in `buildpack-deps:bookworm-scm` (Debian 12 with git; `--image` for
  another) and uses 1 vCPU and 2 GiB (`--vcpus`, `--mem-mb`) for as long as the workflow runs,
  on the profile's queue (`--queue` for another one). Don't use a Spot queue for it.
- It needs remote storage (`default-storage-prefix`) and the profile setting
  `aws-batch-flex-head-role-arn`; the profile from `setup.sh` has both.
- The head job runs the plugin version installed where you run `smkflex`, and the AMI's
  Snakemake (in `host` mode) or one installed at start.

All `smkflex` commands are listed under [The smkflex command](reference.md#the-smkflex-command).

### Interrupted runs

If `snakemake` is killed or loses its connection, its Batch jobs keep running, and a restart
would submit them again. Jobs are tagged with `snakemake-workflow` (a hash of the working
directory, Snakefile and storage prefix), so on restart the plugin finds jobs from the earlier
run that are still active and cancels them before submitting anything. Use
`--aws-batch-flex-orphan-jobs warn` to only list them (the default with `--nolock`), or `ignore`
to skip the check. If Snakemake then says "IncompleteFilesException", run again with
`--rerun-incomplete`.

## Tuning jobs

### Containers and Python

Each rule runs in its own image, and Snakemake comes from the machine image, so images need
nothing of their own (but see [Limits](reference.md#limits): a glibc-based image with `/bin/sh` and bash).

**Same software in every job.** The machine image gets exactly the versions of Snakemake and
its packages installed where you build it. Jobs that install Snakemake at start get the packages
as they were when the run started, so all jobs of a run match. Give images a version tag
(`samtools:1.21--h50ea8bc_0`, not `samtools` or `:latest`; the plugin warns otherwise): an
instance reuses an image it already has, so a moving tag can mean different contents on
different instances. Each job's log records the exact image it ran
(`[snakemake-batch] image ... id=sha256:...`).

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
setting up, or later with [Updating the machine image](setup.md#updating-the-machine-image). Jobs that
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
[Updating the machine image](setup.md#updating-the-machine-image).

### What a run used

At the end of a run the plugin prints what each rule used, from Batch's own start and stop times
(Spot retries included), and how much memory, CPU and disk its jobs really needed:

```
AWS Batch usage of this run (costs: AWS Billing, by the jobs' tags snakemake-run-id and snakemake-rule):
  rule         jobs    vCPU-h     GiB-h  vCPUs   busy   mem MiB    peak  disk GiB
  star_align      4      0.96      0.32      8    7.6      4071    1840       3.2
  fastp_pe        4      0.08      0.32      4    1.4      4071     310       0.4
  total           8      1.04      0.64
vCPUs, mem MiB: what a job had; busy: the most vCPUs in use at once (on EC2 a job can use idle vCPUs of its instance, so this can be more than it had); peak: the most memory its processes used (without file cache); disk GiB: the most its working folder took. Each the highest of the rule's jobs; memory sampled every 0.5 s (a shorter spike isn't seen), CPUs every 2 s.
Memory for each rule (its peak + 30%; mem_mb leaves out the plugin's own 256 MiB, added on top), to paste into the project's profiles/default/config.yaml (smkflex init makes it):
set-resources:
  fastp_pe:
    mem_mb: 300     # job gets 542 MiB; had 4071, peak 310
  star_align:
    mem_mb: 2300    # job gets 2449 MiB; had 4071, peak 1840
Threads for rules that never had most of theirs busy (the most in use at once + 25%; a burst shorter than 2 s isn't seen, so check the tool first):
set-threads:
  fastp_pe: 2     # had 4, at most 1.4 busy
```

- **Cost** is AWS's to say, about a day later. Everything the plugin starts is tagged
  `ManagedBy=snakemake-batch-flex`, which Cost Explorer can filter by once it's switched on under
  **Billing** > **Cost allocation tags**. Per run and per rule: every job is tagged
  `snakemake-run-id` (printed when the run starts) and `snakemake-rule`; with **split cost
  allocation data** for ECS switched on (Billing preferences), the Cost and Usage Report shares
  each instance's cost out to the jobs on it, by those tags.
- **Memory, CPU and disk:** each job samples its container (memory every half second, CPUs every
  2 seconds) and logs the most it saw: its processes' memory (from Linux's cgroup statistics, without file cache, which Linux
  frees before it kills anything), how many vCPUs were busy at once, and how big its working
  folder (inputs, outputs, conda downloads) got. The plugin reads those lines from the job logs
  (on runs with more than 300 jobs, from 25 per rule). A spike between two samples can be missed, which the retry with more memory
  covers. If Linux killed a process for lack of memory in a rule's jobs (a tool can survive
  that), the table says so and leaves that rule out of the suggestions, since what was seen is
  less than it needs. For a job that builds its conda environment, that build counts too (often 1-1.5 GB).
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

Turn the table off with `no-usage-summary`.

### Many small jobs

Every Batch job costs time on top of its command: Batch has to schedule it, start its container
and copy its files from and to the bucket. Measured on real runs, that's about 25 seconds on an
instance that's already running (15 to start, 10 for Snakemake in the job), and a minute or more
when a new instance has to start first. For a rule with thousands of jobs that
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
- **"can't open file" / "No such file" for a file in a job.** Every job gets the workflow
  folder `snakemake` runs in: what git would track (committed or not), or, outside git,
  everything `.gitignore` doesn't exclude, and `config/` always. `data/`, `results/`, `logs/`
  and `benchmarks/` never go along (jobs read and write those in storage), nor do single files
  over 10 MB or anything outside the folder. So check whether the file is git-ignored,
  big, or outside the folder. Data the workflow reads belongs in its inputs, in storage.
- **"Missing input files … (in storage)".** Jobs read their inputs from storage, not from your
  machine, so input data has to be uploaded once: `smkflex push data` (absolute paths too, e.g.
  `smkflex push /shared/refs`). When the file is on this machine, the message says so and gives
  the command.
- **"Every job gets the workflow folder, and it has … MB".** Data sitting in the folder would
  go to every job: add its folders to `.gitignore` (`smkflex init` adds `resources/`), or raise
  `aws-batch-flex-max-sources-mb`.
- **A job fails.** Snakemake prints the end of the job's log, and for causes the plugin recognises
  (out of memory, a full disk, a missing package or storage plugin, S3 permissions) a hint on what
  to do. Full logs are in the AWS console under **CloudWatch** > **Log groups** >
  `/aws/batch/smkflex` (`/aws/batch/` plus the `name` from `terraform.tfvars`; `/aws/batch/job`
  without the Terraform setup); the rule's own `log:` files are in the bucket (`smkflex pull logs`).
- **Where a job's error is.** The plugin shows the end of the job's own output and of the rule's
  `log:` files (most tools write their errors there, with `2> {log}`), fetched from the bucket.
- **A job fails with "403" or "Forbidden" from S3.** Jobs use the job role, not your login, and
  the Terraform setup only lets it use `s3://BUCKET/snakemake/`. Keep the project's folder inside
  it (which `smkflex init` does).
- **Every job of a rule fails at once with "SyntaxError: source code cannot contain null bytes"
  (or "... in file /bin/sh").** The rule's image has a Python `ENTRYPOINT` (such as
  `["python3"]`), and the plugin couldn't read it from the registry: it warns at the start of the
  run. Batch can't change an image's entrypoint, so the plugin has to know it: give the rule
  `batch_entrypoint='["python3"]'`, or for a private registry set `registry-credentials`.
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
