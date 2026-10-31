# Guide

Running workflows with the plugin, and tuning them. Setting up is in the
[README](../README.md#getting-started); every setting is in the [Reference](reference.md).

- [Running workflows](#running-workflows): [published pipelines](#running-a-published-pipeline),
  [coming from a cluster](#coming-from-a-cluster),
  [what works differently from a local run](#what-works-differently-from-a-local-run),
  [long runs with `smkflex`](#long-runs-smkflex),
  [interrupted runs](#interrupted-runs)
- [Tuning jobs](#tuning-jobs): [containers and Python](#containers-and-python),
  [conda](#conda-and-wrappers), [what a run used](#what-a-run-used),
  [many small jobs](#many-small-jobs), [Spot and Fargate](#spot-and-fargate-queues),
  [private images](#private-images), [secrets](#secrets)
- [Troubleshooting](#troubleshooting)

## Running workflows

### Running a published pipeline

Pipelines from the [Snakemake workflow catalog](https://snakemake.github.io/snakemake-workflow-catalog/)
run unchanged. Deploy one with [snakedeploy](https://snakedeploy.readthedocs.io) as the catalog
suggests (it's in the `smk-aws` environment), then treat it like your own workflow:

```bash
snakedeploy deploy-workflow https://github.com/snakemake-workflows/rna-seq-star-deseq2 my-rnaseq --tag v3.1.2
cd my-rnaseq
smkflex init                       # this project's own prefix in the bucket, and a .gitignore
# edit config/config.yaml and the sample sheets (config/samples.tsv, config/units.tsv) as the
# pipeline's docs say, then add what this pipeline needs to the project profile:
cat >> profiles/default/config.yaml <<'EOF'
software-deployment-method: conda
default-resources: [mem_mb=4000]
max-threads: 8
retries: 2
EOF
smkflex push data                  # the input files, at the paths the sample sheets use
snakemake --profile smkflex        # or: smkflex run --profile smkflex
```

These four settings are what this pipeline would need on any cluster: its rules use conda; most
don't set their memory, so `default-resources` applies (otherwise the plugin's default of
2048 MiB); STAR asks for 24 threads, which `max-threads` caps; and `retries` covers jobs that
run out of memory (retried with twice the memory) and flaky downloads. Other pipelines need
other values: their docs say which, and after a first run the end-of-run table suggests memory
for each rule.

Tested with [rna-seq-star-deseq2](https://github.com/snakemake-workflows/rna-seq-star-deseq2) on
its yeast test data, set up as above (v3.1.2): 53 jobs, 13 minutes, about $0.10 of compute.
Without the thread cap it was $0.28, because STAR's 24-thread jobs needed big instances and ran
out of memory once.

- **Packages a pipeline's Snakefile imports**, other than pandas, have to be in the machine image
  (see [Containers and Python](#containers-and-python)).
- **If the pipeline ships its own profile** (`workflow/profiles/default/`), `smkflex init`
  adds to that one, since Snakemake loads only one default profile.
- **A pipeline's own tests** (`.test/` in its repository) are run with `--directory .test`. That
  doesn't work with remote storage, because Snakemake only sends jobs the source files inside
  the folder it starts in. Copy the test config into `config/` instead.
- **git, curl, wget.** For a pipeline loaded from GitHub, Snakemake keeps a git clone, so `git`
  has to be wherever the Snakefile is parsed: on your machine, in the `smkflex` head job (its
  image has it), and in every job. Wrappers and scripts also call `curl` or `wget` without
  declaring them. On EC2, jobs whose image lacks these tools use the AMI's copies, which come
  after the image's own on the `PATH` (AMIs built by older versions of the plugin lack them;
  rebuild with `scripts/update-ami.sh`). On Fargate, the rules' images need their own.
- **Private pipeline repositories** need `GITHUB_TOKEN` set where Snakemake runs and in the jobs.
  Give it to the jobs with `aws-batch-flex-secrets`, not `--envvars`, which sends it as plain
  text (see [Secrets](#secrets)).

`smkflex run --git URL --add config/` does the same without `snakedeploy`: the code comes from a
clone of the repository and your `config/` is laid over it (see [Long runs](#long-runs-smkflex)).

### Coming from a cluster

On a cluster, every job sees the same shared filesystem. On AWS, jobs run on instances that come
and go, and files reach them through S3: each job downloads its inputs and uploads its outputs,
as with any remote storage provider. Most of a pipeline carries over unchanged; these are the
parts that don't.

**Paths and data.** Your files have to be in the bucket, under the project's prefix (which
`smkflex init` sets). Relative paths map straight onto it: `data/S1.fq` is
`s3://BUCKET/snakemake/my-project/data/S1.fq`. So do absolute paths: if your config has
`genome: /shared/refs/hg38.fa`, Snakemake looks for
`s3://BUCKET/snakemake/my-project/shared/refs/hg38.fa`. A config full of cluster paths can
therefore stay as it is, and `smkflex push` uploads each path to where Snakemake will look for
it:

```bash
smkflex push data             # a relative path, from the project folder
smkflex push /shared/refs     # an absolute path, e.g. run on the cluster where the data is
```

It uploads 16 files at a time (big files in parallel parts), about as fast as `aws s3 sync`, and
skips files that are already there and unchanged (same size, not modified since the upload), so
an interrupted upload just continues.

Data that's already in S3, e.g. from a sequencing provider, doesn't need copying: in the rule,
refer to it as `storage("s3://their-bucket/run1/S1.fq")`. The job role then needs read access to
that bucket, which the Terraform setup doesn't give it (add a policy with `s3:GetObject` and
`s3:ListBucket` for it to the job role).

**Resources.** Most SLURM resources have a direct counterpart; put them in the project profile,
`profiles/default/config.yaml`:

| SLURM (cluster profile) | Here (project profile or rule) |
|---|---|
| `threads`, `cpus_per_task` | `threads`: vCPUs for the job |
| `mem_mb`, `mem_mb_per_cpu` | `mem_mb`; for `mem_mb_per_cpu`, use the per-CPU value × threads |
| `runtime` | `runtime`: minutes, after which Batch stops the job |
| `slurm_partition` | `batch_queue`: a queue name, e.g. the Spot or Fargate queue |
| `slurm_account`, `qos` | nothing to set; `batch_tags` can label jobs for cost reports |
| `gpu`, `gres` | `gpu`: number of GPUs (needs a GPU queue; untested) |
| `constraint` (node types) | instance types, set in Terraform (`instance_types`); send rules to a queue with them via `batch_queue` |
| `nodes`, `tasks` (MPI) | not supported: each job runs in one container on one instance |
| `slurm_extra` | the `batch_*` resources, or your own job definition (`batch_job_definition`) |
| `--jobs` | `jobs`: how many Batch jobs to have queued or running at once |
| local scratch, `tmpdir` | the instance's disk, shared by its jobs (`root_volume_gib`, 100 GiB by default); `disk_mb` on Fargate |

**Software.** There's no module system and no cluster-wide software (`/apps/...`): use a
container image per rule (e.g. from biocontainers) or a `conda:` env instead. Conda envs are
built by the jobs: the first job on each instance builds an env and the others reuse it, and
with the S3 cache later runs reuse it too, much like a cluster's shared `.snakemake/conda` (see
[Conda and wrappers](#conda-and-wrappers)).

**Big intermediate files.** Without a shared filesystem, a 30 GB BAM that ten rules read is
downloaded ten times (within AWS, so it's fast and free, but it takes time). Grouping the rules
that use it into one job (`group:`, see [Many small jobs](#many-small-jobs)) avoids that, as do
`temp()` files that never leave a job.

**Where Snakemake runs.** On a cluster you'd start Snakemake in `tmux` on the login node and log
out; `smkflex run` does the equivalent here (see [Long runs](#long-runs-smkflex)). `localrules`
run wherever Snakemake runs: your machine, or the `smkflex` head job.

**Logs.** Snakemake prints the end of a failed job's log; rule `log:` files are in the bucket
(`smkflex pull logs`), and the full job output in CloudWatch (see
[Troubleshooting](#troubleshooting)).

### What works differently from a local run

Jobs run on other instances and share only the bucket, so a few things that work in a local run
need a change:

- **Input data has to be in the bucket.** Upload it once with `smkflex push data` (see
  [Paths and data](#coming-from-a-cluster)); a "Missing input files … (in storage)" error lists
  the files that exist only on your machine.
- **Results are in the bucket, not on your machine.** Code that runs in the Snakemake process and
  opens an output, such as an `onsuccess:` handler that prints a summary, finds no file. Do it in
  a rule that takes the file as input (a `localrules:` rule if it should run on your machine), so
  Snakemake fetches it, or download the results after the run with `smkflex pull results`.
- **Jobs get the workflow folder,** except `data/`, `results/`, `logs/`, `benchmarks/`, anything
  git-ignored, files over 10 MB and anything outside the folder. `config/` is always sent, and
  so is any file under 10 MB that a config value names (a sample sheet in `data/`, a gene list
  the pipeline's `.gitignore` excludes), since scripts often open those by path. Any other file
  a rule reads without declaring it as an input has to be in the folder (see
  [Troubleshooting](#troubleshooting)).
- **A job only gets the inputs its rule declares.** Files a tool expects next to an input, such
  as a genome's index (`genome.fa.bwt`, `genome.fa.fai`, `genome.dict`) or a BAM's `.bai`, aren't
  fetched unless the rule lists them: `input: multiext("genome.fa", ".amb", ".ann", ".bwt",
  ".pac", ".sa")` for bwa, or `bai="x.bam.bai"`. When the tool's error message is a known one,
  the failed job's error says so.
- **Only declared outputs are kept.** When a job ends, it uploads its rule's declared outputs
  (and `log:` and `benchmark:` files) to the bucket; anything else it wrote is lost with the
  container. On a shared filesystem, a later rule can read a file that an earlier rule wrote
  without declaring it; here that file is missing. Tools that write several files, such as an
  index (STAR, bwa, salmon) or a report folder, are the usual cases: declare each file, use
  `multiext()`, or declare the folder with `directory()`. The later rule then has to name it
  as an input.
- **Jobs see files at the same paths as on your machine,** as with a shared filesystem. A job
  works in a folder at the same path as Snakemake does, and keeps its copies of the bucket's
  files at their plain paths (`results/x.bam` there, an absolute `/data/x` at `/data/x`), not
  under `.snakemake/storage/`. So a tool finds an input however the Snakefile passes it:
  `{input}`, a param (`--genomeDir {params.index}`), or a path built with `os.path.abspath()`.
  Inputs named by an absolute path are uploaded by that path: `smkflex push $PWD/input_files`.
  `smkflex run` also unpacks the workflow at the same path as your project folder (except with
  `--git`, which clones into a temporary folder), so such paths agree in its head job. Where the
  image doesn't allow that folder (it runs as a non-root user, or already has files there), the
  job works under `/tmp`, and such pipelines fail as they would with any cloud executor.
- **No `workdir:`.** Jobs need the Snakefile inside the folder Snakemake works in, so the run
  stops at the start and says so. Outputs go under the storage prefix anyway, so remove it.
- **`threads: workflow.cores`** needs `--cores N`, as on a cluster (Snakemake asks for it). Each
  such job then requests N vCPUs, so N has to fit on one instance of the queue.
- **`envmodules:`** is ignored, so the tool it would load is missing: use `container:` or
  `conda:` (see Software above).

### Long runs: `smkflex`

`snakemake` has to keep running until the workflow finishes, so a laptop that sleeps or loses
its connection stops the run (jobs already on Batch carry on, and are cancelled when you
restart). For long workflows, `smkflex run` runs Snakemake itself as a Batch job, the "head
job". It's optional: running `snakemake` from your own machine works as before, and you can use
both on the same workflow.

```bash
cd my-workflow
smkflex run --profile smkflex    # returns once the run is submitted
smkflex logs JOB_ID --follow     # Snakemake's output, live
smkflex status                   # recent runs
smkflex cancel JOB_ID            # stops the run; Snakemake cancels its jobs
```

`smkflex run` packs the workflow folder (the same files jobs get: what git would track, or else
everything `.gitignore` doesn't exclude, plus `config/`, never `data/`, `results/`, `logs/` or
`benchmarks/`) together with the profile, uploads it under the project's prefix (the head job
deletes the upload when it's done), and submits the head job, which runs `snakemake` there. Once
the command returns, your machine can be switched off. Snakemake options go after `--`, e.g.
`smkflex run -- --forceall all`; `--wait` stays attached and prints the log until the run ends.

- `smkflex run --git https://github.com/lab/rnaseq --rev v2.1` runs a workflow straight from a
  git repository instead of the current folder. `--rev` is a branch, tag or commit (default: the
  default branch); `--path` is a subfolder to run from (only its contents are sent; repositories
  in the standard layout, with `workflow/Snakefile`, don't need it). The repository is cloned
  into a temporary folder on your machine, so private repositories work with your usual git
  login and your current folder isn't touched; the profile still comes from here (`--profile`).
  The run is named after the repository.
- `--add PATH` sends a local file or folder as well, at the same path, e.g. `--add config/` to
  lay your own config and sample sheets over a `--git` repository (files with the same name
  replace the repository's; with `--path`, paths are relative to that folder). It also sends
  files that `.gitignore` would leave out. Repeatable.
- Each run records the commit it ran: for a local git folder, with `-modified` if it had
  uncommitted changes or untracked files that aren't git-ignored, and `+added` with `--add`.
  `smkflex status` shows it.
- The `.snakemake` metadata (provenance records, incomplete markers, logs; also with
  `--persistence-backend db`) is saved to the bucket at the end of every run and restored at the
  start of the next, so reruns behave as they do on one machine. Like Snakemake's directory lock,
  only one run per project (storage prefix) is allowed at a time: a second `smkflex run` while
  one is active is refused. To add samples, add them to the sample sheet and run again once the
  first run has finished or been cancelled (finished outputs are kept); for an unrelated set of
  samples, `smkflex init` a new folder.
- The head job runs in `buildpack-deps:bookworm-scm` (Debian 12 with git; `--image` for
  another) and uses 1 vCPU and 2 GiB (`--vcpus`, `--mem-mb`) for as long as the workflow runs,
  on the profile's queue (`--queue` for another). Don't put it on a Spot queue.
- It needs `default-storage-prefix` and `aws-batch-flex-head-role-arn` in the profile; the
  profile from `setup.sh` has both.
- The head job uses the plugin version installed where you run `smkflex`, and the AMI's
  Snakemake (with `bootstrap: host`) or one installed when it starts.

All `smkflex` commands are listed under [The smkflex command](reference.md#the-smkflex-command).

### Interrupted runs

If `snakemake` is killed or loses its connection, its Batch jobs keep running, and a restart
would submit them again. Every job is tagged with `snakemake-workflow` (a hash of the working
directory, Snakefile and storage prefix), so on restart the plugin finds jobs from the earlier
run that are still active and cancels them before submitting anything.
`--aws-batch-flex-orphan-jobs warn` only lists them (the default with `--nolock`), and `ignore`
skips the check. If Snakemake then reports `IncompleteFilesException`, run again with
`--rerun-incomplete`.

## Tuning jobs

### Containers and Python

Each rule runs in its own image and Snakemake comes from the AMI, so images need nothing extra
beyond a glibc base with `/bin/sh` and bash (see [Limits](reference.md#limits)).

**Same software in every job.** The AMI gets exactly the versions of Snakemake and its packages
installed on the machine you build it from. Jobs that install Snakemake at start get the
package versions as they were when the run started, so all jobs of a run match. Pin images to a
version tag (`samtools:1.21--h50ea8bc_0`, not `samtools` or `:latest`; the plugin warns
otherwise): an instance reuses an image it has already pulled, so a moving tag can mean
different contents on different instances. Each job's log records the exact image digest it ran
(`[snakemake-batch] image ... id=sha256:...`).

**Python `script:` rules** run with the image's own `python` if it's 3.10 or newer, so they can
use the image's packages. The exception is a rule whose `params` hold pandas, numpy or polars
objects that the image's Python can't import: its script runs in Snakemake's Python instead, and
the job's log says so. Without a suitable Python in the image, and for `run:` blocks, the code
runs in Snakemake's own Python from the AMI, which has only Snakemake's dependencies; call such
scripts from `shell:` instead, or add their packages to the AMI as below.
`batch_script_python="host"` on a rule (or `aws-batch-flex-script-python: host` for all rules)
always uses Snakemake's Python.

**Python packages your Snakefile imports.** Every job parses the Snakefile again, so a package
that the Snakefile (or an `include:`d file) imports at the top level, such as `pandas`, is needed
in the jobs as well as where you run `snakemake`. Many published pipelines do this. `setup.sh`
puts pandas into the AMI; add others with `--extra-package NAME` at setup, or later as described
in [Updating the machine image](setup.md#updating-the-machine-image). Jobs that install
Snakemake at start (Fargate, `bootstrap: auto`) take `aws-batch-flex-extra-packages: [pandas]`
in the profile instead. A job that fails this way names the missing package.

### Conda and wrappers

Set `software-deployment-method: conda` (or run with `--sdm conda`). Jobs need `conda`: either
the image has it, or the AMI has it (`setup.sh` installs Miniforge by default) and the profile
sets `aws-batch-flex-conda-path: /opt/miniforge3`. Only the AMI's `condabin` is added to the
job's `PATH`, so an image's own conda takes precedence. As usual, the machine you run
`snakemake` on needs `conda` too. Wrappers work the same way; the plugin passes jobs an HTTPS
`--wrapper-prefix`, so images don't need git.

Jobs build envs with strict channel priority (`CONDA_CHANNEL_PRIORITY=strict`), which bioconda
requires. Miniforge is configured that way, but an image's own conda often isn't, and can then
mix packages from different channels that don't work together. If an env solves differently on
AWS than on your machine, check `conda config --show channel_priority` locally: with `flexible`
or `disabled`, your machine can pick packages that strict priority won't.

Each job builds the envs its rule needs, which takes a minute or more per env. To do this once
rather than in every job, built envs are reused. `aws-batch-flex-conda-cache` has three
settings, and the S3 one adds to the default rather than replacing it:

- **`instance`** (the default): jobs on the same EC2 instance share an env folder, so the first
  job that needs an env builds it and the others on that instance reuse it (a job that needs
  an env another job is still building waits for it). The cache is lost when the instance shuts
  down, so the next run, or a new instance, builds the env again. Fargate jobs don't share it:
  each runs alone.
- **An S3 URL** (`s3://BUCKET/snakemake/.smkflex/conda-cache`): the same sharing on each
  instance, plus a copy in S3. Before building an env, a job looks on its instance, then in S3,
  and builds it only if neither has it, in which case it also uploads it. Later jobs on any
  instance, later runs, other projects and Fargate jobs then unpack it in seconds. It's off by
  default because it stores the envs in your bucket (typically 0.1-2 GB each, a few cents a
  month); `setup.sh` writes this line into the profile commented out, so to turn it on,
  uncomment it.
- **`none`**: every job builds its own envs.

On rna-seq-star-deseq2's test data:

| Setting | Env builds | Run time |
|---|---|---|
| `none` | 52 | 18-21 minutes |
| `instance` (default) | 12 | 11 minutes |
| S3 URL, second run | 0 | 7.5 minutes |

An env is reused only for the same definition (the YAML file, its pinned versions, the wrapper
version) and for images with the same glibc version, which conda resolves packages against, so
a cached env never differs from one the job would have built. Editing an env file gives it a new
hash, and the old cached env is simply no longer used; delete `.smkflex/conda-cache/` in the
bucket to clear the cache. Anyone who can write to the bucket could replace a cached env, just
as they could replace the workflow's files. `none` turns caching off.

Envs can also be baked into the AMI and used by name (`conda: "rnaseq"`); see
[Updating the machine image](setup.md#updating-the-machine-image).

### What a run used

At the end of a run the plugin prints what each rule used, from Batch's own start and stop times
(Spot retries included), and how much memory, CPU and disk its jobs actually needed:

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

- **Cost** comes from AWS Billing, about a day later. Everything the plugin starts is tagged
  `ManagedBy=snakemake-batch-flex`, which Cost Explorer can filter by once the tag is activated
  under **Billing** > **Cost allocation tags**. For cost per run and per rule, every job is also
  tagged `snakemake-run-id` (printed when the run starts) and `snakemake-rule`; with **split
  cost allocation data** for ECS turned on (Billing preferences), the Cost and Usage Report
  divides each instance's cost among the jobs on it, by those tags.
- **Memory, CPU and disk.** Each job samples its container (memory every 0.5 s, CPU every 2 s)
  and logs the maximum it saw: its processes' memory (from cgroup statistics, excluding page
  cache, which the kernel reclaims before it OOM-kills anything), how many vCPUs were busy at
  once, and how large its working folder (inputs, outputs, conda downloads) grew. The plugin
  reads those lines from the job logs (on runs with more than 300 jobs, from a sample of 25 per
  rule). A spike between two samples can be missed; the OOM retry with more memory covers that.
  If the kernel OOM-killed a process in a rule's jobs (a tool can survive that), the table says
  so and leaves the rule out of the suggestions, since the observed peak understates what it
  needs. For a job that builds its conda env, the build counts too (often 1-1.5 GB). The
  `set-resources:` block lists every rule whose peak is known, at its peak plus 30% (at least
  512 MiB per job): paste it into the project profile so the next run neither wastes space on
  the instances nor runs out. On rna-seq-star-deseq2's test data the rules used 0.3-2.7 GB of
  the 4 GB they requested, and ran fine with the suggested values.
- **Threads.** Rules that requested 2 or more vCPUs but never had more than 60% of them busy get
  a `set-threads:` suggestion (the peak in use plus 25%). Pipelines written for large cluster
  nodes often request more threads than their tools use, and on Batch that means bigger,
  half-idle instances. A tool that uses its threads only in bursts shorter than 2 seconds can
  look idle, so check before lowering.
- **Disk.** A rule whose jobs used more than 20 GiB gets a hint, since on EC2 the jobs on one
  instance share its disk, and on Fargate a job has 20 GiB unless `disk_mb` requests more.

Turn the table off with `aws-batch-flex-no-usage-summary`.

### Many small jobs

Every Batch job has overhead on top of its command: Batch has to schedule it, start its
container, and copy its files from and to the bucket. Measured on real runs, that's about
25 seconds on an instance that's already running (15 to start, 10 for Snakemake in the job), and
a minute or more when a new instance has to start first. For a rule with thousands of jobs that
take a few seconds each, that overhead dominates the run. Snakemake's job grouping puts many of
them into one Batch job:

```python
rule index_reads:
    input: "reads/{sample}.fq"
    output: "index/{sample}.idx"
    group: "index"
    shell: "..."
```

Then run with `--group-components index=50` (or `group-components: [index=50]` in the project
profile), and each Batch job runs 50 of them. A group runs in one container, so its rules must
use the same image; how Snakemake sums their CPU and memory is described in its
[grouping docs](https://snakemake.readthedocs.io/en/stable/executing/grouping.html). When a rule
ran at least 20 jobs with a median runtime under a minute, the end-of-run table suggests this.

### Spot and Fargate queues

**Spot instances** are AWS's spare capacity: the same instances, usually 60-90% cheaper, but AWS
can reclaim one with two minutes' warning, which kills the job on it. Batch then restarts the
job on another instance (`spot-retries`, 2 by default; ordinary failures aren't retried this
way).
`setup.sh --spot` (or `enable_spot_queue = true` in `terraform.tfvars`) creates a second queue,
`<name>-spot-queue`, next to the normal one; send rules to it:

```python
rule call_variants:
    resources: batch_queue="smkflex-spot-queue"
```

Or make it the profile's `aws-batch-flex-queue` and send the rest to the on-demand queue. Keep
very long jobs (a restart begins from scratch) and `smkflex` head jobs (losing one stops the
run) on the on-demand queue.

**Fargate** runs each job without instances to manage, and bills per job; `setup.sh --fargate`
creates a Fargate queue. Jobs there install Snakemake when they start (10-15 s), get their
scratch disk from `disk_mb`, and are limited to 16 vCPUs and 120 GiB. Their images need `conda`
for `conda:` rules (the S3 conda cache saves them building envs) and `git` for pipelines loaded
from GitHub. EC2 and Fargate queues can be mixed in one run with `batch_queue`.

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

Only images from that registry get the credentials. ECS reads the secret with the execution
role, so that role is needed on EC2 too, and it needs `secretsmanager:GetSecretValue` on the
secret.

### Secrets

Environment variables from `envvars:` or `--envvars` go to every job as plain text: anyone who
can see the account's Batch jobs (the console, `aws batch describe-jobs`) or CloudTrail can read
them. The plugin warns at start when one looks like a secret (`..._TOKEN`, `..._KEY`, `PASSWORD`).

For a token or password, store it as an SSM parameter (SecureString, free) or a Secrets Manager
secret, named under `smkflex/`, and map the variable to it in the profile:

```bash
aws ssm put-parameter --name /smkflex/github-token --type SecureString --value "$GITHUB_TOKEN"
```

```yaml
aws-batch-flex-secrets: [GITHUB_TOKEN=arn:aws:ssm:eu-west-1:123456789012:parameter/smkflex/github-token]
aws-batch-flex-execution-role-arn: arn:aws:iam::123456789012:role/...   # Terraform prints it
```

Only the ARN reaches Batch: ECS reads the value with the execution role when each job starts.
Set `enable_secrets = true` in `terraform.tfvars` to have Terraform create that role (used on
EC2 too) and allow it to read parameters and secrets under `smkflex/` (`<name>/`), and nothing
else. The `smkflex run` head job gets them too. If the Snakefile declares the variable
(`envvars:`), Snakemake still requires it to be set where you start it; the jobs get only the
ARN. A job definition of your own (`aws-batch-flex-job-definition`) doesn't get them: set its
`secrets` yourself.

## Troubleshooting

- **Not sure what's wrong?** Run `smkflex check --profile smkflex` in your workflow folder. It
  checks your AWS login, the queue, the roles, that it can write to the bucket, and the
  Snakemake versions, and says what to fix.
- **"Startup check failed".** The plugin checks the queue before submitting anything; the
  message says what's wrong (usually a typo in the queue name, or the wrong region).
- **Jobs stay `RUNNABLE` for more than ~10 minutes.** No instance could start. Check the compute
  environment in the AWS console (**Batch** > **Compute environments**) for an error, and that
  your subnets have internet access. A job that asks for more vCPUs or memory than the largest
  instance type allowed also waits forever. New accounts sometimes have a low EC2 vCPU quota.
  The plugin warns when a job has waited longer than `runnable-warn-minutes` (15).
- **"can't open file" / "No such file" for a file in a job.** Every job gets the workflow
  folder `snakemake` runs in: what git would track (committed or not), or, outside git,
  everything `.gitignore` doesn't exclude, and always `config/`. `data/`, `results/`, `logs/`
  and `benchmarks/` are never sent (jobs read and write those in storage), nor are files over
  10 MB or anything outside the folder. So check whether the file is git-ignored, large, or
  outside the folder. Data the workflow reads belongs in its declared inputs, in storage.
- **"links to files outside the workflow folder".** Jobs get symlinks as symlinks, so a sample
  sheet linked in from elsewhere would point at nothing; the run stops before submitting
  anything. Copy the file in instead (`cp --remove-destination "$(readlink -f F)" F`). A
  relative link to a file inside the folder is fine, and `smkflex run` sends links' targets.
- **"Missing input files … (in storage)".** Jobs read their inputs from storage, not from your
  machine, so input data has to be uploaded once: `smkflex push data` (absolute paths too, e.g.
  `smkflex push /shared/refs`). When the file is on this machine, the message says so and gives
  the command.
- **"Every job gets the workflow folder, and it has … MiB".** Data sitting in the folder would
  be sent to every job: add its folders to `.gitignore` (and upload what the rules read as
  inputs with `smkflex push`), or raise `aws-batch-flex-max-sources-mb`. `resources/` is sent
  unless you ignore it, because pipelines keep small files there that their scripts read
  directly.
- **A job fails.** Snakemake prints the end of the job's output and of the rule's `log:` files
  (where most tools write their errors, with `2> {log}`), and for causes the plugin recognises
  (out of memory, a full disk, a missing package or storage plugin, S3 permissions) a hint on
  what to do. Full job output is in the AWS console under **CloudWatch** > **Log groups** >
  `/aws/batch/smkflex` (`/aws/batch/` plus the `name` from `terraform.tfvars`; `/aws/batch/job`
  without the Terraform setup); the rule's `log:` files are in the bucket (`smkflex pull logs`).
- **A script fails at once with "StringDtype.__init__() takes from 1 to 2 positional arguments".**
  Snakemake runs with pandas 3 and the rule's env (or wrapper) has pandas 2, which can't load a
  pandas 3 object in the rule's `params`. The plugin converts pandas arrays (a text column's
  `.values`) to numpy arrays, as pandas 2 returned them, but not a pandas `Index` or pandas
  objects nested in a list or dict. Give the rule's env pandas 3 (for a wrapper, use a newer
  wrapper version), or pass plain lists in `params` (`.tolist()`).
- **A job fails with "403" or "Forbidden" from S3.** Jobs use the job role, not your login, and
  the Terraform setup only grants it `s3://BUCKET/snakemake/`. Keep the project's prefix inside
  it (as `smkflex init` does), and see [Paths and data](#coming-from-a-cluster) for other
  buckets.
- **Every job of a rule fails at once with "SyntaxError: source code cannot contain null bytes"
  (or "... in file /bin/sh").** The rule's image has a Python `ENTRYPOINT` (such as
  `["python3"]`), and the plugin couldn't read it from the registry: it warns at the start of the
  run. Batch can't change an image's entrypoint, so the plugin has to know it: give the rule
  `batch_entrypoint='["python3"]'`, or for a private registry set `registry-credentials`.
- **"Ignoring source file ../workflow/...".** Snakemake only sends jobs the source files inside
  the folder you start it from, so start it from the top of the pipeline, not with `--directory`
  pointing at a subfolder. Published pipelines' tests often use `--directory .test`; copy the
  test config into `config/` and run from the top instead.
- **A dry run (`-n`) lists jobs for a finished workflow** with "software environment definition
  has changed". That's a Snakemake quirk with `conda:` rules: a dry run treats the jobs as local
  and computes their env hashes, which a real run (where the jobs build the envs on AWS) doesn't
  record. The real run reports "Nothing to be done". `-n --rerun-triggers mtime params input
  code` shows what the real run will do.
- **`IncompleteFilesException` after Snakemake was stopped.** Run again with
  `--rerun-incomplete`. Jobs left running by the stopped run are cancelled automatically (see
  [Interrupted runs](#interrupted-runs)).
