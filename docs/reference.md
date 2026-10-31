# Reference

Every setting and command, the IAM permissions, what has been tested and the limits. The
[README](../README.md) has the setup and per-rule options; the [Guide](guide.md) explains how to
use them.

- [Profile settings](#profile-settings)
- [The smkflex command](#the-smkflex-command)
- [IAM](#iam)
- [What's been tested](#whats-been-tested)
- [Limits](#limits)

## Profile settings

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
| `head-role-arn` | *none* | The IAM role of the head job, the Batch job in which `smkflex run` runs Snakemake (see [Long runs](guide.md#long-runs-smkflex)). Only `smkflex` uses it. |
| `default-image` | *none* | The image for rules that have no `container:`. The setup writes `buildpack-deps:bookworm-scm` (Debian 12 with curl, wget and git, as a workstation has them): pipelines often call such tools, or a wrapper does, without a container or conda package for them. |
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
| `install-prereqs` | `false` | When installing at start, for images that have none of `curl`, `wget` or `pip`: get uv with the image's conda, mamba or micromamba (any user), or else install `curl` with its package manager (images that run as root). |
| `script-python` | `image` | Which Python runs Python `script:` rules. `image`: the image's own, if it's 3.10 or newer, so the script can use the image's packages (but Snakemake's when the rule's pandas, numpy or polars `params` can't be loaded with it). `host`: always Snakemake's own. |

**Memory, disk and Spot**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `mem-overhead-mib` | `256` | Memory added to each job on top of the rule's `mem_mb`, for the Snakemake process that runs in the job. |
| `oom-memory-factor` | `2` | When a job runs out of memory and Snakemake retries it (`retries:`), the retry gets this many times the memory, and again on each further retry (on Fargate, up to 120 GiB). `1` retries with the same memory. A rule that raises its own memory with `attempt` (`mem_mb=lambda wildcards, attempt: 4000 * attempt`) gets what it asks for, not that times this too. |
| `spot-retries` | `2` | How many times Batch restarts a job when AWS takes its Spot machine back. `0`: never. |
| `ephemeral-storage-gib` | *none: jobs get 20 GiB* | Fargate only: disk space for every job, in GiB: a whole number from 21 to 200, e.g. `50`. Without it, each job has the 20 GiB Fargate always gives. A rule's `disk_mb` can ask for more for its own jobs. (On EC2, jobs share their machine's disk; its size is set in Terraform.) |
| `assign-public-ip` | `ENABLED` | Fargate only: `ENABLED` gives jobs a public IP address, which they need to reach the internet (images, S3) unless the subnet has a NAT gateway. `DISABLED` for subnets that do. |

**Conda**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `conda-path` | *none* | Where the machine image has conda (`setup.sh` sets it), so `conda:` rules also work in images without conda. EC2 only. |
| `conda-cache` | `instance` | Reuse conda environments instead of building them in every job. `instance`: jobs on the same machine share them. An S3 URL, e.g. `s3://BUCKET/snakemake/.smkflex/conda-cache`: they're also kept there for later jobs and runs. `none`: build in every job. See [Conda and wrappers](guide.md#conda-and-wrappers). |

**Logs, checks and leftovers**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `log-group` | `/aws/batch/job` | The CloudWatch log group for job logs. It must already exist. Terraform creates one that deletes logs after 30 days and puts it in the profile; Batch's default group keeps them forever. |
| `no-usage-summary` | `false` | Don't print the table of what the run used at the end (see [What a run used](guide.md#what-a-run-used)). |
| `max-sources-mb` | `100` | Every job gets the workflow folder (what git would track, or everything `.gitignore` doesn't exclude, plus `config/`; never `data/`, `results/`, `logs/` or `benchmarks/`, and no single file over 10 MB). Above this many MB the run stops with a message saying which folders to put in `.gitignore`: data belongs in storage. |
| `runnable-warn-minutes` | `15` | Warn when a job has waited this long for a machine to run on. |
| `skip-preflight` | `false` | Skip the checks of the queue and roles at the start of a run. |
| `orphan-jobs` | `cancel` | What to do with jobs still running from an earlier run that was killed: `cancel`, `warn` or `ignore` (see [Interrupted runs](guide.md#interrupted-runs)). |
| `no-auto-tags` | `false` | Don't tag jobs with their run, rule and so on. Without those tags, jobs left over from a killed run can't be found. |

**Less common**

| Setting (`aws-batch-flex-...`) | Default | What it's for |
|---|---|---|
| `platform` | *read from the queue* | `ec2` or `fargate`. Only needed if it can't be read from the queue. |
| `volumes` | *none* | Folders on the EC2 machine to make available in every job, as `MACHINE_PATH:JOB_PATH`, with `:ro` for read-only, e.g. `[/data/refs:/refs:ro]`. |
| `scheduling-priority` | *none* | Priority for every job (0-9999), for queues with a fair-share scheduling policy. Other queues ignore it, with a warning. |
| `registry-credentials` | *none* | `REGISTRY=SECRET_ARN` for private registries outside AWS (see [Private images](guide.md#private-images)). |
| `secrets` | *none* | `NAME=ARN` of an SSM parameter or Secrets Manager secret: environment variables jobs get without the value passing through Batch (see [Secrets](guide.md#secrets)). Needs `execution-role-arn`. |
| `job-definition` | *none* | Use an existing Batch job definition (name, `name:revision` or ARN) instead of the ones the plugin makes, e.g. where you aren't allowed to make them. It decides the image, job role, mounted folders and `/dev/shm`; vCPUs, memory, timeout and tags still come from the rules. If it doesn't mount the machine image's Snakemake, jobs install it when they start. |
| `job-definition-prefix` | `snakemake` | How the job definitions the plugin makes are named. |

Every job is also tagged with `snakemake-run-id`, `snakemake-workflow`, `snakemake-rule` and
`snakemake-job-id`, so you can find a run's jobs, or a rule's, in the Batch console and in billing,
and always with `ManagedBy=snakemake-batch-flex` (even with `no-auto-tags`), which the IAM policy
needs to let you stop them.

## The smkflex command

`smkflex` comes with the plugin. Without `--profile`, it uses the only profile for this plugin in
`~/.config/snakemake/`; `smkflex COMMAND --help` lists each command's options.

| Command | What it does |
|---|---|
| `smkflex check` | Checks the setup: AWS login, queue, roles, bucket, and that the versions fit together. |
| `smkflex init` | Sets up the current folder as a project: its own folder in the bucket, and a `.gitignore` (see [Your own workflow](../README.md#your-own-workflow)). |
| `smkflex push PATH...` | Uploads input files or folders to where the rules look for them, e.g. `smkflex push data /shared/refs`. |
| `smkflex pull PATH...` | Downloads outputs from the bucket into the current folder, e.g. `smkflex pull results logs` (`--to` puts them elsewhere). |
| `smkflex run [-- OPTIONS]` | Runs the workflow on AWS in a head job, so your machine can be switched off (see [Long runs](guide.md#long-runs-smkflex)). Snakemake options go after `--`, e.g. `smkflex run -- --forceall`. `--git URL` runs a workflow from a git repository; `--wait` waits for the run to finish. It prints the run's JOB_ID. |
| `smkflex status` | Lists recent runs started with `smkflex run`: their JOB_ID, state and the commit each one ran. |
| `smkflex logs JOB_ID` | Prints a run's Snakemake output; `--follow` keeps watching. |
| `smkflex cancel JOB_ID` | Stops a run. Snakemake cancels its jobs first. |

## IAM

The identity running `snakemake` needs `batch:SubmitJob`, `DescribeJobs`, `TerminateJob`,
`RegisterJobDefinition`, `DescribeJobDefinitions`, `DescribeJobQueues`, `ListJobs`,
`DescribeComputeEnvironments` and `TagResource`, plus `logs:GetLogEvents`, `iam:PassRole` on the
job role(s), and read/write on the S3 prefix. Optionally `ec2:DescribeImages` and
`ec2:DescribeLaunchTemplateVersions`, so the plugin can warn when the AMI's Snakemake version is
far from yours. `terraform output launcher_policy_json` prints a
policy with all of this, limited to what the plugin makes, so it can't touch other Batch work in
the account: it registers only `snakemake-*` job definitions, and never privileged ones (root on the instance), stops only jobs tagged
`ManagedBy=snakemake-batch-flex` (the plugin and `smkflex` tag every job with it), and reads only
the plugin's log group. The job role needs read/write on the prefix. For `smkflex`, the head
job's role needs the same as the identity running `snakemake`, and that identity also needs
`iam:PassRole` on the head role (both are in the Terraform setup).

## What's been tested

Run on real AWS (eu-west-1): `shell:`, `run:`, Python/bash/R `script:` (Python scripts in the
image's own Python), `wrapper:`, conda (YAML in a conda image, YAML and named envs in conda-less
images), a different image per rule, a 6-image variant-calling pipeline, EC2 and Fargate
(separately and mixed in one run), checkpoints, groups, shadow, benchmark/log, retries, `temp()`,
`directory()`, resources and tags, OOM and timeout reporting, cancelling with Ctrl-C, startup
checks, fan-out to 100 jobs, install-at-start on a bare `debian:12`, cancelling jobs left over
from an interrupted run, the CPU and disk figures in the usage table, stopping jobs
under the Terraform IAM policy (which only allows jobs tagged `ManagedBy=snakemake-batch-flex`), `/dev/shm` size per rule, Fargate disk size from `disk_mb`, updating the
AMI with `update-ami.sh`, using an existing job definition, a private ECR image, job logs in
Terraform's own log group (EC2 and Fargate), a whole workflow run as a `smkflex` head job (from a
folder and with `--git`) and cancelling one, jobs on the Spot queue, the usage table (with each
rule's peak memory and the hint about many short jobs), retrying a job that ran out of memory with
more, the whole setup with `scripts/setup.sh` (twice, as a second, separate setup in the same
account; with conda and pandas in the image, and `smkflex init`/`push`/`pull`), the published
rna-seq-star-deseq2 pipeline on its test data (with `snakemake`, as a `smkflex` head job, and set
up with `snakedeploy`), and the conda env cache on it (per machine: 12 builds instead of 52, 11
instead of 18-21 minutes; from S3 on a second run: no builds, 7.5 minutes), and a job
reading an SSM parameter through `aws-batch-flex-secrets` on EC2 (`run.sh secrets`).

Only tested against a fake Batch: private registry
credentials (outside AWS), `--aws-batch-flex-volumes`, a single Ctrl-C, a Spot instance being
taken back mid-job.

Not tested yet: `notebook:`, Julia, GPU.

To run the AWS tests yourself, after `scripts/setup.sh`:

```bash
cd examples/conformance
bash run.sh script fanout conda pipeline rules     # about 15 minutes, a few cents
```

Each name is one small workflow (`bash run.sh` alone runs `script` and `fanout`; the comment at
the top of `run.sh` lists the rest, some of which need the Spot or Fargate queue). It prints
PASS or FAIL for each, and keeps Snakemake's output in `conformance-NAME.log`. It uses the
`smkflex` profile (`SMKFLEX_PROFILE=name` for another), or a `profile/config.yaml` in that
folder if there is one. `bash orphan-test.sh` tests what happens when Snakemake is killed
mid-run. The tests in `tests/` need no AWS: `pip install -e ".[test]"`, then
`python -m pytest -n auto`.

## Limits

- Host mode needs a glibc image with `/bin/sh` and bash. Alpine/musl images exit with 127 and say
  why. Build the AMI on the same CPU architecture as your instances.
- `pipe()` doesn't work. `--sdm apptainer` isn't supported, and Singularity images (`.sif`) can't
  be used.
- Images with an ENTRYPOINT: the plugin reads it from the image's registry (once per image per
  run) and gives the jobs a command it runs: a shell gets `-c <script>`, Python a `-c` that hands
  over to `/bin/sh`, and everything else `/bin/sh -c <script>`, which wrappers (tini, dumb-init,
  env) and entrypoint scripts that end with `exec "$@"` run. One that does something else (such
  as `["java", "-jar", "app.jar"]`) can't run the job: use an image without it.
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
