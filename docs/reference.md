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

Settings go in the global `smkflex` profile (`~/.config/snakemake/smkflex/config.yaml`), in a
project profile (`profiles/default/config.yaml`), or on the command line with `--` in front:

```yaml
aws-batch-flex-ephemeral-storage-gib: 50          # command line: --aws-batch-flex-ephemeral-storage-gib 50
aws-batch-flex-tags: [project=rnaseq, team=lab]   # command line: --aws-batch-flex-tags project=rnaseq team=lab
aws-batch-flex-skip-preflight: true               # command line: --aws-batch-flex-skip-preflight
```

- **Lists** (`tags`, `volumes`, `extra-packages`, `registry-credentials`, `secrets`) are YAML
  lists in a profile and space-separated values on the command line.
- **On/off settings** (`install-prereqs`, `skip-preflight`, `no-usage-summary`, `no-auto-tags`)
  are `true` or `false` in a profile and a bare flag on the command line.
- **ARNs** are full IAM or secret ARNs, e.g. `arn:aws:iam::123456789012:role/smkflex-job`;
  `terraform -chdir=terraform output` prints the ones the Terraform setup made.

Only `queue` is required, and `setup.sh` writes the usual settings for you; most users never
need more than the first group below. In the Default column, a value in `code` is the default,
and one in *italics* says what happens when the setting isn't given. Invalid values stop the
run at the start with a message saying what's allowed.

### Basics

| Setting (`aws-batch-flex-...`) | Value | Default | What it does |
|---|---|---|---|
| `queue` | Batch job queue name | *required* | The queue jobs go to. A rule can send its jobs elsewhere with `batch_queue`. |
| `region` | AWS region code, e.g. `eu-west-1` | *from `AWS_REGION` or your AWS config* | The region of the queue, bucket and roles. |
| `job-role-arn` | IAM role ARN | *the instance role (EC2 only)* | The role jobs run as; it needs read and write access to the S3 prefix. Without it, jobs use the instances' role, which only works if the instances allow it (`jobs_use_instance_role` in Terraform). |
| `execution-role-arn` | IAM role ARN | *none* | The role ECS uses to start a job: to pull its image, send its logs and read secrets. Required for Fargate queues, `registry-credentials` and `secrets`. |
| `head-role-arn` | IAM role ARN | *none* | The role of the `smkflex run` head job (see [Long runs](guide.md#long-runs-smkflex)). Only `smkflex` uses it; Snakemake ignores it. |
| `default-image` | image, e.g. `ubuntu:24.04` or `quay.io/org/tool:1.0` | *Snakemake's `--container-image`* | The image for rules without `container:`. Setup writes `buildpack-deps:bookworm-scm` (Debian 12 with curl, wget and git), since pipelines and wrappers often call such tools without a container or conda package for them. |
| `tags` | list of `KEY=VALUE` | *none* | Extra tags on every job and job definition, e.g. `[project=rnaseq, team=genomics]`, to break costs down. A rule's `batch_tags` adds to or overrides them. |

### How jobs get Snakemake

Jobs run in each rule's own image, which normally has no Snakemake in it. On EC2, Snakemake comes
from the AMI (`host`); otherwise it's installed when each job starts, which takes 10-15 s per
job.

| Setting (`aws-batch-flex-...`) | Value | Default | What it does |
|---|---|---|---|
| `bootstrap` | `host`, `auto`, `always` or `never` | `auto` | `host`: mount Snakemake from the AMI (what `setup.sh` sets; EC2 only). `auto`: use the image's own Snakemake if it's the same version as yours, otherwise install it when the job starts. `always`: always install it at start. `never`: the image already has Snakemake and the storage plugins. |
| `host-env-path` | absolute path on the instance | `/opt/snakemake-env` | Where the AMI has Snakemake; it's mounted read-only at the same path in jobs. |
| `host-env-wait-seconds` | whole seconds, `0` or more | `120` | How long a job waits for that Snakemake if its instance is still installing it at boot (see [Without the machine image](setup.md#without-the-machine-image)). An AMI made by `setup.sh` has it from the start, so `0` is fine there. |
| `extra-packages` | list of pip requirements, e.g. `[pandas, "scikit-learn>=1.5"]` | *none* | Installed along with Snakemake when a job installs it at start (`auto`, `always`): packages your Snakefile imports, or other Snakemake plugins. With `host`, add them to the AMI instead. |
| `python-version` | `MAJOR.MINOR`, e.g. `3.12` | *the Python you run Snakemake with* | The Python that jobs install Snakemake with at start. |
| `install-prereqs` | `true` or `false` | `false` | When installing at start, for images that have none of `curl`, `wget` or `pip`: get uv with the image's conda, mamba or micromamba (any user), or else install `curl` with its package manager (apt, apk, dnf or yum; images that run as root). |
| `script-python` | `image` or `host` | `image` | Which Python runs Python `script:` rules. `image`: the image's own if it's 3.10 or newer, so the script can use the image's packages (Snakemake's when the rule's pandas, numpy or polars `params` can't be loaded with it). `host`: always Snakemake's. A rule can choose with `batch_script_python`. |

### Memory, disk and Spot

| Setting (`aws-batch-flex-...`) | Value | Default | What it does |
|---|---|---|---|
| `mem-overhead-mib` | MiB, `0` or more | `256` | Added to each rule's `mem_mb` for the Snakemake process that runs in the job and shares its memory limit. `0` requests exactly `mem_mb`. Not added to rules that set no memory (they get 2048 MiB). |
| `oom-memory-factor` | number, `1` or more (e.g. `1.5`) | `2` | When a job runs out of memory and Snakemake retries it (`retries`), the retry gets this many times the memory, compounding on each further retry (on Fargate, up to 120 GiB). `1` retries with the same memory. A rule that scales its own memory with `attempt` gets exactly what it asks for. |
| `spot-retries` | whole number, `0` to `9` | `2` | How many times Batch restarts a job when AWS reclaims its Spot instance, separately from Snakemake's `retries`. `0`: never. A rule can override it with `batch_spot_retries`. |
| `ephemeral-storage-gib` | GiB, `21` to `200` | *none: jobs get 20 GiB* | Fargate only: scratch disk for every job. A rule's `disk_mb` can ask for more for its own jobs. (On EC2, jobs share their instance's disk, whose size is set in Terraform.) |
| `assign-public-ip` | `ENABLED` or `DISABLED` | `ENABLED` | Fargate only: `ENABLED` gives jobs a public IP address, which they need to reach the internet (images, S3) in a public subnet. Use `DISABLED` in private subnets with a NAT gateway or VPC endpoints. |

### Conda

| Setting (`aws-batch-flex-...`) | Value | Default | What it does |
|---|---|---|---|
| `conda-path` | absolute path on the instance, e.g. `/opt/miniforge3` | *none* | Where the AMI has conda (`setup.sh` sets it). It's mounted read-only into jobs, so `conda:` rules work in images without conda, and envs baked into the AMI can be used by name. EC2 only. |
| `conda-cache` | `instance`, `none`, or an `s3://` URL | `instance` | Reuse built conda envs instead of building them in every job. `instance`: jobs on the same EC2 instance share them. An S3 URL, e.g. `s3://BUCKET/snakemake/.smkflex/conda-cache`: they're also kept there for later jobs and runs, and Fargate jobs can use them. `none`: build in every job. See [Conda and wrappers](guide.md#conda-and-wrappers). |

### Logs, checks and leftovers

| Setting (`aws-batch-flex-...`) | Value | Default | What it does |
|---|---|---|---|
| `log-group` | CloudWatch log group name | `/aws/batch/job` | Where job logs go. It must already exist. Terraform creates `/aws/batch/<name>`, which deletes logs after 30 days, and puts it in the profile; Batch's default group keeps them forever. |
| `no-usage-summary` | `true` or `false` | `false` | Don't print the end-of-run usage table (see [What a run used](guide.md#what-a-run-used)). |
| `max-sources-mb` | MiB, `1` or more | `100` | Every job gets the workflow folder (what git would track, or everything `.gitignore` doesn't exclude, plus `config/`; never `data/`, `results/`, `logs/` or `benchmarks/`, nor any file over 10 MB). If that's bigger than this (as `du -sh` reports sizes), the run stops and says which folders to add to `.gitignore`. |
| `runnable-warn-minutes` | whole minutes | `15` | Warn when a job has waited this long in `RUNNABLE` for an instance, which usually means no allowed instance type is big enough. |
| `skip-preflight` | `true` or `false` | `false` | Skip the start-of-run checks of the queue, its compute environments and the job role. |
| `orphan-jobs` | `cancel`, `warn` or `ignore` | `cancel` (`warn` with `--nolock`) | What to do with jobs still active from an earlier, interrupted run of the same workflow: cancel them, only list them, or skip the check (see [Interrupted runs](guide.md#interrupted-runs)). |
| `no-auto-tags` | `true` or `false` | `false` | Don't tag jobs with their run, workflow, rule and job ID. This also turns off the check for jobs left over from an interrupted run, and per-rule cost reporting. |

### Less common

| Setting (`aws-batch-flex-...`) | Value | Default | What it does |
|---|---|---|---|
| `platform` | `ec2` or `fargate` | *detected per queue; `ec2` if the queue can't be read* | Only needed if the plugin isn't allowed to describe the queue. A rule can force it with `batch_platform`. |
| `volumes` | list of `HOST_PATH:JOB_PATH`, optionally with `:ro` | *none* | EC2 only: host paths to mount into every job, e.g. `[/data/refs:/refs:ro]`. |
| `scheduling-priority` | whole number, `0` to `9999` | *none* | Priority for every job, higher first. Only for queues with a fair-share scheduling policy; other queues ignore it, with a warning. A rule can override it with `batch_priority`. |
| `registry-credentials` | list of `REGISTRY=SECRET_ARN` | *none* | Credentials for private registries outside AWS, e.g. `[docker.io=arn:aws:secretsmanager:...:secret:dockerhub]`, where the secret holds `{"username": ..., "password": ...}`. Only images from that registry use it. Needs `execution-role-arn`. ECR needs none (see [Private images](guide.md#private-images)). |
| `secrets` | list of `NAME=ARN` | *none* | Environment variables that jobs get from an SSM parameter or Secrets Manager secret, without the value passing through Batch, e.g. `[GITHUB_TOKEN=arn:aws:ssm:eu-west-1:123456789012:parameter/smkflex/github-token]`. Needs `execution-role-arn` (see [Secrets](guide.md#secrets)). |
| `job-definition` | job definition name, `name:revision` or ARN | *none* | Use an existing Batch job definition instead of the ones the plugin registers, e.g. where you aren't allowed to register them. It decides the image, job role, mounts and `/dev/shm`; vCPUs, memory, timeout and tags still come from the rules. If it doesn't mount the AMI's Snakemake, jobs install it at start. A rule can choose its own with `batch_job_definition`. |
| `job-definition-prefix` | text | `snakemake` | Name prefix for the job definitions the plugin registers. The Terraform IAM policy only allows `snakemake-*`. |

Every job is also tagged with `snakemake-run-id`, `snakemake-workflow`, `snakemake-rule` and
`snakemake-job-id`, so you can find a run's or a rule's jobs in the Batch console and in billing.
Every job is always tagged `ManagedBy=snakemake-batch-flex` (even with `no-auto-tags`), which
the IAM policy requires before it lets you stop a job.

## The smkflex command

`smkflex` is installed with the plugin. Every command takes `--profile NAME` (a profile name, as
for `snakemake --profile`, or a folder with `config.yaml`). Without it, `smkflex` uses
`./profile` if there is one, then `$SNAKEMAKE_PROFILE`, then the only profile in
`~/.config/snakemake/` that selects this plugin. Like Snakemake, it also reads the project
profile (`profiles/default/config.yaml`) on top. `smkflex COMMAND --help` lists each command's
options.

| Command | Options | What it does |
|---|---|---|
| `smkflex check` | | Checks the setup (AWS login, queue, roles, bucket access, Snakemake versions) and says what to fix. |
| `smkflex init` | `--name NAME`: the project's prefix in the bucket (default: the folder's name) | Sets up the current folder as a project: a project profile with its own `default-storage-prefix`, and a `.gitignore` (see [Your own workflow](../README.md#your-own-workflow)). |
| `smkflex push PATH...` | `--dry-run`: only list what would be uploaded; `--force`: upload unchanged files too; `--prefix S3_URL`: upload elsewhere | Uploads files or folders to where the rules look for them. Paths are as the workflow names them: relative (`data`) or absolute (`/shared/refs`). |
| `smkflex pull PATH...` | `--to DIR`: download into DIR (default: here); `--dry-run`; `--force`: download unchanged files too; `--prefix S3_URL` | Downloads outputs from the bucket, e.g. `smkflex pull results logs`. Without a path, it lists the top-level folders in storage. |
| `smkflex run [-- SNAKEMAKE_ARGS]` | `--wait`: stay attached and print the log; `--git URL`, `--rev REV`, `--path DIR`: run from a git repository; `--add PATH`: send extra files (repeatable); `--name NAME`; `--queue`, `--role`, `--image`, `--vcpus` (default `1`), `--mem-mb` (default `2048`): the head job; `--max-bundle-mb` (default `100`) | Runs Snakemake on AWS in a head job, so your machine can be switched off, and prints the run's JOB_ID. Snakemake arguments go after `--`, e.g. `smkflex run -- --forceall`. See [Long runs](guide.md#long-runs-smkflex). |
| `smkflex status` | `--queue QUEUE`; `--limit N` (default `20`) | Lists recent `smkflex run` runs: JOB_ID, state and the commit each one ran. |
| `smkflex logs JOB_ID` | `--follow` (`-f`): keep watching | Prints a run's Snakemake output. |
| `smkflex cancel JOB_ID` | | Stops a run; Snakemake cancels its jobs first. |

## IAM

The identity running `snakemake` needs `batch:SubmitJob`, `DescribeJobs`, `TerminateJob`,
`RegisterJobDefinition`, `DescribeJobDefinitions`, `DescribeJobQueues`, `ListJobs`,
`DescribeComputeEnvironments` and `TagResource`, plus `logs:GetLogEvents`, `iam:PassRole` on the
job role(s), and read/write on the S3 prefix. `ec2:DescribeImages` and
`ec2:DescribeLaunchTemplateVersions` are optional; they let the plugin warn when the AMI's
Snakemake version is far from yours.

`terraform output launcher_policy_json` prints a policy with all of this, scoped to what the
plugin creates so it can't touch other Batch work in the account: it registers only
`snakemake-*` job definitions and never privileged ones (root on the instance), stops only jobs
tagged `ManagedBy=snakemake-batch-flex` (the plugin and `smkflex` tag every job with it), and
reads only the plugin's log group.

The job role needs read/write on the prefix. For `smkflex run`, the head job's role needs the
same permissions as the identity running `snakemake`, and that identity also needs
`iam:PassRole` on the head role (the Terraform setup includes both).

## What's been tested

On real AWS (eu-west-1):

- **Rule types:** `shell:`, `run:`, Python, bash and R `script:` (Python scripts in the image's
  own Python), `wrapper:`.
- **Conda:** YAML envs in an image with conda; YAML and named (AMI-baked) envs in images
  without it; the env cache on rna-seq-star-deseq2 (per instance: 12 builds instead of 52,
  11 minutes instead of 18-21; from S3 on a second run: no builds, 7.5 minutes).
- **Workflow features:** a different image per rule, checkpoints, groups, `shadow:`,
  `benchmark:`, `log:`, retries, `temp()`, `directory()`, resources and tags, fan-out to 100 jobs.
- **Platforms:** EC2 and Fargate, separately and mixed in one run; the Spot queue; `/dev/shm`
  size per rule; Fargate disk size from `disk_mb`; install-at-start on a bare `debian:12`.
- **Images and job definitions:** a private ECR image, an existing job definition, secrets from
  SSM through `aws-batch-flex-secrets` on EC2.
- **Failures and cancelling:** OOM and timeout reporting, retrying an OOM-killed job with more
  memory, Ctrl-C, cancelling jobs left over from an interrupted run, stopping jobs under the
  Terraform IAM policy, startup checks.
- **Usage table:** the CPU, disk and peak-memory figures, and the hint about many short jobs.
- **`smkflex`:** a whole workflow as a head job (from a folder and with `--git`) and cancelling
  one; `init`, `push` and `pull`.
- **Setup and upkeep:** the whole setup with `scripts/setup.sh` (twice, the second as a
  separate setup in the same account, with conda and pandas in the AMI), updating the AMI with
  `update-ami.sh`, job logs in Terraform's log group (EC2 and Fargate).
- **Pipelines:** a 6-image variant-calling pipeline, and the published rna-seq-star-deseq2 on its
  test data (with `snakemake`, as a `smkflex` head job, and deployed with `snakedeploy`).

Only against a fake Batch: private registry credentials (outside AWS),
`--aws-batch-flex-volumes`, a single Ctrl-C, a Spot instance being reclaimed mid-job.

Not tested yet: `notebook:`, Julia, GPU.

To run the AWS tests yourself, after `scripts/setup.sh`:

```bash
cd examples/conformance
bash run.sh script fanout conda pipeline rules     # about 15 minutes, a few cents
```

Each name is one small workflow (`bash run.sh` alone runs `script` and `fanout`; the comment at
the top of `run.sh` lists the rest, some of which need the Spot or Fargate queue). It prints
PASS or FAIL for each and keeps Snakemake's output in `conformance-NAME.log`. It uses the
`smkflex` profile (`SMKFLEX_PROFILE=name` for another), or `profile/config.yaml` in that folder
if it exists. `bash orphan-test.sh` tests what happens when Snakemake is killed mid-run. The
tests in `tests/` need no AWS: `pip install -e ".[test]"`, then `python -m pytest -n auto`.

## Limits

- With `bootstrap: host` (Snakemake from the AMI), images must be glibc-based and have
  `/bin/sh` and bash. Alpine/musl images exit with code 127 and say why. Build the AMI on the
  same CPU architecture as your instances.
- `pipe()` doesn't work. `--sdm apptainer` isn't supported, and Singularity images (`.sif`) can't
  be used.
- Images with an `ENTRYPOINT`: Batch can't override it, so the plugin reads it from the image's
  registry (once per image per run) and builds a command it can run: a shell gets
  `-c <script>`, Python gets a `-c` that hands over to `/bin/sh`, and anything else gets
  `/bin/sh -c <script>`, which works with wrappers (tini, dumb-init, env) and entrypoint scripts
  that end with `exec "$@"`. An entrypoint that does something else (such as
  `["java", "-jar", "app.jar"]`) can't run the job: use an image without it.
- All rules in a group must use the same image.
- On EC2, all jobs on an instance share its disk (`root_volume_gib` in Terraform, 100 GiB in the
  example), and `disk_mb` is not enforced: Batch places jobs by CPU and memory only. Several jobs
  with large files on one instance can fill it, and they then fail with "No space left on
  device" (the error says so). Use a bigger volume, or a second queue with bigger disks for the
  heavy rules (`batch_queue`).
- Install-at-start fails on very old images (e.g. glibc 2.23).
- Your Snakemake and the AMI's don't have to be the same version: 9.5 up to 9.27 mix fine in both
  directions (tested against the fake Batch; 9.0.x doesn't work). But every storage plugin
  installed on your machine must also be in the AMI, because Snakemake passes all of their
  options to the jobs; a job that fails this way names the plugin.
