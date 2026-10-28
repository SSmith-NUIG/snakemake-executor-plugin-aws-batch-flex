# Changelog

## 0.1.0 (2026-10-28)

First release. Needs Snakemake 9 (9.5 or newer) and Python 3.11 to 3.13. Tested on Linux; macOS
should work, and on Windows use WSL.

### Features

* run each rule as an AWS Batch job in the image from its `container:` directive, without
  Snakemake or Python in the image
* Snakemake from a machine image (AMI) mounted read-only into EC2 jobs, or installed when the job
  starts (Fargate, or without the AMI)
* mix EC2, Spot and Fargate queues in one run (`batch_queue` per rule); the platform is detected
  per queue
* per-rule `threads`, `mem_mb`, `runtime`, `gpu`, `/dev/shm` size (`batch_shm_mb`), Fargate disk
  (`disk_mb`), tags, Spot retries, fair-share priority and existing job definitions
* Python `script:` rules run in the image's own Python (3.10 or newer)
* conda environments and wrappers in images without conda, with built environments reused per
  machine and optionally kept in S3 (`conda-cache`)
* retry jobs that ran out of memory with more memory (`oom-memory-factor`)
* failed jobs show the end of their log, with hints for known causes (out of memory, full disk,
  missing package or storage plugin, S3 permissions, an image `ENTRYPOINT` in the way)
* startup checks of the queue, compute environments and roles; jobs left over from an
  interrupted run are cancelled
* end-of-run table of what each rule used (vCPU- and GiB-hours); costs come from AWS Billing, by
  the tags `snakemake-run-id` and `snakemake-rule` that every job carries
* peak memory, busy vCPUs and disk use per rule, with `set-resources:` and `set-threads:`
  suggestions to paste
* every job gets the whole workflow folder (what git tracks, or all that `.gitignore` doesn't
  exclude, and `config/`), so scripts that `shell:` runs, imported modules and sample sheets
  reach it without git; `data/`, `results/`, `logs/` and `benchmarks/` stay behind, and more than
  100 MB (`max-sources-mb`) stops the run at the start
* the same Snakemake and package versions in every job, pinned to the AMI's or the local ones
* a missing input says that jobs read inputs from the bucket, and the `smkflex push` that uploads it
* runs that can't work stop at the start: a `workdir:` directive, a job command too long for ECS
* `script:` rules and pandas work in images that bring their own conda libraries (bioconda's)
* jobs can't use the instances' credentials, and the launcher policy can't register privileged
  job definitions
* `smkflex run`: run Snakemake itself on AWS in a head job, from the current folder or a git
  repository (`--git`, `--rev`, `--path`, `--add`), with `.snakemake` state (also with
  `--persistence-backend db`) kept between runs and saved while it runs; one run per project
* `smkflex logs`, `status` and `cancel` for those runs
* `smkflex init`, `push` and `pull`: a project folder in the bucket, uploading inputs and
  downloading results at the paths Snakemake uses
* `smkflex check`: checks the whole setup and says what to fix
* `scripts/setup.sh`: one-command setup of the AMI, queue, compute environment, roles, bucket,
  log group and `smkflex` profile, with optional Spot and Fargate queues
* `scripts/update-ami.sh`, `cleanup-ami.sh` and `cleanup-jobdefs.sh` to update the AMI and remove
  the plugin's old images and job definitions
* Terraform setup with 30-day job logs, optional expiry of old files in the bucket, and an IAM
  policy for people who only run pipelines that can't stop or change other Batch work
* private images: ECR through the IAM roles, other registries through Secrets Manager

### Documentation

* README with a step-by-step setup from an empty AWS account to a finished test run
* guide, reference and setup pages in `docs/`, and runnable tests on AWS in `examples/conformance/`
