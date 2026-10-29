# Changelog

## [0.2.0](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/compare/v0.1.0...v0.2.0) (2026-10-29)


### ⚠ BREAKING CHANGES

* the setting no-cost-summary is now no-usage-summary.

### Features

* a job too long for ECS stops before it's submitted ([7063a60](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/7063a6004a358a5fc9e82e148b2e8e92adc2da32))
* images with an ENTRYPOINT work ([860e3e6](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/860e3e68e3e28ecd4da8d7c16c8d6311c7b42f64))
* say how to upload an input that is only on this machine ([7c71647](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/7c71647364eb3aeac43f264b0b552e83f10a124e))
* the end-of-run table shows what was used, not what it cost ([6e9ff1a](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/6e9ff1a5024d32756c4c581b38c6ff028b18e08c))
* the same software in every job ([75fae77](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/75fae77775c964bbcda58482b54bd01c05b69a40))


### Bug Fixes

* a missing input in storage says jobs read inputs from the bucket and how to upload them ([51f7cdc](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/51f7cdc14795d856fd5fdb1f69d90dd74b116503))
* a stopped or failed head job keeps Snakemake's state ([1638554](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/1638554a3ed0dee0102e6639105175df0d5c7ab2))
* a workdir: directive stops the run at the start ([fb9fbd3](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/fb9fbd3a206f099057f607f28d2536dea542b134))
* AWS errors are recognised by their codes, not their text ([ca58ba0](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/ca58ba0802327c17145d4e883604e324035102a9))
* data/, results/, logs/ and benchmarks/ never go to the jobs ([0ea8fad](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/0ea8fad8317a01d7b8a236cf384c30deee3b0a9e))
* every job gets the workflow folder, as smkflex run sends it ([1e8799a](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/1e8799a17f76b4db104a63ab9d05a32dba64c6c6))
* every system library numpy lacks in an image comes from the AMI's conda ([3ef1f1f](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/3ef1f1f281322ec215735a1eda002ff6e0154588))
* jobs can't take the instances' credentials ([29c922b](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/29c922b62b01aee22b27fefc74dba4a129b79424))
* jobs only run job tools that match their checksum ([6204321](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/6204321fb887b535c1b9a617fe7f1636314a9048))
* memory figures miss fewer spikes and retries don't double twice ([b83b52a](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/b83b52a5ec8d984a3ea842da7400e768b4cafc24))
* script: rules work again in images without libstdc++ (bioconda's) ([316e2a9](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/316e2a9053acd36442d96addd151e1ac04ef9bee))
* scripts that shell: commands run reach the jobs without git ([3fc9506](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/3fc95066c15532e9c8d7891b0f28d3e07ade94ac))
* smkflex checks the profile's settings as snakemake does ([e36e6bf](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/e36e6bfb179437a00b711fb34c66787ecd2f57ea))
* smkflex keeps Snakemake's records with the database backend too ([01af045](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/01af045e9629559ae298194028a67332ef5f0b6d))
* smkflex push and pull notice files edited to the same size ([34e3ccb](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/34e3ccb72d18f839ce5081083dca0459ac578283))
* smkflex run keeps one run and one state per project ([ed8f721](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/ed8f721c4ca1502f3a935de166ac69955f11706b))
* the AMI pins extra packages (e.g. pandas) to this machine's versions too ([3df530c](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/3df530cd1900b598195d61825255e0062f5268ff))
* the AMI's pandas loads in bioconda images too ([ea0796f](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/ea0796f670a4660b06301328a5ed82ea22e6e395))
* the conda env cache keeps CPU types apart ([a7f9dfb](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/a7f9dfb3509bbca4311dc33836d60b0606c646c9))
* the launcher policy can't register privileged job definitions ([f6b5a51](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/f6b5a518cf6140b33b8e1934f6aa11d9d77617d4))
* the recorded commit counts new files that the run sends ([4c9212e](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/4c9212e7df3d39f1bf0d85e9e2b25738613a554a))
* the usage table says what memory a suggestion gives and why busy can exceed vCPUs ([da6da04](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/da6da043cb42269ba7d3988b286cb6dd602e72e0))
* workarounds check the code they patch, and say when they can't ([9509885](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/95098853c614be259cf21e7b7bfcd982abce16dc))


### Performance Improvements

* jobs' commands leave room under ECS's limit ([31a916c](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/31a916c5126bb76dd52c30e6226a05310d87ba02))
* the S3 directory workaround no longer costs a request per file ([cc8bfd8](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/cc8bfd84162ff716b89f95e664d9a509beeeb356))


### Documentation

* a Batch job's overhead as measured, not guessed ([fe7751e](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/fe7751e4e3a37913962ccb0332e1bd790e45a31f))
* adding your own user-data, and installing the env at boot ([4e7d5b4](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/4e7d5b4fc422be231f6b6f08f24c1e3a3bf14a77))
* changelog for 0.1.0 covers this week's changes ([adadbdf](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/adadbdf5d7f054ca13d9925cfe14c135075584b6))
* what works differently from a local run ([8f2cbdd](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/8f2cbdd4769159c1b33ac92a5f58684f45ac8121))

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
* images with an `ENTRYPOINT` (`["python3"]`, tini, entrypoint scripts): it's read from the image's
  registry and the job's command is made to suit it, as Batch can't change it (`batch_entrypoint`
  if the registry can't be read)
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
