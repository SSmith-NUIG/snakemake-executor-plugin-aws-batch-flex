# Changelog

## [0.2.1](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/compare/v0.2.1...v0.2.1) (2026-10-31)


### Miscellaneous Chores

* release 0.2.1 ([3b93b0d](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/3b93b0d8c9c4570b3beed242ef2e6421f3ccdcf8))

## [0.2.1](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/compare/v0.2.0...v0.2.1) (2026-10-31)


### Documentation

* rewrite the docs for Snakemake users, with values for every option, and disclose AI assistance ([8116e61](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/8116e610b00bc107f1ac1317f34d0d7659f0fc0a))
* update what's been tested on AWS ([164b06c](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/164b06c8c06e34097081ba156c323272233eae85))

## [0.2.0](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/compare/v0.1.1...v0.2.0) (2026-10-31)


### Features

* aws-batch-flex-secrets gives jobs secrets without sending their values ([21a72f6](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/21a72f600fbd16d6542d74445361f4019432ed07))
* jobs keep storage files at their plain paths, as on a shared filesystem ([28d950d](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/28d950d95a61b0e0336b48fe5325f2047d0ef0d8))
* jobs run at the same path as Snakemake, so os.path.abspath() agrees ([cd003bd](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/cd003bd1ae8512e36bb5ac9ddcee981c8d09e8c0))


### Bug Fixes

* a failed group job shows why, which Snakemake's report of it leaves out ([2a7444d](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/2a7444d7e6fb2dc42df4fa898feb1617378d3381))
* a failed job also shows a Python traceback from earlier in its log file ([588f572](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/588f572915b9e36753eb6ed50850bee50543c14d))
* a failed job shows the end of its rule's log files, and why an index is missing ([5fc4dc2](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/5fc4dc2e2bf860fdfd4f6b4f0cbcfa39458723f1))
* a failed job's directory output on S3 no longer stops the run ([b8385c7](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/b8385c71d5d6c1eb2811d658ce8063e14d71bb78))
* a file git tracks but that was deleted here no longer stops the run ([f36e9bd](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/f36e9bded626173e429d10fd571e7c023eed9df9))
* a script that fails on pandas 3 data in a pandas 2 environment says why and what to do ([31cb5d0](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/31cb5d07e563ce3677183735056935f0c83ec53f))
* a script whose params are pandas objects runs where it can load them ([1651cf1](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/1651cf1519182abd1bb7381aae6aff298c807a3f))
* clearer failures and safer transfers found in a review ([e3ba7a5](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/e3ba7a55a698b9feadcbbae5fbd58086a3624856))
* conda environments are solved with strict channel priority in every job ([cc599f7](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/cc599f7b782a97e95af1fc20a2212f700fd09846))
* files the config names reach the jobs, even when git-ignored ([452d0ec](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/452d0ec02ea76e8a3c48794702b3dd5c1df7a8fe))
* jobs whose image has no curl or wget use the AMI's, as they do for git ([f184897](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/f184897ed64f113560b7ad95eba7d24e79218523))
* pandas 3 text arrays reach scripts as numpy arrays with earlier Snakemake 9 too ([ea96583](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/ea96583e4b12edd58d6d869d37515f41bdce28ba))
* pandas 3 text arrays reach scripts as numpy arrays, as pandas 2's did ([0d25fcf](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/0d25fcf386e1415cca156c3cadec752f835f17a7))
* smkflex init no longer keeps resources/ from the jobs ([893da48](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/893da48aaf130064fbfc4d8e2ce8ecd058239613))
* smkflex run --max-bundle-mb counts MB, as max-sources-mb does ([844a001](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/844a001dfeb56391734a5e410844fba684d5f580))
* smkflex run unpacks the workflow at the project's own path, so abspath agrees ([f015c2c](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/f015c2cc9eb12b8d3a3e0a9b221a38946b1db58f))
* stop before submitting when the workflow links to files outside its folder ([9c3fa53](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/9c3fa53d9e90c98f391ba9b24232fe7ab655b042))
* the setup's default image for rules without a container has curl, wget and git ([85b73f7](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/85b73f729d21d34ace3d3d78dad5cb017dfad2a0))
* the usage table suggests set-resources only for rules, not group jobs ([2cd816d](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/2cd816de2fd39277b24641522d0943cd3b08a895))
* workflow folder size limits count MiB, as ls -lh and du -sh show sizes ([9b10513](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/9b10513b9e88291692953b712393bef3e68340e0))


### Performance Improvements

* directory() outputs on S3 move 32 files at a time, not one by one ([3bd2513](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/3bd25136cda3dafd11d6eb00d25d63e07791dc97))


### Documentation

* aws-batch-flex-secrets tested on AWS (EC2) ([7da69e7](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/7da69e7e3d9fe206734761f3be1e4c8c18f82130))
* jobs build conda envs with strict channel priority ([098c4e9](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/098c4e9e9a70649731e1839821082fa23b3150d9))
* which python a script: rule gets when its params are pandas objects ([c02248f](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/c02248f21758e3ed821567944a72fd1a93e814c5))

## [0.1.1](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/compare/v0.1.0...v0.1.1) (2026-10-29)


### Bug Fixes

* big runs read peak memory from jobs across the whole run, not only the last ones ([147a054](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/147a054053c7e7dbfee38029c95d9fa92dd44f00))
* cancelling a run stops every job, also those Snakemake is checking on ([9edb388](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/9edb3880575bb8d3c4fb2aceb469f7a120bfb3a4))
* non-root images without curl get uv from their own conda at job start ([374cc30](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/374cc303e6d1d42b17255dc36fe05c76ec84beff))
* smkflex push gives the .gitignore note only when it uploaded, and no rate for tiny transfers ([6be28ad](https://github.com/SSmith-NUIG/snakemake-executor-plugin-aws-batch-flex/commit/6be28ad2efe30a30f13c4c9bd077969f573024fb))

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
