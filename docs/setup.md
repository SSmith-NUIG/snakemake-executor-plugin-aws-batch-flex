# Setup and upkeep

Beyond the README's [Getting started](../README.md#getting-started): managed AWS accounts,
keeping the AMI up to date, doing the setup by hand, and removing everything.

- [University and company accounts](#university-and-company-accounts)
- [Updating the machine image](#updating-the-machine-image)
- [Setting up by hand](#setting-up-by-hand)
- [Without the machine image](#without-the-machine-image)
- [Your own user-data](#your-own-user-data)
- [Costs and removing everything](#costs-and-removing-everything)

## University and company accounts

Organisations often manage AWS access centrally: you log in through single sign-on, can't
create IAM users, and don't have administrator rights. The plugin works with that, with these
adjustments.

**Logging in with single sign-on.** Instead of access keys
([step 3](../README.md#3-connect-to-your-aws-account)), run `aws configure sso` once (your IT
department can tell you the start URL and region) and give the profile a name, e.g. `lab`. Then,
in each new terminal:

```bash
aws sso login --profile lab
export AWS_PROFILE=lab
```

`setup.sh`, Terraform, Snakemake and `smkflex` all use that login.

**Permissions.** `setup.sh` creates IAM roles, a Batch queue and compute environment, an S3
bucket, a log group and an AMI. If you aren't allowed to, ask IT to run `setup.sh` once for the
lab, or to give you `PowerUserAccess` plus permission to create IAM roles and instance profiles
named `smkflex-*`. Once it's set up, people who only run pipelines need much less: the policy
printed by `terraform -chdir=terraform output -raw launcher_policy_json` (see
[IAM](reference.md#iam)), which IT can attach to your login.

**No default VPC.** Some accounts have no default subnets, in which case `setup.sh` stops and
says so. Ask IT which subnets to use and pass them with `--subnets subnet-a,subnet-b`. Instances
need outbound internet access (to pull images and install conda packages): a public subnet, or a
private one with a NAT gateway (for Fargate jobs there, also set
`aws-batch-flex-assign-public-ip: DISABLED` in the profile).

## Updating the machine image

To add Python packages or named conda envs to the AMI, or to update its Snakemake, bake a new
AMI on top of the current one:

```bash
bash scripts/update-ami.sh --subnet subnet-XXXX --region eu-west-1 --extra-package scikit-learn
bash scripts/update-ami.sh --subnet subnet-XXXX --region eu-west-1 --conda-env envs/rnaseq.yaml
```

The new AMI gets the Snakemake and package versions installed where you run the script. The
script points `terraform/terraform.tfvars` at the new AMI and shows the Terraform plan;
`terraform apply` then updates the compute environment in place (new instances start from the
new AMI) and leaves the queue alone. Apply it while no jobs are running. Envs baked with
`--conda-env` can be used by name (`conda: "rnaseq"`). Old AMIs can be removed with
`scripts/cleanup-ami.sh` (see [Costs and removing everything](#costs-and-removing-everything)).

## Setting up by hand

What `scripts/setup.sh` does, step by step.

### Find your subnets

Instances start inside a subnet of your account's VPC. New accounts come with default subnets
that have internet access, which is what's needed here (to pull container images). List them:

```bash
aws ec2 describe-subnets --filters Name=default-for-az,Values=true --query 'Subnets[].SubnetId' --output text
```

You'll get two or more IDs like `subnet-0a1b2c3d`. Note them down. If the list is empty, your
account has no default VPC; create one with `aws ec2 create-default-vpc` and run the command
again.

### Build the machine image

This starts a temporary instance from Amazon's ECS-optimized AMI (the standard image for Batch),
installs Snakemake on it, saves the result as your own AMI, and terminates the instance. First
look up the current ECS-optimized AMI for your region:

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

### Create the AWS resources

[Terraform](https://www.terraform.io) creates everything else from the files in `terraform/`: the
job queue, the compute environment that starts and stops instances for it, the IAM roles the
instances and jobs need, a log group, and an S3 bucket for your files.

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

`plan` only shows what would be created. The last line should read something like
`Plan: 20 to add, 0 to change, 0 to destroy`. If anything would be changed or destroyed, stop
and check you're in the right folder and account. Otherwise create it all (type `yes` when
asked):

```bash
terraform apply
cd ..
```

No instances are running yet: the compute environment starts them when jobs arrive and stops
them when the queue is empty.

### Make a profile

Terraform prints a profile for your account; install it under the name `smkflex`:

```bash
mkdir -p ~/.config/snakemake/smkflex
terraform -chdir=terraform output -raw snakemake_profile > ~/.config/snakemake/smkflex/config.yaml
echo "jobs: 100" >> ~/.config/snakemake/smkflex/config.yaml
cat ~/.config/snakemake/smkflex/config.yaml
```

If the AMI has conda (`--conda`), also add `aws-batch-flex-conda-path: /opt/miniforge3`. You
should see lines like `executor: aws-batch-flex` and `aws-batch-flex-queue: smkflex-queue`.

## Without the machine image

To try the plugin on an existing Batch queue without building an AMI, set
`aws-batch-flex-bootstrap: auto` in the profile instead of `host`. Snakemake is then installed
into each container when its job starts (about 10-15 s per job), which needs internet access and
`curl`, `wget` or `pip` in the image. For images with none of these,
`--aws-batch-flex-install-prereqs` gets uv through the image's own conda, mamba or micromamba if
it has one (as any user), or else installs `curl` with the image's package manager (which needs
an image that runs as root).

Alternatively, install the Snakemake env on each instance at boot, using the stock ECS-optimized
AMI rather than one of your own. Each new instance then spends about a minute on it before its
first job (not yet tried on AWS). Generate the instances' user-data, keeping the plugin's own
boot script:

```bash
snakemake-batch-flex-host-env userdata --storage-plugin s3 \
  --include terraform/userdata.snakemake.txt > terraform/userdata.mime
```

Then set `userdata_file = "userdata.mime"`, and the stock ECS-optimized AMI as `ami_id`, in
`terraform.tfvars`.

## Your own user-data

Instances boot with `terraform/userdata.snakemake.txt`, which configures the ECS agent for large
images and for stopping jobs cleanly. To run your own setup at boot as well (a site agent, a
mount, a proxy), merge the two rather than replacing the plugin's: `--include` keeps each file
as it is (a script, a `#cloud-config`, or a multipart document) and packs them into the single
MIME document EC2 accepts:

```bash
snakemake-batch-flex-host-env userdata --storage-plugin s3 \
  --include terraform/userdata.snakemake.txt --include my-setup.sh > terraform/userdata.mime
```

This also adds the boot-time Snakemake installer (see above). It installs the versions from the
machine where you generated the user-data. If the AMI already has exactly those versions (for
example, it was baked from the same environment), the installer does nothing; otherwise it
installs over the AMI's env, which takes about a minute per instance. Then set
`userdata_file = "userdata.mime"` in `terraform.tfvars`, and run `terraform plan` and `apply`.

## Costs and removing everything

While idle, the only costs are the files in your bucket, the job logs and the stored AMI (cents
per month for small amounts). Job logs are deleted after 30 days (`log_retention_days` in
`terraform.tfvars`). Files in the bucket stay until you delete them; set `s3_expire_days` to
have files under the prefix deleted automatically that many days after they were written (only
if you copy results you want to keep elsewhere). Incomplete multipart uploads, e.g. from a killed
job, are cleared after 7 days. Instances only run while jobs do: the AWS console's **EC2** >
**Instances** page should show no running Batch instances a few minutes after a workflow ends.

To remove everything:

```bash
aws s3 rm s3://YOUR-BUCKET --recursive          # empty the bucket (deletes your results!)
cd terraform && terraform destroy && cd ..       # remove queue, roles, bucket, ...
bash scripts/cleanup-ami.sh --region eu-west-1 --no-tfvars ami-XXXXXXXX   # the AMI (ID in terraform/terraform.tfvars)
bash scripts/cleanup-jobdefs.sh --region eu-west-1                       # the plugin's job definitions
rm -r ~/.config/snakemake/smkflex                                        # the profile
```

`terraform destroy` refuses to delete a bucket that still has files in it, hence the first line.
`cleanup-ami.sh` only deletes AMIs that `bake-ami.sh` made, asks before deleting, and refuses
any AMI still used by a compute environment, launch template or instance. Normally it also
refuses the AMI named in `terraform.tfvars`; `--no-tfvars` skips that one check, since after
`terraform destroy` nothing uses it any more.

`cleanup-jobdefs.sh` is also useful on its own from time to time: job definitions for images you
no longer use stay in AWS (they cost nothing, but accumulate). It only deregisters the plugin's
own definitions, never one an active job uses, and the plugin registers them again if a later
run needs one. Add `--dry-run` to see the list first.
