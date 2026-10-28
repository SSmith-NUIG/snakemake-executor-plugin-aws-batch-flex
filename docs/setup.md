# Setup and upkeep

Beyond the [README's Getting started](../README.md#getting-started): other kinds of AWS accounts,
keeping the machine image up to date, doing the setup by hand, and removing everything.

- [University and company accounts](#university-and-company-accounts)
- [Updating the machine image](#updating-the-machine-image)
- [Setting up by hand](#setting-up-by-hand)
- [Without the machine image](#without-the-machine-image)
- [Costs and removing everything](#costs-and-removing-everything)

## University and company accounts

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
by `terraform -chdir=terraform output -raw launcher_policy_json` (see [IAM](reference.md#iam)), which IT can
attach to your login.

**No default network.** Some accounts have no default subnets, so `setup.sh` stops and says so.
Ask IT which subnets to use and pass them with `--subnets subnet-a,subnet-b`. The machines need
outbound internet (to pull images and install conda packages): a public subnet, or a private one
with a NAT gateway (for Fargate jobs there, also set `aws-batch-flex-assign-public-ip: DISABLED`
in the profile).

## Updating the machine image

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

## Setting up by hand

What `scripts/setup.sh` does, step by step.

### Find your subnets

AWS machines start inside a "subnet", a slice of your account's network. New accounts come with
default subnets that have internet access, which is what's needed here (to download container
images). List them:

```bash
aws ec2 describe-subnets --filters Name=default-for-az,Values=true --query 'Subnets[].SubnetId' --output text
```

You'll get two or more IDs like `subnet-0a1b2c3d`. Note them down. If the list is empty, your
account has no default network; create one with `aws ec2 create-default-vpc` and run the command
again.

### Build the machine image

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

### Create the AWS resources

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

### Make a profile

Terraform prints a profile for you; install it under the name `smkflex`:

```bash
mkdir -p ~/.config/snakemake/smkflex
terraform -chdir=terraform output -raw snakemake_profile > ~/.config/snakemake/smkflex/config.yaml
echo "jobs: 100" >> ~/.config/snakemake/smkflex/config.yaml
cat ~/.config/snakemake/smkflex/config.yaml
```

(With conda in the image, also add `aws-batch-flex-conda-path: /opt/miniforge3`.) You should see
lines like `executor: aws-batch-flex` and `aws-batch-flex-queue: smkflex-queue`.

## Without the machine image

To try the plugin on an existing Batch queue without building an AMI, set
`aws-batch-flex-bootstrap: auto` in the profile instead of `host`. Snakemake is then installed into
each container when its job starts (about 10-15 s per job), which needs internet access and
`curl`, `wget` or `pip` in the image (`--aws-batch-flex-install-prereqs` installs `curl` first if
missing).

Or install the Snakemake env on each instance as it boots, with the stock ECS image and no AMI of
your own: every new instance then spends about a minute on it before its first job (not yet
tried on AWS). Make the instances' user-data with it, keeping the plugin's own boot script:

```bash
snakemake-batch-flex-host-env userdata --storage-plugin s3 \
  --include terraform/userdata.snakemake.txt > terraform/userdata.mime
```

and set `userdata_file = "userdata.mime"` and the stock ECS AMI as `ami_id` in `terraform.tfvars`.

## Your own user-data

The instances boot with `terraform/userdata.snakemake.txt`, which sets the ECS agent up for big
images and for stopping jobs cleanly. To run your own setup at boot as well (a site's agent, a
mount, a proxy), merge the two rather than replacing ours: `--include` keeps each file as it is
(a script, a `#cloud-config`, or a multipart document) and packs them into the one document EC2
takes:

```bash
snakemake-batch-flex-host-env userdata --storage-plugin s3 \
  --include terraform/userdata.snakemake.txt --include my-setup.sh > terraform/userdata.mime
```

That also installs the Snakemake env at boot (see above). On an AMI baked with the same versions
(made from the same machine, before its Snakemake changed) the installer finds it ready and does
nothing; otherwise it installs this machine's versions over it, about a minute per instance.
Then set `userdata_file = "userdata.mime"` in
`terraform.tfvars`, and `terraform plan` / `apply`.

## Costs and removing everything

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
