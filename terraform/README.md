# Terraform

Creates a Batch compute environment and job queue, a launch template, a security group (egress
only), the instance, job and head-job roles, a CloudWatch log group and an S3 bucket. Everything
is new and tagged `ManagedBy=snakemake-batch-flex`; the only existing things it reads are your
subnets and the AMI ID you give it. `fargate.tf` adds an optional Fargate queue
(`enable_fargate = true`) and `spot.tf` an optional Spot queue (`enable_spot_queue = true`).

`scripts/setup.sh` runs all of this for you. To do it by hand, see
[Setting up by hand](../docs/setup.md#setting-up-by-hand); in short:

```bash
# 1. Bake an AMI: the ECS-optimized AMI plus /opt/snakemake-env (10-20 minutes)
bash ../scripts/bake-ami.sh --base-ami <ecs-optimized ami> --subnet <public subnet> --region <region>

# 2. Copy terraform.tfvars.example to terraform.tfvars and fill it in, then:
terraform init
terraform plan        # should only add resources
terraform apply
mkdir -p ~/.config/snakemake/smkflex
terraform output -raw snakemake_profile > ~/.config/snakemake/smkflex/config.yaml
```

Instances boot with `userdata.snakemake.txt`: it waits for Batch to write `ECS_CLUSTER`, then
adds three ECS agent settings and starts the agent. To add your own boot steps, see
[Your own user-data](../docs/setup.md#your-own-user-data).

With a baked AMI you can set `aws-batch-flex-host-env-wait-seconds: 0` in the profile.

## Variables

Set in `terraform.tfvars` (`terraform.tfvars.example` has the usual ones). Only the first three
are required.

| Variable | Value | Default | What it does |
|---|---|---|---|
| `region` | AWS region code, e.g. `"eu-west-1"` | *required* | Where everything is created. |
| `subnet_ids` | list of subnet IDs | *required* | Subnets the instances start in; they need outbound internet access. Only referenced, never changed. |
| `ami_id` | AMI ID | *required* | The AMI made by `scripts/bake-ami.sh` (or the stock ECS-optimized AMI with install-at-boot user-data). |
| `name` | text | `"smkflex"` | Prefix for every resource; `<name>-queue` must not already exist. |
| `root_volume_gib` | GiB | *the AMI's size (30 GiB on the ECS-optimized AMI)* | Disk per instance, shared by its jobs (images and job files). `setup.sh` and the example set `100`. |
| `root_volume_iops`, `root_volume_throughput` | number | *gp3 defaults* | Faster disk for I/O-heavy jobs. |
| `root_volume_encrypted` | `true` or `false` | `false` | Encrypt the instances' disks. |
| `root_device_name` | device name | `"/dev/xvda"` | Only for an AMI whose root device has another name. |
| `instance_types` | list of EC2 types or families | `["optimal"]` | Instance types Batch may start, e.g. `["c6i", "r6i"]`. A job bigger than the largest of them never starts. |
| `min_vcpus`, `max_vcpus` | whole numbers | `0`, `256` | vCPUs kept running when idle, and the most running at once (caps cost). |
| `ecs_image_type` | `"ECS_AL2023"` or another Batch image type | `"ECS_AL2023"` | Must match the AMI's base. |
| `enable_spot_queue` | `true` or `false` | `false` | Also create a Spot compute environment and `<name>-spot-queue`. |
| `spot_max_vcpus` | whole number | `256` | `max_vcpus` for the Spot queue. |
| `enable_fargate` | `true` or `false` | `false` | Also create a Fargate queue and execution role, and print a Fargate profile. |
| `fargate_max_vcpus` | whole number | `32` | `max_vcpus` for the Fargate queue. |
| `enable_secrets` | `true` or `false` | `false` | Create the execution role that lets jobs read SSM parameters and secrets under `<name>/` (for `aws-batch-flex-secrets`), on EC2 too. |
| `s3_bucket` | bucket name | *a new bucket, `<name>-<account>-<region>`* | Use an existing bucket instead. |
| `s3_prefix` | text | `"snakemake"` | The prefix in the bucket that workflows use and the job role can access. |
| `s3_expire_days` | days | *keep files* | Delete files under the prefix this many days after they were written (only on a bucket this module creates). |
| `log_retention_days` | days, or `null` | `30` | Job logs go to `/aws/batch/<name>` and are kept this long. `null`: use Batch's default group, which keeps them forever. |
| `job_role_arn` | IAM role ARN | *a new role with access to the prefix* | Use an existing job role instead. |
| `jobs_use_instance_role` | `true` or `false` | `false` | Let jobs fall back to the instance role (and give it the S3 prefix). Off, a job can't take the instances' own credentials. |
| `instance_role_policy_arns` | list of policy ARNs | `[]` | Extra managed policies for the instance role. |
| `extra_security_group_ids` | list of security group IDs | `[]` | Extra security groups for the instances, besides the egress-only one created here. |
| `batch_service_role_arn` | IAM role ARN | *the Batch service-linked role* | Only if your account requires a specific Batch service role. |
| `userdata_file` | file name in this folder, or `null` | `"userdata.snakemake.txt"` | The instances' boot script; see [Your own user-data](../docs/setup.md#your-own-user-data). |
| `tags` | map of tags | `{}` | Extra tags on every resource (`ManagedBy=snakemake-batch-flex` is always added). |

## Removing it

`terraform destroy` removes what `terraform output created` lists. It fails on the bucket while
the bucket still has files in it, so empty the bucket first. The AMI is made by the bake script,
not Terraform: remove it with `scripts/cleanup-ami.sh`, which only deletes AMIs tagged
`ManagedBy=snakemake-batch-flex`, and never one that a compute environment, launch template or
instance still uses, or the one in `terraform.tfvars` (after `terraform destroy`, pass
`--no-tfvars` to skip that last check). See
[Costs and removing everything](../docs/setup.md#costs-and-removing-everything).

## Notes

State is a local file in this directory (no backend block). Needs AWS provider v6. Pick a
`name` whose `<name>-queue` doesn't already exist. Instances need outbound internet access to
pull images, and jobs need it in install-at-start mode. Extra instance policies go in
`instance_role_policy_arns`.
