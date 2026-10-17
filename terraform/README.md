# Terraform

This directory creates a Batch compute environment, job queue, launch template, security
group (egress only), instance and job roles, and an S3 bucket. Everything is new and tagged
`ManagedBy=snakemake-batch-flex`; the only existing things it reads are your subnets and the
AMI id you give it. `fargate.tf` adds an optional Fargate queue (`enable_fargate = true`).

`launcher/` is an optional small EC2 instance to run `snakemake` from. It hasn't been applied
yet, so treat it as a draft.

## Steps

```bash
# 1. Bake an AMI: a stock ECS-optimized AMI plus /opt/snakemake-env (takes 5-10 min)
bash ../scripts/bake-ami.sh --base-ami <ecs-optimized ami> --subnet <public subnet> --region <region>

# 2. Copy terraform.tfvars.example to terraform.tfvars and fill it in, then
terraform init
terraform plan        # only add resources should show up
terraform apply
terraform output -raw snakemake_profile > ../examples/conformance/profile/config.yaml
```

The instances boot with `userdata.snakemake.txt`. it waits for Batch to write `ECS_CLUSTER`,
then adds two ECS agent settings and starts the agent.

With a baked AMI you can set `aws-batch-flex-host-env-wait-seconds: 0` in the profile.

## Removing it

`terraform destroy` removes what's in `terraform output created`. It fails on the bucket while
the bucket still has data in it so empty the bucket first. The AMI is made by the bake script not Terraform. remove old
AMIs with `scripts/cleanup-ami.sh` This should only delete AMIs tagged `ManagedBy=snakemake-batch-flex`
and never one that a compute environment, launch template or instance is still using, or the one
in `terraform.tfvars` (after `terraform destroy`, pass `--no-tfvars` to skip that last check).

## Notes

State is a local file in this directory (no backend block). Needs AWS provider v6. Pick a
`name` whose `<name>-queue` doesn't already exist. Instances need outbound internet to pull
images, and jobs need it in install-at-start mode. Extra instance policies go in
`instance_role_policy_arns`.

