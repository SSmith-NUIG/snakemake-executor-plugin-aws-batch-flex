variable "region" {
  type = string
}

variable "name" {
  description = "Prefix for every resource created here. <name>-queue must not already exist."
  type        = string
  default     = "smkflex"
}

# inputs that are only read
variable "subnet_ids" {
  description = "Existing subnets to launch into. They are only referenced; nothing in them or their VPC is changed."
  type        = list(string)
}

variable "ami_id" {
  description = "AMI for the instances: the standalone copy made by scripts/bake-ami.sh."
  type        = string
}

variable "userdata_file" {
  description = <<-EOT
    Boot-time user-data for the instances. userdata.snakemake.txt waits for Batch to set
    ECS_CLUSTER, adds two ECS agent settings and starts the agent. Null: none.
    If the AMI was not baked with the Snakemake env, generate user-data that installs it at boot
    (--include merges in any user-data you already use):
      snakemake-batch-flex-host-env userdata --storage-plugin s3 [--include existing.txt] > userdata.mime
  EOT
  type        = string
  default     = "userdata.snakemake.txt"
}

# things created new, with optional overrides
variable "extra_security_group_ids" {
  description = "Additional security groups. A new egress-only group is always created."
  type        = list(string)
  default     = []
}

variable "instance_role_policy_arns" {
  description = "Extra managed policies for the instance role (it always gets the ECS one, which includes ECR pulls)."
  type        = list(string)
  default     = []
}

variable "s3_bucket" {
  description = "Null (default): create a new bucket named <name>-<account>-<region>. Set to use an existing one."
  type        = string
  default     = null
}

variable "s3_prefix" {
  type    = string
  default = "snakemake"
}

variable "s3_expire_days" {
  description = "Delete files under s3_prefix this many days after they were written (only on a bucket this module creates). Null: keep them."
  type        = number
  default     = null
}

variable "log_retention_days" {
  description = "Job logs go to their own log group, /aws/batch/<name>, kept this many days. Null: use Batch's default group /aws/batch/job (kept forever)."
  type        = number
  default     = 30
}

variable "job_role_arn" {
  description = "Null: create a new job role with access to the S3 prefix."
  type        = string
  default     = null
}

variable "jobs_use_instance_role" {
  description = <<-EOT
    Let jobs reach the instance role (IMDS hop limit 2) and give that role the S3 prefix, for
    jobs without a job role. Off: the jobs use only their job role, and a job can't take the
    instances' own credentials.
  EOT
  type        = bool
  default     = false
}

variable "batch_service_role_arn" {
  description = "Null uses the AWS Batch service-linked role (shared account-level role)."
  type        = string
  default     = null
}

# capacity / launch template
variable "ecs_image_type" {
  type    = string
  default = "ECS_AL2023"
}


variable "instance_types" {
  type    = list(string)
  default = ["optimal"]
}

variable "min_vcpus" {
  type    = number
  default = 0
}

variable "max_vcpus" {
  type    = number
  default = 256
}

variable "root_device_name" {
  type    = string
  default = "/dev/xvda"
}

variable "root_volume_gib" {
  description = "Null keeps the AMI's size."
  type        = number
  default     = null
}

variable "root_volume_iops" {
  type    = number
  default = null
}

variable "root_volume_throughput" {
  type    = number
  default = null
}

variable "root_volume_encrypted" {
  type    = bool
  default = false
}

variable "tags" {
  type    = map(string)
  default = {}
}
