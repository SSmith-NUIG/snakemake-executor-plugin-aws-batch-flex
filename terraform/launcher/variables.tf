variable "region" {
  description = "AWS region of your Batch queue."
  type        = string
}

variable "name" {
  type    = string
  default = "snakemake-launcher"
}

variable "subnet_id" {
  description = "Subnet for the launcher. Needs outbound internet (public subnet with auto-assign public IP, or a private subnet behind a NAT gateway)."
  type        = string
}

variable "assign_public_ip" {
  description = "Set false when the subnet is private (NAT)."
  type        = bool
  default     = true
}

variable "instance_type" {
  type    = string
  default = "t3.small"
}

variable "batch_queue_name" {
  description = "Batch job queue the workflows submit to."
  type        = string
}

variable "s3_bucket" {
  description = "Existing bucket used for the plugin zip and as Snakemake's workdir."
  type        = string
}

variable "s3_workdir_prefix" {
  type    = string
  default = "smk-test"
}

variable "plugin_zip_path" {
  description = "Path on THIS machine to snakemake-executor-plugin-aws-batch-flex.zip."
  type        = string
  default     = "./snakemake-executor-plugin-aws-batch-flex.zip"
}

variable "job_role_arn" {
  description = "Optional job role to pass to Batch jobs. Null: jobs use the compute environment's instance profile."
  type        = string
  default     = null
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "snakemake_version" {
  description = "Must equal the Snakemake version baked into the Batch AMI (the host env), or jobs and launcher disagree. Check: snakemake --version in the env you ran bake-ami.sh from."
  type        = string
  default     = "9.27.0"
}

variable "s3_plugin_version" {
  description = "snakemake-storage-plugin-s3 version baked into the AMI."
  type        = string
  default     = "0.3.6"
}
