# Optional Fargate queue (install-at-start mode; no host, so no AMI and no host mount).
# Off by default; set enable_fargate = true in terraform.tfvars.

variable "enable_fargate" {
  description = "Also create a Fargate compute environment + queue + execution role."
  type        = bool
  default     = false
}

variable "fargate_max_vcpus" {
  type    = number
  default = 32
}

# Fargate pulls images and writes logs through this role (not the job role).
resource "aws_iam_role" "fargate_exec" {
  count              = var.enable_fargate ? 1 : 0
  name_prefix        = "${var.name}-fargate-exec-"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "fargate_exec" {
  count      = var.enable_fargate ? 1 : 0
  role       = aws_iam_role.fargate_exec[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_batch_compute_environment" "fargate" {
  count        = var.enable_fargate ? 1 : 0
  name_prefix  = "${var.name}-fargate-"
  type         = "MANAGED"
  state        = "ENABLED"
  service_role = var.batch_service_role_arn

  compute_resources {
    type               = "FARGATE"
    max_vcpus          = var.fargate_max_vcpus
    subnets            = var.subnet_ids
    security_group_ids = concat([aws_security_group.batch.id], var.extra_security_group_ids)
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_batch_job_queue" "fargate" {
  count    = var.enable_fargate ? 1 : 0
  name     = "${var.name}-fargate-queue"
  state    = "ENABLED"
  priority = 1
  tags     = local.tags

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.fargate[0].arn
  }
}

output "snakemake_profile_fargate" {
  description = "Profile for the Fargate queue (install-at-start; needs internet via a public IP)."
  value = var.enable_fargate ? join("\n", concat([
    "executor: aws-batch-flex",
    "default-storage-provider: s3",
    "default-storage-prefix: s3://${local.bucket}/${var.s3_prefix}",
    "aws-batch-flex-queue: ${aws_batch_job_queue.fargate[0].name}",
    "aws-batch-flex-region: ${var.region}",
    "aws-batch-flex-platform: fargate",
    "aws-batch-flex-job-role-arn: ${local.job_role_arn}",
    "aws-batch-flex-execution-role-arn: ${aws_iam_role.fargate_exec[0].arn}",
    "aws-batch-flex-head-role-arn: ${aws_iam_role.head.arn}",
    "aws-batch-flex-bootstrap: auto",
    "aws-batch-flex-install-prereqs: true",
    "aws-batch-flex-default-image: buildpack-deps:bookworm-scm",
    "default-resources: [mem_mb=512]",
    "jobs: 5",
  ], local.log_group == null ? [] : ["aws-batch-flex-log-group: ${local.log_group}"])) : "enable_fargate is false"
}
