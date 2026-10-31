output "queue_name" {
  value = aws_batch_job_queue.this.name
}

output "s3_bucket" {
  value = local.bucket
}

output "job_role_arn" {
  value = local.job_role_arn
}

output "head_role_arn" {
  description = "Role for the head job of `smkflex run`."
  value       = aws_iam_role.head.arn
}

output "created" {
  description = "Everything this module created (all new, all tagged ManagedBy=snakemake-batch-flex)."
  value = {
    compute_environment = aws_batch_compute_environment.this.arn
    job_queue           = aws_batch_job_queue.this.arn
    launch_template     = aws_launch_template.this.id
    security_group      = aws_security_group.batch.id
    instance_role       = aws_iam_role.instance.arn
    instance_profile    = aws_iam_instance_profile.instance.arn
    head_role           = aws_iam_role.head.arn
    log_group           = local.log_group
  }
}

output "launcher_policy_json" {
  description = "Attach to the user/role that runs `snakemake`. SubmitJob is limited to this module's queues."
  value       = data.aws_iam_policy_document.launcher.json
}

output "snakemake_profile" {
  description = "Paste into profile/config.yaml."
  value       = <<-EOT
    executor: aws-batch-flex
    default-storage-provider: s3
    default-storage-prefix: s3://${local.bucket}/${var.s3_prefix}
    aws-batch-flex-queue: ${aws_batch_job_queue.this.name}
    aws-batch-flex-region: ${var.region}
    aws-batch-flex-job-role-arn: ${local.job_role_arn}
    aws-batch-flex-head-role-arn: ${aws_iam_role.head.arn}
    aws-batch-flex-bootstrap: host
    aws-batch-flex-host-env-wait-seconds: 180
    aws-batch-flex-default-image: buildpack-deps:bookworm-scm%{if var.enable_secrets}
    aws-batch-flex-execution-role-arn: ${aws_iam_role.fargate_exec[0].arn}%{endif}%{if local.log_group != null}
    aws-batch-flex-log-group: ${local.log_group}%{endif}
  EOT
}
