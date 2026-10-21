# Optional Spot queue next to the on-demand one: same AMI, launch template and roles, but
# instances from AWS's spare capacity, usually 60-90% cheaper. AWS can take a Spot instance
# back with two minutes' warning; Batch then retries the job (the plugin's spot-retries).
# Send rules here with resources: batch_queue="<name>-spot-queue", or make it the profile's
# queue and send long jobs (and smkflex head jobs) to the on-demand one.
# Off by default; set enable_spot_queue = true in terraform.tfvars.

variable "enable_spot_queue" {
  description = "Also create a Spot compute environment + queue."
  type        = bool
  default     = false
}

variable "spot_max_vcpus" {
  type    = number
  default = 256
}

resource "aws_batch_compute_environment" "spot" {
  count        = var.enable_spot_queue ? 1 : 0
  name_prefix  = "${var.name}-spot-"
  type         = "MANAGED"
  state        = "ENABLED"
  service_role = var.batch_service_role_arn

  compute_resources {
    type = "SPOT"
    # cheap pools that are also least likely to be taken back
    allocation_strategy = "SPOT_PRICE_CAPACITY_OPTIMIZED"
    min_vcpus           = 0
    max_vcpus           = var.spot_max_vcpus
    instance_type       = var.instance_types
    instance_role       = aws_iam_instance_profile.instance.arn
    subnets             = var.subnet_ids
    security_group_ids  = concat([aws_security_group.batch.id], var.extra_security_group_ids)
    tags                = local.tags

    ec2_configuration {
      image_type        = var.ecs_image_type
      image_id_override = var.ami_id
    }

    launch_template {
      launch_template_id = aws_launch_template.this.id
      version            = tostring(aws_launch_template.this.latest_version)
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.instance_ecs,
    aws_iam_role_policy_attachment.instance_extra,
  ]

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_batch_job_queue" "spot" {
  count    = var.enable_spot_queue ? 1 : 0
  name     = "${var.name}-spot-queue"
  state    = "ENABLED"
  priority = 1
  tags     = local.tags

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.spot[0].arn
  }
}

output "spot_queue_name" {
  value = var.enable_spot_queue ? aws_batch_job_queue.spot[0].name : "enable_spot_queue is false"
}
