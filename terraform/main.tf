# Everything here is created new. Existing resources are only read: the subnets and the AMI
# you pass in.

data "aws_caller_identity" "me" {}

data "aws_subnet" "first" {
  id = var.subnet_ids[0]
}

locals {
  tags         = merge({ ManagedBy = "snakemake-batch-flex" }, var.tags)
  userdata_b64 = var.userdata_file == null ? null : filebase64(var.userdata_file)
  bucket       = coalesce(var.s3_bucket, one(aws_s3_bucket.work[*].id))
  job_role_arn = coalesce(var.job_role_arn, one(aws_iam_role.job[*].arn))
}

# S3 work bucket (new unless you pass one)
resource "aws_s3_bucket" "work" {
  count  = var.s3_bucket == null ? 1 : 0
  bucket = "${var.name}-${data.aws_caller_identity.me.account_id}-${var.region}"
  tags   = local.tags
}

resource "aws_s3_bucket_public_access_block" "work" {
  count                   = var.s3_bucket == null ? 1 : 0
  bucket                  = aws_s3_bucket.work[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "s3_access" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["arn:aws:s3:::${local.bucket}/${var.s3_prefix}/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${local.bucket}"]
  }
}

# instance role + profile
data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name_prefix        = "${var.name}-instance-"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "instance_ecs" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "instance_extra" {
  for_each   = toset(var.instance_role_policy_arns)
  role       = aws_iam_role.instance.name
  policy_arn = each.value
}

# fallback credentials for jobs that run without a job role
resource "aws_iam_role_policy" "instance_s3" {
  name   = "s3-work-prefix"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.s3_access.json
}

resource "aws_iam_instance_profile" "instance" {
  name_prefix = "${var.name}-instance-"
  role        = aws_iam_role.instance.name
  tags        = local.tags
}

# security group (new, egress only)
resource "aws_security_group" "batch" {
  name_prefix = "${var.name}-batch-"
  description = "Snakemake Batch instances: egress only"
  vpc_id      = data.aws_subnet.first.vpc_id
  tags        = merge(local.tags, { Name = "${var.name}-batch" })

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# launch template (new)
resource "aws_launch_template" "this" {
  name_prefix            = "${var.name}-batch-"
  image_id               = var.ami_id
  user_data              = local.userdata_b64
  update_default_version = true

  # hop limit 2 lets containers reach the instance role through IMDS
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  dynamic "block_device_mappings" {
    for_each = var.root_volume_gib == null ? [] : [1]
    content {
      device_name = var.root_device_name
      ebs {
        volume_size           = var.root_volume_gib
        volume_type           = "gp3"
        iops                  = var.root_volume_iops
        throughput            = var.root_volume_throughput
        encrypted             = var.root_volume_encrypted
        delete_on_termination = true
      }
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags          = merge(local.tags, { Name = "${var.name}-batch" })
  }
}

# compute environment + queue (new)
resource "aws_batch_compute_environment" "this" {
  name_prefix                      = "${var.name}-"
  type                            = "MANAGED"
  state                           = "ENABLED"
  service_role                    = var.batch_service_role_arn

  compute_resources {
    type                = var.use_spot ? "SPOT" : "EC2"
    allocation_strategy = var.use_spot ? "SPOT_CAPACITY_OPTIMIZED" : "BEST_FIT_PROGRESSIVE"
    min_vcpus           = var.min_vcpus
    max_vcpus           = var.max_vcpus
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
      # explicit version so a changed template replaces the environment (no stale $Latest)
      version = tostring(aws_launch_template.this.latest_version)
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

resource "aws_batch_job_queue" "this" {
  name     = "${var.name}-queue"
  state    = "ENABLED"
  priority = 1
  tags     = local.tags

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.this.arn
  }
}

# job role (new unless you pass one)
data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "job" {
  count              = var.job_role_arn == null ? 1 : 0
  name_prefix        = "${var.name}-job-"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "job_s3" {
  count  = var.job_role_arn == null ? 1 : 0
  name   = "s3-access"
  role   = aws_iam_role.job[0].id
  policy = data.aws_iam_policy_document.s3_access.json
}

# policy for whoever runs snakemake, scoped to this queue
data "aws_iam_policy_document" "launcher" {
  statement {
    actions = ["batch:SubmitJob"]
    resources = [
      aws_batch_job_queue.this.arn,
      "arn:aws:batch:${var.region}:${data.aws_caller_identity.me.account_id}:job-definition/snakemake-*",
    ]
  }
  statement {
    actions = [
      "batch:DescribeJobs", "batch:TerminateJob", "batch:RegisterJobDefinition",
      "batch:DescribeJobDefinitions", "batch:DescribeJobQueues", "batch:TagResource",
    ]
    resources = ["*"]
  }
  statement {
    actions   = ["logs:GetLogEvents"]
    resources = ["*"]
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [local.job_role_arn]
  }
  source_policy_documents = [data.aws_iam_policy_document.s3_access.json]
}
