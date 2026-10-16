data "aws_caller_identity" "me" {}

data "aws_subnet" "this" {
  id = var.subnet_id
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

locals {
  zip_key = "tools/${basename(var.plugin_zip_path)}"
}

# plugin zip -> S3 (the instance pulls it from here on first boot)
resource "aws_s3_object" "plugin" {
  bucket = var.s3_bucket
  key    = local.zip_key
  source = var.plugin_zip_path
  etag   = filemd5(var.plugin_zip_path)
  tags   = var.tags
}

# IAM: Session Manager access + what the plugin needs to submit/track jobs
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "access" {
  # SubmitJob only on the Snakemake queue and snakemake-* job definitions: this identity
  # cannot submit to any other queue in the account.
  statement {
    actions = ["batch:SubmitJob"]
    resources = [
      "arn:aws:batch:${var.region}:${data.aws_caller_identity.me.account_id}:job-queue/${var.batch_queue_name}",
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
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
    ]
    resources = ["arn:aws:s3:::${var.s3_bucket}/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.s3_bucket}"]
  }
  dynamic "statement" {
    for_each = var.job_role_arn == null ? [] : [1]
    content {
      actions   = ["iam:PassRole"]
      resources = [var.job_role_arn]
    }
  }
}

resource "aws_iam_role" "this" {
  name_prefix        = "${var.name}-"
  assume_role_policy = data.aws_iam_policy_document.assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "access" {
  name   = "snakemake-launcher-access"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.access.json
}

resource "aws_iam_instance_profile" "this" {
  name_prefix = "${var.name}-"
  role        = aws_iam_role.this.name
  tags        = var.tags
}

# network: no inbound at all (connect with Session Manager)
resource "aws_security_group" "this" {
  name_prefix = "${var.name}-"
  description = "Snakemake launcher: egress only"
  vpc_id      = data.aws_subnet.this.vpc_id
  tags        = merge(var.tags, { Name = var.name })

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# the instance
resource "aws_instance" "this" {
  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.this.id]
  iam_instance_profile        = aws_iam_instance_profile.this.name
  associate_public_ip_address = var.assign_public_ip
  user_data_replace_on_change = true # new plugin zip => fresh instance

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    bucket     = var.s3_bucket
    key        = local.zip_key
    region     = var.region
    queue      = var.batch_queue_name
    workdir    = "s3://${var.s3_bucket}/${var.s3_workdir_prefix}"
    job_role   = var.job_role_arn == null ? "" : var.job_role_arn
    snakemake_version = var.snakemake_version
    s3_plugin_version = var.s3_plugin_version
    zip_md5    = filemd5(var.plugin_zip_path)
  })

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  tags = merge(var.tags, { Name = var.name })

  depends_on = [aws_s3_object.plugin, aws_iam_role_policy.access, aws_iam_role_policy_attachment.ssm]
}
