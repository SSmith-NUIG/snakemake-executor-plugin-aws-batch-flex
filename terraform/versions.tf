terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0" # v6 renamed aws_batch_compute_environment name arguments
    }
  }
}

provider "aws" {
  region = var.region
}
