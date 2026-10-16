output "instance_id" {
  value = aws_instance.this.id
}

output "connect_with_cli" {
  description = "Needs the AWS CLI Session Manager plugin on your laptop; otherwise use EC2 console -> Connect -> Session Manager."
  value       = "aws ssm start-session --target ${aws_instance.this.id} --region ${var.region}"
}

output "console_url" {
  value = "https://${var.region}.console.aws.amazon.com/systems-manager/session-manager/${aws_instance.this.id}?region=${var.region}"
}

output "next_steps" {
  value = <<-EOT
    1. Wait ~3 min for first-boot setup, then connect (see connect_with_cli / EC2 console).
    2. sudo -iu ec2-user     (new shell: venv active, in examples/smoke-test)
    3. ls ~/.snakemake-setup-done || sudo tail -50 /var/log/snakemake-setup.log
    4. snakemake --profile profile --verbose
  EOT
}
