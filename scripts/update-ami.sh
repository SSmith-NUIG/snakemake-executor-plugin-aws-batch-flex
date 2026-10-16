#!/usr/bin/env bash
# Add conda environments (or refresh the host env) without remaking the queue: bakes a new AMI on top of
# the current one, points terraform.tfvars at it and shows the plan. Run from the repo root.
#   bash scripts/update-ami.sh --subnet subnet-abc --region eu-west-1 --conda-env envs/rnaseq.yaml
# Applying replaces the compute environment only; the job queue and everything else stay. Do it when no jobs run.
set -euo pipefail
TF=terraform/terraform.tfvars
CUR=$(sed -n 's/^ami_id *= *"\(ami-[^"]*\)".*/\1/p' "$TF")
[ -n "$CUR" ] || { echo "no ami_id in $TF" >&2; exit 1; }
echo "current Snakemake AMI: $CUR"
OUT=$(bash scripts/bake-ami.sh --base-ami "$CUR" "$@" | tee /dev/stderr)
NEW=$(echo "$OUT" | sed -n 's/^DONE\. New standalone AMI: \(ami-[^ ]*\).*/\1/p')
[ -n "$NEW" ] || { echo "bake did not finish; tfvars unchanged" >&2; exit 1; }
cp "$TF" "$TF.bak-$(date +%s)"
sed -i "s/^ami_id *=.*/ami_id = \"$NEW\"/" "$TF"
echo; echo "ami_id is now $NEW (backup kept). Plan:"; (cd terraform && terraform plan | tail -15)
echo; echo "If the plan only replaces the compute environment/launch template: cd terraform && terraform apply"
