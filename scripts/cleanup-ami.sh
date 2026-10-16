#!/usr/bin/env bash
# Delete Snakemake-created AMIs and their snapshots. Enforced safety rules:
#   * only AMIs tagged ManagedBy=snakemake-batch-flex (anything else, e.g. another stack's AMI, is refused)
#   * never the ami_id currently in terraform/terraform.tfvars, nor anything passed with --keep
#   * lists what will go and asks for 'yes' (skip with --yes)
#   bash scripts/cleanup-ami.sh --region eu-west-1 ami-aaaa ami-bbbb
set -euo pipefail
AWS=${AWS:-aws}
REGION=""; KEEP=(); IDS=(); YES=0; TF=${TFVARS:-terraform/terraform.tfvars}
while [ $# -gt 0 ]; do
  case "$1" in
    --region) REGION=$2; shift 2;;
    --keep) KEEP+=("$2"); shift 2;;
    --tfvars) TF=$2; shift 2;;
    --yes) YES=1; shift;;
    ami-*) IDS+=("$1"); shift;;
    *) echo "usage: cleanup-ami.sh --region R [--keep ami-x] [--yes] ami-id..." >&2; exit 2;;
  esac
done
[ -n "$REGION" ] && [ ${#IDS[@]} -gt 0 ] || { echo "need --region and at least one ami id" >&2; exit 2; }
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION AWS_PAGER=""
if [ -f "$TF" ]; then
  CUR=$(sed -n 's/^ami_id *= *"\(ami-[^"]*\)".*/\1/p' "$TF"); [ -n "$CUR" ] && KEEP+=("$CUR")
fi
PLAN=()
for id in "${IDS[@]}"; do
  for k in "${KEEP[@]:-}"; do
    [ "$id" = "$k" ] && { echo "REFUSED $id: it is the AMI in use (or marked --keep)" >&2; exit 1; }
  done
  tag=$($AWS ec2 describe-images --image-ids "$id" --query 'Images[0].Tags[?Key==`ManagedBy`].Value|[0]' --output text)
  [ "$tag" = "snakemake-batch-flex" ] || { echo "REFUSED $id: not tagged ManagedBy=snakemake-batch-flex" >&2; exit 1; }
  name=$($AWS ec2 describe-images --image-ids "$id" --query 'Images[0].Name' --output text)
  snaps=$($AWS ec2 describe-images --image-ids "$id" --query 'Images[0].BlockDeviceMappings[].Ebs.SnapshotId' --output text)
  echo "will delete: $id ($name)  snapshots: $snaps"
  PLAN+=("$id|$snaps")
done
echo "protected (kept): ${KEEP[*]:-none}"
if [ "$YES" != 1 ]; then read -r -p "Type yes to delete the above: " ans; [ "$ans" = yes ] || { echo "aborted"; exit 1; }; fi
for item in "${PLAN[@]}"; do
  id=${item%%|*}; snaps=${item#*|}
  $AWS ec2 deregister-image --image-id "$id"
  for s in $snaps; do $AWS ec2 delete-snapshot --snapshot-id "$s"; done
  echo "deleted $id and its snapshots"
done
