#!/usr/bin/env bash
# Delete Snakemake-created AMIs and their snapshots. Enforced safety rules:
#   * only AMIs tagged ManagedBy=snakemake-batch-flex (anything else, e.g. another stack's AMI, is refused)
#   * never an AMI that is in use: by a Batch compute environment (directly or through the launch
#     template version it uses), by the default or latest version of any launch template, or by an
#     instance that still exists. If that can't be checked, nothing is deleted.
#   * never the ami_id currently in terraform/terraform.tfvars (skip with --no-tfvars), nor anything passed with --keep
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
    --no-tfvars) TF=""; shift;;   # e.g. after terraform destroy; the in-use checks still run
    --yes) YES=1; shift;;
    ami-*) IDS+=("$1"); shift;;
    *) echo "usage: cleanup-ami.sh --region R [--keep ami-x] [--no-tfvars] [--yes] ami-id..." >&2; exit 2;;
  esac
done
[ -n "$REGION" ] && [ ${#IDS[@]} -gt 0 ] || { echo "need --region and at least one ami id" >&2; exit 2; }
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION AWS_PAGER=""
if [ -n "$TF" ] && [ -f "$TF" ]; then
  CUR=$(sed -n 's/^ami_id *= *"\(ami-[^"]*\)".*/\1/p' "$TF"); [ -n "$CUR" ] && KEEP+=("$CUR")
fi
fail() { echo "REFUSED: could not check whether the AMIs are in use ($1); nothing deleted" >&2; exit 1; }
USED=" "
out=$($AWS batch describe-compute-environments --output text \
  --query 'computeEnvironments[].computeResources.[imageId, ec2Configuration[].imageIdOverride]') \
  || fail "batch describe-compute-environments"
USED+="$(echo $out) "
# launch template versions that compute environments point at
lts=$($AWS batch describe-compute-environments --output text \
  --query 'computeEnvironments[].computeResources.launchTemplate.[launchTemplateId || launchTemplateName, version]') \
  || fail "batch describe-compute-environments"
while read -r lt ver; do
  [ -n "$lt" ] && [ "$lt" != None ] || continue
  [ -n "$ver" ] && [ "$ver" != None ] || ver='$Default'
  case "$lt" in lt-*) sel=(--launch-template-id "$lt");; *) sel=(--launch-template-name "$lt");; esac
  out=$($AWS ec2 describe-launch-template-versions "${sel[@]}" --versions "$ver" --output text \
    --query 'LaunchTemplateVersions[].LaunchTemplateData.ImageId') || fail "describe-launch-template-versions $lt"
  USED+="$(echo $out) "
done <<< "$lts"
# the current versions of every launch template
for lt in $($AWS ec2 describe-launch-templates --query 'LaunchTemplates[].LaunchTemplateId' --output text || echo FAIL); do
  [ "$lt" = FAIL ] && fail "describe-launch-templates"
  out=$($AWS ec2 describe-launch-template-versions --launch-template-id "$lt" --versions '$Default' '$Latest' \
    --output text --query 'LaunchTemplateVersions[].LaunchTemplateData.ImageId') || fail "describe-launch-template-versions $lt"
  USED+="$(echo $out) "
done
for id in "${IDS[@]}"; do
  case "$USED" in *" $id "*)
    echo "REFUSED $id: it is in use by a Batch compute environment or launch template" >&2; exit 1;;
  esac
  n=$($AWS ec2 describe-instances --filters "Name=image-id,Values=$id" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down" \
    --query 'length(Reservations[].Instances[])' --output text) || fail "describe-instances"
  [ "$n" = 0 ] || { echo "REFUSED $id: $n instance(s) still use it" >&2; exit 1; }
done

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
