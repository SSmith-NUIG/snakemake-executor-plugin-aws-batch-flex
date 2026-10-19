#!/usr/bin/env bash
# Build a Snakemake AMI: launch a builder from a base AMI, install the Snakemake host env,
# and image it. The base AMI is only used as an image to launch from: it is never
# modified, shared, or deregistered. Everything this script creates is tagged
# ManagedBy=snakemake-batch-flex and the temporary pieces are removed again.
#
#   bash bake-ami.sh --base-ami ami-0123 --subnet subnet-abc --region eu-west-1
#
# Needs: the plugin installed (snakemake-batch-flex-host-env on PATH) and AWS credentials
# that may run ec2 run-instances/create-image (and manage the temporary SG).
set -euo pipefail

AWS=${AWS:-aws}
SLEEP=${SMK_BAKE_SLEEP:-sleep}
INSTANCE_TYPE=t3.medium
ENV_PATH=/opt/snakemake-env
STAMP=$(date +%Y%m%d-%H%M%S)
NAME="snakemake-host-env-$STAMP"
PLUGINS=()
TIMEOUT=${SMK_BAKE_TIMEOUT:-1200}            # builder: boot + install + stop
IMAGE_TIMEOUT=${SMK_BAKE_IMAGE_TIMEOUT:-3600}  # snapshot + register the AMI
DRY_RUN=0
BASE="" SUBNET="" REGION=""
CONDA=0; CONDA_PATH=/opt/miniforge3; CONDA_ENVS=()

usage() { sed -n '2,12p' "$0"; echo "options: --base-ami --subnet --region [--name N] [--instance-type T] [--storage-plugin s3 ...] [--env-path P] [--conda] [--conda-path P] [--conda-env env.yaml ...] [--dry-run]"; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --base-ami) BASE=$2; shift 2;;
    --subnet) SUBNET=$2; shift 2;;
    --region) REGION=$2; shift 2;;
    --name) NAME=$2; shift 2;;
    --instance-type) INSTANCE_TYPE=$2; shift 2;;
    --storage-plugin) PLUGINS+=("$2"); shift 2;;
    --env-path) ENV_PATH=$2; shift 2;;
    --conda) CONDA=1; shift;;
    --conda-path) CONDA=1; CONDA_PATH=$2; shift 2;;
    --conda-env) CONDA=1; CONDA_ENVS+=("$2"); shift 2;;
    --dry-run) DRY_RUN=1; shift;;
    *) usage;;
  esac
done
[ -n "$BASE" ] && [ -n "$SUBNET" ] && [ -n "$REGION" ] || usage
[ ${#PLUGINS[@]} -gt 0 ] || PLUGINS=(s3)
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION AWS_PAGER=""
command -v snakemake-batch-flex-host-env >/dev/null || { echo "snakemake-batch-flex-host-env not found: activate the env where the plugin is installed" >&2; exit 1; }

PLUGIN_ARGS=()
for p in "${PLUGINS[@]}"; do PLUGIN_ARGS+=(--storage-plugin "$p"); done
INSTALLER=$(snakemake-batch-flex-host-env script --path "$ENV_PATH" "${PLUGIN_ARGS[@]}")
# the Snakemake version going into the AMI; tagged on the image so the plugin can compare it
SMK_VERSION=$(printf '%s\n' "$INSTALLER" | grep -o 'snakemake==[0-9][0-9.]*' | head -1 | cut -d= -f3)
SMK_VERSION=${SMK_VERSION:-unknown}

CONDA_SCRIPT=""
if [ "$CONDA" = 1 ]; then
  CARGS=(--conda-path "$CONDA_PATH"); for f in "${CONDA_ENVS[@]}"; do CARGS+=(--conda-env "$f"); done
  CONDA_SCRIPT=$(snakemake-batch-flex-host-env conda "${CARGS[@]}")
fi

# user-data for the temporary builder: install, then on success clean ECS state and stop itself;
# on failure stay running. The host script reads that state (it never depends on console output,
# which some AMIs do not populate).
UD=$(mktemp)
{
  echo '#!/bin/bash'
  echo "cat > /tmp/install.sh <<'SMK_INSTALL_EOF'"
  echo "$INSTALLER"
  echo "SMK_INSTALL_EOF"
  if [ -n "$CONDA_SCRIPT" ]; then
    echo "cat > /tmp/conda.sh <<'SMK_CONDA_EOF'"; echo "$CONDA_SCRIPT"; echo "SMK_CONDA_EOF"
    echo 'if bash /tmp/install.sh > /var/log/snakemake-bake.log 2>&1 && bash /tmp/conda.sh >> /var/log/snakemake-bake.log 2>&1; then'
  else
    echo 'if bash /tmp/install.sh > /var/log/snakemake-bake.log 2>&1; then'
  fi
  echo '  rm -f /tmp/install.sh /tmp/conda.sh /tmp/env-*.yaml /var/log/snakemake-bake.log'
  echo '  systemctl stop ecs 2>/dev/null || true'
  echo '  rm -rf /var/lib/ecs/data/* /var/log/ecs/* 2>/dev/null || true'
  echo '  sync; shutdown -h now'
  echo 'else'
  echo '  # stay running: the host script treats "builder never stopped" as a failed install'
  echo '  { echo SMK_BAKE_FAILED; tail -40 /var/log/snakemake-bake.log; } > /dev/console 2>&1'
  echo '  sleep 3600'
  echo 'fi'
} > "$UD"

echo "base AMI (read-only use): $BASE   region: $REGION   subnet: $SUBNET"
[ "$(wc -c < "$UD")" -le 16000 ] || { echo "builder user-data is over EC2's 16 KB limit: use fewer or smaller --conda-env files" >&2; exit 1; }
echo "new AMI name: $NAME   env path: $ENV_PATH   storage plugins: ${PLUGINS[*]}"
if [ "$DRY_RUN" = 1 ]; then echo "--dry-run: nothing created. Builder user-data:"; cat "$UD"; rm -f "$UD"; exit 0; fi

TAGS='{Key=ManagedBy,Value=snakemake-batch-flex}'
INSTANCE="" SG=""
cleanup() {
  set +e
  if [ -n "$INSTANCE" ]; then
    echo "cleanup: terminating builder $INSTANCE"
    $AWS ec2 terminate-instances --instance-ids "$INSTANCE" >/dev/null
    $AWS ec2 wait instance-terminated --instance-ids "$INSTANCE"
  fi
  if [ -n "$SG" ]; then
    for i in 1 2 3 4 5 6; do
      $AWS ec2 delete-security-group --group-id "$SG" >/dev/null 2>&1 && { echo "cleanup: deleted temporary SG $SG"; break; }
      $SLEEP 10
    done
  fi
  rm -f "$UD"
}
trap cleanup EXIT

# Poll a state instead of using `aws ec2 wait`: the CLI's instance-stopped waiter treats
# 'pending' (the state right after launch) as a failure, and its default time limit is too
# short for a large root volume.   poll LABEL WANT_REGEX FAIL_REGEX TIMEOUT_SECONDS COMMAND...
poll() {
  local label=$1 want=$2 bad=$3 limit=$4 waited=0 state last=""
  shift 4
  while :; do
    state=$("$@" 2>/dev/null || echo unknown)
    [ "$state" != "$last" ] && echo "  $label: $state" && last=$state
    [[ "$state" =~ ^($want)$ ]] && return 0
    if [[ "$state" =~ ^($bad)$ ]]; then echo "$label ended in state '$state'" >&2; return 1; fi
    if [ "$waited" -ge "$limit" ]; then echo "timed out after ${limit}s waiting for $label (state: $state)" >&2; return 1; fi
    $SLEEP 15; waited=$((waited + 15))
  done
}

VPC=$($AWS ec2 describe-subnets --subnet-ids "$SUBNET" --query 'Subnets[0].VpcId' --output text)
SG=$($AWS ec2 create-security-group --group-name "snakemake-ami-builder-$STAMP" \
  --description "temporary egress-only group for the Snakemake AMI build" --vpc-id "$VPC" \
  --tag-specifications "ResourceType=security-group,Tags=[$TAGS]" --query GroupId --output text)
echo "temporary security group: $SG (egress only)"

INSTANCE=$($AWS ec2 run-instances --image-id "$BASE" --instance-type "$INSTANCE_TYPE" --count 1 \
  --network-interfaces "DeviceIndex=0,SubnetId=$SUBNET,Groups=$SG,AssociatePublicIpAddress=true" \
  --metadata-options "HttpEndpoint=enabled,HttpTokens=required,HttpPutResponseHopLimit=2" \
  --instance-initiated-shutdown-behavior stop \
  --user-data "file://$UD" \
  --tag-specifications "ResourceType=instance,Tags=[$TAGS,{Key=Name,Value=snakemake-ami-builder}]" \
  --query 'Instances[0].InstanceId' --output text)
echo "builder instance: $INSTANCE (it stops itself when the install is done)"

diagnose() {   # best effort only: some AMIs never populate the console
  local out
  out=$($AWS ec2 get-console-output --instance-id "$INSTANCE" --latest --output text 2>&1 || true)
  if [ -n "$(echo "$out" | tr -d '[:space:]')" ]; then
    printf '%s\n' "$out" > "bake-console-$STAMP.log"
    echo "console output saved to bake-console-$STAMP.log; last lines:" >&2; echo "$out" | tail -30 >&2
  else
    echo "(no console output available from this AMI, so no further detail)" >&2
  fi
}

echo "waiting for the builder to boot, install and stop itself (up to $((TIMEOUT / 60)) min)..."
if ! poll "builder" stopped 'terminated|shutting-down' "$TIMEOUT" \
     $AWS ec2 describe-instances --instance-ids "$INSTANCE" --query 'Reservations[0].Instances[0].State.Name' --output text; then
  echo "The builder did not stop itself, which means the install did not finish successfully" >&2
  echo "(it only shuts down after the installer succeeds)." >&2
  diagnose
  exit 1
fi
echo "builder stopped itself: the installer finished successfully"

IMAGE=$($AWS ec2 create-image --instance-id "$INSTANCE" --name "$NAME" \
  --description "Snakemake host env on $ENV_PATH, copied from $BASE" \
  --tag-specifications "ResourceType=image,Tags=[$TAGS,{Key=BaseAmi,Value=$BASE},{Key=SnakemakeVersion,Value=$SMK_VERSION}]" \
                       "ResourceType=snapshot,Tags=[$TAGS]" \
  --query ImageId --output text)
echo "creating AMI $IMAGE ..."
poll "AMI $IMAGE" available 'failed|deregistered|error|invalid' "$IMAGE_TIMEOUT" \
  $AWS ec2 describe-images --image-ids "$IMAGE" --query 'Images[0].State' --output text

echo
echo "DONE. New standalone AMI: $IMAGE ($NAME)"
echo "Put this in terraform.tfvars:   ami_id = \"$IMAGE\""
