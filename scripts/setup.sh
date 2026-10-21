#!/usr/bin/env bash
# Set everything up in one go: from a working AWS login to a queue, a bucket and a Snakemake
# profile, with a test workflow ready to run. It does what "Setting up by hand" in the README describes:
#   1. checks the tools and your AWS login, and finds the default subnets
#   2. builds the machine image (scripts/bake-ami.sh, 10-20 min)
#   3. writes terraform.tfvars, shows `terraform plan` and asks before creating anything
#   4. writes the profile and runs `smkflex check`
# Running it again picks up where it stopped (it reuses the image and settings it wrote).
#
#   bash scripts/setup.sh --region eu-west-1
#   bash scripts/setup.sh --region eu-west-1 --fargate --spot
#
# Options: --region R, --name N (prefix of everything created, default smkflex), --fargate,
#   --spot, --conda (conda in the image, for --sdm conda), --extra-package REQ (a Python package
#   your Snakefile imports, e.g. pandas; repeatable), --ami ID (skip building one),
#   --subnets a,b (default: the default subnets), --tf-dir DIR (default terraform/; another
#   folder keeps a separate setup with its own state), --workflow-dir DIR (where the profile
#   and the test Snakefile go; default examples/smoke-test), --dry-run (only check and print).
set -euo pipefail

AWS=${AWS:-aws}
TERRAFORM=${TERRAFORM:-terraform}
HERE=$(cd "$(dirname "$0")/.." && pwd)
REGION="" NAME="" AMI="" SUBNETS="" FARGATE=false SPOT=false CONDA=0 DRY=0
EXTRA_PKGS=()
TF_DIR="$HERE/terraform" WF_DIR="$HERE/examples/smoke-test"

usage() { sed -n '2,19p' "$0"; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --region) REGION=$2; shift 2;;
    --name) NAME=$2; shift 2;;
    --ami) AMI=$2; shift 2;;
    --subnets) SUBNETS=${2//,/ }; shift 2;;
    --fargate) FARGATE=true; shift;;
    --spot) SPOT=true; shift;;
    --conda) CONDA=1; shift;;
    --extra-package) EXTRA_PKGS+=(--extra-package "$2"); shift 2;;
    --tf-dir) TF_DIR=$2; shift 2;;
    --workflow-dir) WF_DIR=$2; shift 2;;
    --dry-run) DRY=1; shift;;
    -h|--help) usage;;
    *) echo "unknown option $1" >&2; usage;;
  esac
done

step() { printf '\n== %s\n' "$*"; }
die() { printf 'setup: %s\n' "$*" >&2; exit 1; }
ask() { read -r -p "$1 Type yes to continue: " answer; [ "$answer" = yes ] || die "stopped; nothing more was done"; }
tfvar() { [ -f "$TFV" ] && sed -n "s/^$1 *= *\"\{0,1\}\([^\"#]*\)\"\{0,1\}.*/\1/p" "$TFV" | head -1 | sed 's/ *$//'; }

step "Checking the tools and your AWS login"
for t in "$AWS" "$TERRAFORM" snakemake smkflex snakemake-batch-flex-host-env; do
  command -v "$t" >/dev/null || die "'$t' not found. Run: conda activate smk-aws, then pip install -e . in the repository (README step 2)"
done
python -c "import snakemake_storage_plugin_s3" 2>/dev/null || die "snakemake-storage-plugin-s3 isn't installed: pip install snakemake-storage-plugin-s3"
REGION=${REGION:-$($AWS configure get region 2>/dev/null || true)}
[ -n "$REGION" ] || die "no region: pass --region (e.g. eu-west-1) or set one with aws configure"
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION AWS_PAGER=""
WHO=$($AWS sts get-caller-identity --query Arn --output text 2>/dev/null) \
  || die "not logged in to AWS. Run aws configure (README step 3), then check with: aws sts get-caller-identity"
ACCOUNT=$($AWS sts get-caller-identity --query Account --output text)
echo "AWS login: $WHO (account $ACCOUNT, region $REGION)"

TFV="$TF_DIR/terraform.tfvars"
if [ -f "$TFV" ]; then
  # a setup that was started before: keep its settings
  OLD_NAME=$(tfvar name); OLD_REGION=$(tfvar region)
  [ -z "$NAME" ] || [ "$NAME" = "$OLD_NAME" ] || die "$TFV is for name '$OLD_NAME'; use --tf-dir with another folder for a second setup"
  [ "$REGION" = "$OLD_REGION" ] || die "$TFV is for region $OLD_REGION, not $REGION"
  NAME=$OLD_NAME; AMI=${AMI:-$(tfvar ami_id)}
  echo "continuing the setup in $TF_DIR (name $NAME)"
fi
NAME=${NAME:-smkflex}
[[ "$NAME" =~ ^[a-z][a-z0-9-]{1,30}$ ]] || die "--name must be lowercase letters, digits and dashes (it's part of the bucket name)"

# The names must be free, unless this setup's own state already has them.
if ! { [ -f "$TF_DIR/terraform.tfstate" ] && $TERRAFORM -chdir="$TF_DIR" state list 2>/dev/null | grep -q '^aws_batch_job_queue.this$'; }; then
  q=$($AWS batch describe-job-queues --job-queues "$NAME-queue" --query 'jobQueues[0].jobQueueName' --output text)
  [ "$q" = None ] || [ -z "$q" ] || die "a queue named $NAME-queue already exists in $REGION; pick another --name"
  BUCKET="$NAME-$ACCOUNT-$REGION"
  if $AWS s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
    die "a bucket named $BUCKET already exists; pick another --name"
  fi
fi

if [ -z "$SUBNETS" ] && [ -f "$TFV" ]; then
  SUBNETS=$(sed -n 's/^subnet_ids *= *\[\(.*\)\].*/\1/p' "$TFV" | tr -d '",')
fi
if [ -z "$SUBNETS" ]; then
  SUBNETS=$($AWS ec2 describe-subnets --filters Name=default-for-az,Values=true --query 'Subnets[].SubnetId' --output text)
  [ -n "$SUBNETS" ] && [ "$SUBNETS" != None ] || die "this account has no default subnets in $REGION. Create the default network with: aws ec2 create-default-vpc   (then run this again), or pass --subnets"
fi
read -r -a SUBNET_LIST <<< "$SUBNETS"

step "Plan"
echo "  name:      $NAME (queue $NAME-queue, bucket $NAME-$ACCOUNT-$REGION)"
echo "  subnets:   ${SUBNET_LIST[*]}"
if [ -n "$AMI" ]; then echo "  image:     $AMI"; else echo "  image:     built now from the current Amazon ECS image (10-20 min, a few cents)"; fi
echo "  extras:    Fargate queue: $FARGATE, Spot queue: $SPOT, conda in the image: $([ $CONDA = 1 ] && echo yes || echo no)"
echo "  terraform: $TF_DIR"
echo "  profile:   $WF_DIR/profile/config.yaml"
if [ "$DRY" = 1 ]; then echo; echo "--dry-run: nothing was created"; exit 0; fi
[ -n "$AMI" ] || ask "This starts a temporary machine to build the image."

if [ -z "$AMI" ]; then
  step "Building the machine image"
  BASE=$($AWS ssm get-parameter --name /aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id \
    --query Parameter.Value --output text)
  LOG=$(mktemp)
  EXTRA=("${EXTRA_PKGS[@]}"); [ "$CONDA" = 1 ] && EXTRA+=(--conda)
  bash "$HERE/scripts/bake-ami.sh" --base-ami "$BASE" --subnet "${SUBNET_LIST[0]}" --region "$REGION" "${EXTRA[@]}" | tee "$LOG"
  AMI=$(sed -n 's/^DONE\. New standalone AMI: \(ami-[0-9a-f]*\).*/\1/p' "$LOG")
  rm -f "$LOG"
  [ -n "$AMI" ] || die "the image build didn't report an AMI; see the output above"
fi

step "Writing $TFV"
mkdir -p "$TF_DIR"
if [ "$(cd "$TF_DIR" && pwd)" != "$HERE/terraform" ]; then
  # a separate setup: its own copy of the Terraform files, and its own state
  cp "$HERE"/terraform/*.tf "$HERE"/terraform/userdata.snakemake.txt "$HERE"/terraform/.terraform.lock.hcl "$TF_DIR"/
fi
if [ -f "$TFV" ]; then
  # keep what's there, but record the image and the switches given now
  sed -i "s/^ami_id *=.*/ami_id = \"$AMI\"/" "$TFV"
  grep -q '^ami_id' "$TFV" || echo "ami_id = \"$AMI\"" >> "$TFV"
  for kv in "enable_fargate=$FARGATE" "enable_spot_queue=$SPOT"; do
    k=${kv%%=*}; v=${kv#*=}
    if [ "$v" = true ]; then
      if grep -q "^$k" "$TFV"; then sed -i "s/^$k *=.*/$k = true/" "$TFV"; else echo "$k = true" >> "$TFV"; fi
    fi
  done
else
  {
    echo "# written by scripts/setup.sh"
    echo "region = \"$REGION\""
    echo "name   = \"$NAME\""
    printf 'subnet_ids = ['; printf '"%s", ' "${SUBNET_LIST[@]}" | sed 's/, $//'; echo ']'
    echo "ami_id = \"$AMI\""
    echo "root_volume_gib = 100"
    echo "enable_fargate = $FARGATE"
    echo "enable_spot_queue = $SPOT"
  } > "$TFV"
fi
cat "$TFV"

step "Terraform plan"
$TERRAFORM -chdir="$TF_DIR" init -input=false >/dev/null
PLAN=$(mktemp)
$TERRAFORM -chdir="$TF_DIR" plan -input=false -no-color -out=tfplan > "$PLAN"
grep -E '^  # .* will be|^Plan:|^No changes' "$PLAN" || true
if grep -q '^No changes' "$PLAN"; then
  echo "everything already exists"
else
  summary=$(grep '^Plan:' "$PLAN")
  case "$summary" in
    *" 0 to change, 0 to destroy"*) ;;
    *) die "the plan would change or remove existing resources ($summary). Check it with: terraform -chdir=$TF_DIR plan";;
  esac
  ask "Terraform will create the resources above (they cost nothing while idle)."
  $TERRAFORM -chdir="$TF_DIR" apply -input=false tfplan
fi

step "Writing the profile"
mkdir -p "$WF_DIR/profile"
P="$WF_DIR/profile/config.yaml"
[ -f "$P" ] && cp "$P" "$P.bak" && echo "(the old profile is in $P.bak)"
{
  $TERRAFORM -chdir="$TF_DIR" output -raw snakemake_profile
  echo "default-resources: [mem_mb=512]"
  echo "jobs: 5"
  if [ "$CONDA" = 1 ]; then echo "aws-batch-flex-conda-path: /opt/miniforge3"; fi
} > "$P"
[ -f "$WF_DIR/Snakefile" ] || cp "$HERE/examples/smoke-test/Snakefile" "$WF_DIR/Snakefile"
cat "$P"

step "Checking the setup"
(cd "$WF_DIR" && smkflex check) || die "the check found problems (above)"

step "Done"
echo "Run the test workflow:"
echo "  cd $WF_DIR && snakemake --profile profile"
echo "or let AWS run it, so your machine can be switched off:"
echo "  cd $WF_DIR && smkflex run --wait"
