#!/usr/bin/env bash
# Deregister job definitions the plugin registered and no active job uses. Safe for the plugin:
# a run that needs one again simply registers it again. Rules:
#   * only names <prefix>-<12 hex> (the plugin's own pattern) that are tagged
#     ManagedBy=snakemake-batch-flex; --include-untagged also takes untagged ones with that name
#     (registered before the plugin tagged its definitions)
#   * never one used by a job that is SUBMITTED..RUNNING in any queue; if that can't be checked,
#     nothing is deregistered
#   * lists what will go and asks for 'yes' (skip with --yes); --dry-run only lists
#   bash scripts/cleanup-jobdefs.sh --region eu-west-1 [--prefix snakemake] [--include-untagged] [--dry-run] [--yes]
set -euo pipefail
AWS=${AWS:-aws}
REGION=""; PREFIX=snakemake; UNTAGGED=0; DRY=0; YES=0
while [ $# -gt 0 ]; do
  case "$1" in
    --region) REGION=$2; shift 2;;
    --prefix) PREFIX=$2; shift 2;;
    --include-untagged) UNTAGGED=1; shift;;
    --dry-run) DRY=1; shift;;
    --yes) YES=1; shift;;
    *) echo "usage: cleanup-jobdefs.sh --region R [--prefix P] [--include-untagged] [--dry-run] [--yes]" >&2; exit 2;;
  esac
done
[ -n "$REGION" ] || { echo "need --region" >&2; exit 2; }
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION AWS_PAGER=""
fail() { echo "REFUSED: could not check which job definitions are in use ($1); nothing deregistered" >&2; exit 1; }

defs=$($AWS batch describe-job-definitions --status ACTIVE --output text \
  --query 'jobDefinitions[].[jobDefinitionArn, jobDefinitionName, tags.ManagedBy]') || fail "describe-job-definitions"

# job definitions of every job that is still active, in any queue
USED=" "
queues=$($AWS batch describe-job-queues --query 'jobQueues[].jobQueueName' --output text) || fail "describe-job-queues"
for q in $queues; do
  for st in SUBMITTED PENDING RUNNABLE STARTING RUNNING; do
    ids=$($AWS batch list-jobs --job-queue "$q" --job-status "$st" --query 'jobSummaryList[].jobId' --output text) \
      || fail "list-jobs $q"
    set -- $ids
    while [ $# -gt 0 ]; do
      batch=("${@:1:100}"); shift $(( $# < 100 ? $# : 100 ))
      out=$($AWS batch describe-jobs --jobs "${batch[@]}" --query 'jobs[].jobDefinition' --output text) \
        || fail "describe-jobs"
      USED+="$(echo $out) "
    done
  done
done

PLAN=()
while read -r arn name tag; do
  [ -n "$arn" ] || continue
  [[ "$name" =~ ^${PREFIX}-[0-9a-f]{12}$ ]] || continue
  if [ "$tag" != snakemake-batch-flex ] && [ "$UNTAGGED" != 1 ]; then continue; fi
  case "$USED" in *" $arn "*) echo "keep (in use): $arn"; continue;; esac
  PLAN+=("$arn")
done <<< "$defs"

[ ${#PLAN[@]} -gt 0 ] || { echo "nothing to deregister"; exit 0; }
printf 'will deregister: %s\n' "${PLAN[@]}"
[ "$DRY" = 1 ] && { echo "--dry-run: nothing deregistered"; exit 0; }
if [ "$YES" != 1 ]; then read -r -p "Type yes to deregister the above: " ans; [ "$ans" = yes ] || { echo "aborted"; exit 1; }; fi
for arn in "${PLAN[@]}"; do
  $AWS batch deregister-job-definition --job-definition "$arn" && echo "deregistered $arn"
done
