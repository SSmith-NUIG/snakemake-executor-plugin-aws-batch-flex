#!/usr/bin/env bash
# Interrupted run: start orphans.smk, kill -9 Snakemake once its jobs are submitted, restart.
# The restart should cancel the 3 old jobs and then finish with 3 new ones.
set -u
rm -rf .snakemake
snakemake --profile profile -s orphans.smk --forceall -j 3 > conformance-orphans-1.log 2>&1 &
pid=$!
for _ in $(seq 60); do
  [ "$(grep -c '^Submitted' conformance-orphans-1.log)" -ge 3 ] && break
  sleep 5
done
old=$(grep -o 'AWS Batch job [0-9a-f-]*' conformance-orphans-1.log | awk '{print $4}')
echo "killing snakemake; old jobs: $(echo $old | wc -w)"
kill -9 $pid; sleep 2
snakemake --profile profile -s orphans.smk --unlock > conformance-orphans-unlock.log 2>&1
snakemake --profile profile -s orphans.smk -j 3 > conformance-orphans-2.log 2>&1
echo "restart exit code: $?"
grep -hE "Cancelling|earlier run" conformance-orphans-unlock.log conformance-orphans-2.log
echo "old jobs now:"
aws batch describe-jobs --jobs $old --query 'jobs[].[jobId,status,statusReason]' --output text
