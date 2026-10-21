#!/usr/bin/env bash
# Real-AWS conformance run. From this directory, with profile/config.yaml in place (see
# ../smoke-test/README.md). Usage:  bash run.sh [script|fanout|conda|pipeline|pyimage|pyrule|shm|existing|ecr|rscript|wrapper|mixed|disk|spot|oom|rules ...]   (default: script fanout)
# `mixed` = EC2 + Fargate queues in one run, `disk` = Fargate disk size from disk_mb
# (both need enable_fargate=true in terraform), `spot` = fanout on the Spot queue (enable_spot_queue=true)
# `rules` = checkpoint group shadow benchmark retries pipe tempdir directory (one small workflow each)
set -u
N=${N:-100}
PREFIX=$(grep '^default-storage-prefix:' profile/config.yaml | awk '{print $2}')
pass=(); fail=()
check() { # name, S3 key, expected-substring
  got=$(aws s3 cp "$PREFIX/$2" - 2>/dev/null)
  if echo "$got" | grep -q "$3"; then echo "  content OK: $2"; return 0; else echo "  content WRONG in $2: '$got' (wanted '$3')"; return 1; fi
}
run() { # name, snakemake args...
  name=$1; shift; echo "=== $name"; rm -rf .snakemake
  if snakemake --profile profile --forceall "$@" > "conformance-$name.log" 2>&1; then
    case $name in
      script) check script results/py_script.txt "pyscript threads=2 rule=py_script" && check script results/sh_script.txt "shscript threads=1";;
      fanout) check fanout results/total.txt "$(( N * (N - 1) / 2 ))";;
      conda)  check conda results/conda.txt "pyyaml";;
      pipeline) got=$(aws s3 cp "$PREFIX/results/summary.tsv" - 2>/dev/null); echo "$got" | sed 's/^/  /'
                [ "$(echo "$got" | awk 'NR>1 && $2>=5' | wc -l)" = 3 ] && echo "  3 samples, each with >=5 variants";;
      checkpoint) check checkpoint results/cp/total.txt "^6$";;
      group)      check group results/grp/final.txt "second" && [ "$(grep -c Submitted conformance-group.log)" = 1 ] && echo "  one Batch job for the group";;
      shadow)     check shadow results/shadow/out.txt "kept";;
      benchmark)  check benchmark results/bm/log.txt "logline" && check benchmark results/bm/bench.tsv "^s";;
      retries)    check retries results/rt/out.txt "ok attempt=2";;
      pipe)       check pipe results/pipe/out.txt "^3$" && [ "$(grep -c Submitted conformance-pipe.log)" = 1 ] && echo "  one Batch job for the pipe group";;
      tempdir)    check tempdir results/td/final.txt "x" && ! aws s3 ls "$PREFIX/results/td/tmp.txt" 2>/dev/null | grep -q tmp && echo "  temp file removed from storage";;
      directory)  check directory results/dr/total.txt "^3$";;
      existing)   check existing results/py_script.txt "pyscript threads=2 rule=py_script" \
                    && ! grep -q "Registering AWS Batch job definition" conformance-existing.log \
                    && echo "  used the existing job definition, registered none";;
      ecr)        check ecr results/ecr/os.txt "Debian GNU/Linux 12";;
      shm)        check shm results/shm/big.txt "^1024$" && check shm results/shm/default.txt "^64$";;
      pyimage)    check pyimage results/pyimg/out.txt "sum=6 pandas=2.2.1";;
      pyrule)     check pyrule results/pyrule/image.txt "python=/usr/local/bin/python pandas=yes" \
                    && check pyrule results/pyrule/host.txt "python=/opt/snakemake-env/.* pandas=no";;
      rscript)    check rscript results/r/out.txt "rscript threads=2 rule=r_script R=4";;
      wrapper)    check wrapper results/wr/ref.fa.fai "^chr2	8	";;
      mixed)      check mixed results/mixed/both.txt "AWS_ECS_EC2" && check mixed results/mixed/both.txt "AWS_ECS_FARGATE";;
      disk)       check disk results/disk/check.txt "^disk ok";;
      oom)        check oom results/oom/out.txt "^ok" && grep -q "ran out of memory with .* retrying with" conformance-oom.log \
                    && echo "  retried with more memory";;
      spot)       check spot results/total.txt "$(( N * (N - 1) / 2 ))" && grep -q "queue $SQ," conformance-spot.log \
                    && echo "  jobs went to $SQ";;
    esac && pass+=($name) || fail+=($name)
  else echo "  snakemake FAILED (see conformance-$name.log)"; fail+=($name); fi
}
for t in "${@:-script fanout}"; do
  for name in $t; do
    case $name in
      script) run script -s script.smk;;
      fanout) run fanout -s fanout.smk --config n_units=$N -j $N;;
      conda)  run conda -s conda.smk --sdm conda;;
      pipeline) run pipeline -s pipeline.smk -j 8;;
      shm) run shm -s shm.smk -j 2;;
      ecr) # a private ECR image: ECR_IMAGE=<uri> to use an existing one, otherwise a test repo is
           # created with a copy of debian:12-slim and deleted afterwards (needs ecr:CreateRepository + docker)
           if [ -n "${ECR_IMAGE:-}" ]; then
             run ecr -s ecr.smk --config ecr_image=docker://$ECR_IMAGE
           else
             REGION=$(grep '^aws-batch-flex-region:' profile/config.yaml | awk '{print $2}')
             ACCT=$(aws sts get-caller-identity --query Account --output text)
             REG=$ACCT.dkr.ecr.$REGION.amazonaws.com; IMG=$REG/smkflex-ecr-test:debian12
             if aws ecr create-repository --repository-name smkflex-ecr-test --region $REGION \
                  --tags Key=ManagedBy,Value=snakemake-batch-flex --query repository.repositoryUri --output text \
                && aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REG \
                && docker pull -q debian:12-slim && docker tag debian:12-slim $IMG && docker push -q $IMG; then
               run ecr -s ecr.smk --config ecr_image=docker://$IMG
             else
               echo "  could not set up the test image; nothing submitted"; fail+=(ecr)
             fi
             aws ecr delete-repository --repository-name smkflex-ecr-test --region $REGION --force \
               --query repository.repositoryName --output text 2>/dev/null && echo "  deleted ECR repo"
           fi;;
      existing) # a job definition made outside the plugin (with the host-env mount), removed again after
             ROLE=$(grep '^aws-batch-flex-job-role-arn:' profile/config.yaml | awk '{print $2}')
             ARN=$(aws batch register-job-definition --job-definition-name smkflex-existing-test --type container \
               --platform-capabilities EC2 --tags ManagedBy=snakemake-batch-flex --query jobDefinitionArn --output text \
               --container-properties "{\"image\":\"debian:12\",\"jobRoleArn\":\"$ROLE\",\"resourceRequirements\":[{\"type\":\"VCPU\",\"value\":\"1\"},{\"type\":\"MEMORY\",\"value\":\"512\"}],\"volumes\":[{\"name\":\"smkenv\",\"host\":{\"sourcePath\":\"/opt/snakemake-env\"}}],\"mountPoints\":[{\"sourceVolume\":\"smkenv\",\"containerPath\":\"/opt/snakemake-env\",\"readOnly\":true}]}")
             echo "  registered $ARN"
             run existing -s script.smk -j 2 --aws-batch-flex-job-definition "$ARN"
             aws batch deregister-job-definition --job-definition "$ARN" && echo "  deregistered $ARN";;
      pyimage) run pyimage -s pyimage.smk;;
      oom) run oom -s oom.smk --retries 1;;
      pyrule) run pyrule -s pyrule.smk -j 2;;
      rscript) run rscript -s rscript.smk;;
      wrapper) run wrapper -s wrapper.smk --sdm conda -j 2;;
      mixed|disk) FQ=$(terraform -chdir=../../terraform output -raw snakemake_profile_fargate | sed -n 's/^aws-batch-flex-queue: //p')
             XR=$(terraform -chdir=../../terraform output -raw snakemake_profile_fargate | sed -n 's/^aws-batch-flex-execution-role-arn: //p')
             run $name -s $name.smk --config fargate_queue=$FQ --aws-batch-flex-execution-role-arn "$XR" \
               --aws-batch-flex-install-prereqs -j 2;;
      spot) SQ=$(terraform -chdir=../../terraform output -raw spot_queue_name)
            run spot -s fanout.smk --config n_units=$N -j $N --aws-batch-flex-queue "$SQ";;
      rules) bash "$0" checkpoint group shadow benchmark retries pipe tempdir directory; exit;;
      checkpoint|group|shadow|benchmark|retries|pipe|tempdir|directory) run $name -s $name.smk -j 4;;
    esac
  done
done
echo; echo "PASS: ${pass[*]:-none}"; echo "FAIL: ${fail[*]:-none}"
