# A secret from SSM Parameter Store (aws-batch-flex-secrets): ECS reads it when the job starts,
# so the value never passes through Batch. run.sh makes the parameter and removes it again.
container: "docker://debian:12"

rule all:
    input: "results/secrets/out.txt"

rule secret:
    output: "results/secrets/out.txt"
    resources: mem_mb=512
    shell:
        """
        case "${{SMKFLEX_TEST_TOKEN:-}}" in
          hello-ssm) echo ok > {output} ;;
          "") echo "SMKFLEX_TEST_TOKEN is not set" >&2; exit 1 ;;
          *) echo "SMKFLEX_TEST_TOKEN has the wrong value" >&2; exit 1 ;;
        esac
        """
