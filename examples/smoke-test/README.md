After `terraform apply`:

    terraform -chdir=../../terraform output -raw snakemake_profile > profile/config.yaml
    printf 'default-resources: [mem_mb=512]\njobs: 5\n' >> profile/config.yaml
    snakemake --profile profile
    aws s3 cp s3://<bucket>/snakemake/results/sum.txt -     # 45
    aws s3 cp s3://<bucket>/snakemake/results/hello.txt -   # "image has NO python3"
