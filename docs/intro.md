Runs Snakemake workflows on [AWS Batch](https://aws.amazon.com/batch/) with off-the-shelf
container images: each rule runs in the image its `container:` directive names, exactly as
published (quay.io/biocontainers, Docker Hub, ECR, ...). You don't build images with Snakemake
in them, and the images don't need Python either: on EC2, a Snakemake environment baked into
the AMI is mounted into every job; on Fargate, Snakemake is installed when the job starts.

It needs Snakemake 9.5 or newer, and the S3 storage plugin (see below).
