Runs Snakemake jobs on [AWS Batch](https://aws.amazon.com/batch/), each in the image named by
its rule's `container:` directive (Docker Hub, quay.io/biocontainers, ECR, ...). The images
don't need Snakemake or Python: on EC2, a Snakemake environment baked into the AMI is mounted
into every job; on Fargate, Snakemake is installed when the job starts.

It needs Snakemake 9.5 or newer, and the S3 storage plugin (see below).
