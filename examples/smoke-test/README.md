# Smoke test

Two small jobs in `debian:12`, an image without Python: the quickest check that the setup works.
After `scripts/setup.sh` (step 4 of the README's Getting started):

```bash
cd examples/smoke-test
snakemake --profile smkflex
smkflex pull results --profile smkflex
cat results/hello.txt results/sum.txt     # "image has NO python3", then 45
```

`smkflex run --profile smkflex --wait` runs the same as a head job on AWS. For a test of every
feature, see `../conformance/run.sh`.
