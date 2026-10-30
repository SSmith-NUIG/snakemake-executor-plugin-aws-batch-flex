# Known pipelines on AWS Batch

Published Snakemake pipelines, each at a fixed release and with its own test data, run on real
AWS Batch through the plugin. They're the check before a release that the plugin works with
pipelines as people write them, not only with the small workflows in `../conformance/`.

| Name | Pipeline | What it exercises |
|---|---|---|
| `tutorial` | The Snakemake tutorial's workflow (`tutorial/`), on [snakemake-tutorial-data](https://github.com/snakemake/snakemake-tutorial-data) | conda envs, `script:`, `temp()`/`protected()`, logs, config |
| `rnaseq` | [rna-seq-star-deseq2](https://github.com/snakemake-workflows/rna-seq-star-deseq2) | wrappers, R scripts, Ensembl downloads |
| `kallisto` | [rna-seq-kallisto-sleuth](https://github.com/snakemake-workflows/rna-seq-kallisto-sleuth) | many `script:` steps (23 R scripts) |
| `nanopore` | [transcriptome-differential-expression](https://github.com/snakemake-workflows/transcriptome-differential-expression) | **not runnable on Batch as written** (below) |
| `varlociraptor` | [dna-seq-varlociraptor](https://github.com/snakemake-workflows/dna-seq-varlociraptor) | a large DAG, modules, meta-wrappers |
| `zarp` | [zarp](https://github.com/zavolanlab/zarp) | a `container:` per rule (biocontainers and others), a checkpoint |
| `grenepipe` | [grenepipe](https://github.com/lczech/grenepipe) | inputs named by absolute path, checkpoints |

```
bash run.sh --check tutorial   # set it up and dry-run it here: no AWS, nothing to pay
bash run.sh rnaseq kallisto    # on AWS, from this machine (EC2 queue of the profile)
MODE=head bash run.sh rnaseq   # on AWS, with Snakemake itself in a head job (smkflex run)
```

Each run clones the pipeline into `~/smkflex-pipelines/NAME` (`WORK=` to change), sets its test up
as its own CI does but from the top of the repository (the plugin can't run with
`--directory .test`), uploads the inputs with `smkflex push` to its own project folder
(`pipelines-NAME` in the bucket), and runs it. The log is `~/smkflex-pipelines/NAME.log`;
`results.tsv` here gets a line per run.

Costs: each takes about 15-60 minutes of small instances, most of it building conda envs.
Afterwards, `aws s3 rm --recursive <bucket prefix>/pipelines-NAME/` removes its files.

## Not runnable as written

**nanopore** (transcriptome-differential-expression v2.6.0) finds each sample's reads by searching
the folder Snakemake runs in (`os.walk(Path.cwd())` in `workflow/rules/commons.smk`) and uses the
absolute path it finds. Every Batch job reads the Snakefile again in its own folder, where the
reads aren't (they come from the bucket) and the paths differ, so the jobs can't rebuild their
inputs. It needs every machine to see the same files at the same paths (a laptop, or a cluster's
shared filesystem); any executor without one has the same problem. The fix belongs in the
pipeline: build the read paths from `config["inputdir"]`, relative to the workflow folder.
