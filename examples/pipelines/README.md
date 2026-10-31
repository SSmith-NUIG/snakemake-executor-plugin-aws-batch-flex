# Known pipelines on AWS Batch

Published Snakemake pipelines, each pinned to a release and run on its own test data on real
AWS Batch through the plugin. They are the pre-release check that the plugin works with
pipelines as people actually write them, not only with the small workflows in
`../conformance/`.

| Name | Pipeline | What it exercises |
|---|---|---|
| `tutorial` | The Snakemake tutorial's workflow (`tutorial/`), on [snakemake-tutorial-data](https://github.com/snakemake/snakemake-tutorial-data) | conda envs, `script:`, `temp()`/`protected()`, logs, config |
| `rnaseq` | [rna-seq-star-deseq2](https://github.com/snakemake-workflows/rna-seq-star-deseq2) | wrappers, R scripts, Ensembl downloads |
| `kallisto` | [rna-seq-kallisto-sleuth](https://github.com/snakemake-workflows/rna-seq-kallisto-sleuth) | many `script:` steps (23 R scripts) |
| `nanopore` | [transcriptome-differential-expression](https://github.com/snakemake-workflows/transcriptome-differential-expression) | **not runnable on Batch as written** (below) |
| `varlociraptor` | [dna-seq-varlociraptor](https://github.com/snakemake-workflows/dna-seq-varlociraptor) | a large DAG, modules, meta-wrappers |
| `zarp` | [zarp](https://github.com/zavolanlab/zarp) | **not runnable on Batch as written** (below) |
| `grenepipe` | [grenepipe](https://github.com/lczech/grenepipe) | **needs Snakemake 8** (below) |

```bash
bash run.sh --check tutorial   # set it up and dry-run it locally: no AWS, no cost
bash run.sh rnaseq kallisto    # on AWS, from this machine (the profile's EC2 queue)
MODE=head bash run.sh rnaseq   # on AWS, with Snakemake itself in a head job (smkflex run)
```

Each run clones the pipeline into `~/smkflex-pipelines/NAME` (set `WORK=` to change this), sets
up its test as its own CI does but from the top of the repository (the plugin can't run with
`--directory .test`), uploads the inputs with `smkflex push` to its own prefix
(`pipelines-NAME` in the bucket), and runs it. The log goes to `~/smkflex-pipelines/NAME.log`,
and `results.tsv` here gets one line per run.

Costs: each takes about 15-60 minutes of small instances, mostly building conda envs.
Afterwards, `aws s3 rm --recursive <bucket prefix>/pipelines-NAME/` removes its files.

## Not runnable as written

**nanopore** (transcriptome-differential-expression v2.6.0) finds each sample's reads by walking
the folder Snakemake runs in (`os.walk(Path.cwd())` in `workflow/rules/commons.smk`) and uses
the absolute paths it finds. Every Batch job parses the Snakefile again in its own folder, where
the reads aren't present (they come from the bucket), so the jobs can't reconstruct their
inputs. The pipeline needs every job to see the same files at the same paths (a single machine,
or a cluster's shared filesystem); any executor without one has the same problem. The fix
belongs in the pipeline: build the read paths from `config["inputdir"]`, relative to the
workflow folder.

**zarp** (v1.0.0-rc.1) names inputs by absolute paths and passes them to tools as params, which
works on Batch (jobs see files at the same paths as Snakemake). But its rules leave outputs
undeclared: `create_index_star` declares only `chrName.txt` and `chrNameLength.txt` of the STAR
index, while STAR writes a dozen more files beside them. Snakemake only uploads declared
outputs, so the rest of the index never reaches the bucket and `map_genome_star` can't open
`genomeParameters.txt`; the quantification merges fail the same way. A shared filesystem keeps
undeclared files, which is why it works on a cluster. The fix belongs in the pipeline: declare
such folders with `directory()`.

**grenepipe** (v0.15.0, and its master branch in October 2026) pins Snakemake 8.15.2 and reads
`logger.mode`, which Snakemake 9 removed, so it stops at its first line with any Snakemake 9
("'Logger' object has no attribute 'mode'"). The plugin needs Snakemake 9.
