with open(snakemake.output[0], "w") as f:
    f.write(f"{snakemake.params.word} threads={snakemake.threads} rule={snakemake.rule}\n")
