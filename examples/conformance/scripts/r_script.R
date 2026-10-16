# Uses only base R and the `snakemake` S4 object that Snakemake injects.
line <- sprintf("%s threads=%d rule=%s R=%s", snakemake@params[["word"]], snakemake@threads,
                snakemake@rule, R.version$major)
writeLines(line, snakemake@output[[1]])
