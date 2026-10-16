import sys
import pandas as pd

df = pd.DataFrame({"x": range(snakemake.params.n)})
with open(snakemake.output[0], "w") as f:
    f.write(f"sum={df.x.sum()} pandas={pd.__version__} python={sys.executable}\n")
