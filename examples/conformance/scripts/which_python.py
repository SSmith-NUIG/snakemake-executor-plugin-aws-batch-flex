import sys

try:
    import pandas  # noqa: F401
    has_pandas = "yes"
except ImportError:
    has_pandas = "no"
with open(snakemake.output[0], "w") as f:
    f.write(f"python={sys.executable} pandas={has_pandas}\n")
