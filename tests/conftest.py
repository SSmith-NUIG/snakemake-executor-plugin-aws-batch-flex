"""Settings for every test, inherited by the Snakemake processes the tests start."""
import os

# The fake Batch's jobs end at once: check on them every second instead of every 10 s
os.environ.setdefault("SMK_FLEX_TEST_STATUS_SECONDS", "1")
