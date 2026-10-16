# conda: YAML env in an image that has conda. Needs --sdm conda and internet in the job.
container: "docker://condaforge/miniforge3:latest"

rule all:
    input: "results/conda.txt"

rule conda_rule:
    output: "results/conda.txt"
    conda: "envs/tiny.yaml"
    resources: mem_mb=1024
    shell:
        "python --version > {output} 2>&1; "
        "python -c 'import yaml; print(\"pyyaml\", yaml.__version__)' >> {output}"
