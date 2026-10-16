container: "docker://debian:12"
rule all:
    input: "results/named_env.txt"
rule named_env:
    output: "results/named_env.txt"
    conda: "smalltest"
    resources: mem_mb=512
    shell: "echo $CONDA_PREFIX > {output}; python -c \"import yaml; print(yaml.__version__)\" >> {output}"
