# Images with an ENTRYPOINT, which Batch can't change: one rule per kind. A job that its image's
# entrypoint didn't run as a shell command fails.
IMAGES = {
    "python": "vergaju/evoflow:latest",               # ENTRYPOINT ["python3"]
    "script": "continuumio/miniconda3:26.7.1-1",      # ["/usr/local/bin/docker-entrypoint.sh"]
    "micromamba": "mambaorg/micromamba:2.3.0",        # ["/usr/local/bin/_entrypoint.sh"]
    "tini": "condaforge/miniforge3:25.3.1-0",         # ["tini", "--"]
    "none": "python:3.12-slim",                       # no ENTRYPOINT
}

rule all:
    input: expand("results/ep/{kind}.txt", kind=[*IMAGES, "override"])

for kind, image in IMAGES.items():
    rule:
        name: f"ep_{kind}"
        output: f"results/ep/{kind}.txt"
        container: f"docker://{image}"
        params: kind=kind
        resources: mem_mb=1024
        shell: "echo ok {params.kind} > {output}"

# The entrypoint given in the rule, as when the registry can't be read: no lookup
rule ep_override:
    output: "results/ep/override.txt"
    container: "docker://vergaju/evoflow:latest"
    resources: mem_mb=1024, batch_entrypoint='["python3"]'
    shell: "echo ok override > {output}"
