# Small variant-calling pipeline, each tool in its own stock image (none has Snakemake).
# Swap an image with e.g. --config img_bcftools=docker://some/image:tag
SAMPLES = ["s1", "s2", "s3"]
IMG = {
    "ref": config.get("img_ref", "docker://debian:12"),
    "sim": config.get("img_sim", "docker://ubuntu:22.04"),
    "fastp": config.get("img_fastp", "docker://staphb/fastp:0.23.4"),
    "bwa": config.get("img_bwa", "docker://staphb/bwa:0.7.17"),
    "samtools": config.get("img_samtools", "docker://staphb/samtools:1.20"),
    "bcftools": config.get("img_bcftools", "docker://staphb/bcftools:1.20"),
}
TOOLCMD = {
    "ref": "head -n1 /etc/os-release", "sim": "head -n1 /etc/os-release",
    "fastp": "fastp --version", "bwa": "bwa 2>&1 | grep -i version",
    "samtools": "samtools --version | head -n1", "bcftools": "bcftools --version | head -n1",
}
IDX = [".amb", ".ann", ".bwt", ".pac", ".sa"]
AWK_REF = 'BEGIN{srand(42); print ">chr1"; split("A C G T",b," "); for(i=0;i<250;i++){l=""; for(j=0;j<80;j++) l=l b[int(rand()*4)+1]; print l}}'
AWK_SIM = r'''function rc(s,  i,r,c){r="";for(i=length(s);i>=1;i--){c=substr(s,i,1);r=r (c=="A"?"T":(c=="C"?"G":(c=="G"?"C":(c=="T"?"A":"N"))))}return r}
/^>/{next}
{g=g $0}
END{srand(seed);L=length(g);for(k=0;k<10;k++){p=int(rand()*(L-400))+200;b=substr(g,p,1);nb=(b=="A")?"C":"A";g=substr(g,1,p-1) nb substr(g,p+1)}
q="";for(i=0;i<len;i++)q=q "I"
for(i=0;i<n;i++){f=int(rand()*(L-350))+1;s1=substr(g,f,len);s2=rc(substr(g,f+200,len));print "@r" i "/1\n" s1 "\n+\n" q > o1;print "@r" i "/2\n" s2 "\n+\n" q > o2}}'''

rule all:
    input: "results/summary.tsv", expand("results/checks/{t}.txt", t=list(IMG))

# one tiny job per image: records libc, bash, and that python/snakemake are NOT in the image
for _t in IMG:
    rule:
        name: f"check_{_t}"
        output: f"results/checks/{_t}.txt"
        container: IMG[_t]
        resources: mem_mb=256
        shell:
            f"""( echo "arch: $(uname -m)"; echo "libc: $(ldd --version 2>&1 | head -n1)"; echo "bash: $(command -v bash)"; echo "python3: $(command -v python3 || echo none)"; echo "snakemake: $(command -v snakemake || echo none)"; echo "tool: $({TOOLCMD[_t]} 2>&1 | head -n1)" ) > {{output}}"""

rule make_reference:
    output: "results/ref/ref.fa"
    container: IMG["ref"]
    resources: mem_mb=256
    params: prog=lambda wildcards: AWK_REF
    shell: "awk '{params.prog}' > {output}"

rule bwa_index:
    input: "results/ref/ref.fa"
    output: multiext("results/ref/ref.fa", *IDX)
    container: IMG["bwa"]
    resources: mem_mb=3000
    shell: "bwa index {input}"

rule faidx:
    input: "results/ref/ref.fa"
    output: "results/ref/ref.fa.fai"
    container: IMG["samtools"]
    resources: mem_mb=512
    shell: "samtools faidx {input}"

rule simulate:
    input: "results/ref/ref.fa"
    output: r1="results/reads/{sample}_R1.fq", r2="results/reads/{sample}_R2.fq"
    container: IMG["sim"]
    resources: mem_mb=512
    params: prog=lambda wildcards: AWK_SIM, seed=lambda wc: SAMPLES.index(wc.sample) * 7 + 1
    shell: "awk -v seed={params.seed} -v n=3000 -v len=100 -v o1={output.r1} -v o2={output.r2} '{params.prog}' {input}"

rule trim:
    input: r1="results/reads/{sample}_R1.fq", r2="results/reads/{sample}_R2.fq"
    output: r1="results/trimmed/{sample}_R1.fq", r2="results/trimmed/{sample}_R2.fq", json="results/trimmed/{sample}.json", html="results/trimmed/{sample}.html"
    container: IMG["fastp"]
    threads: 2
    resources: mem_mb=3000
    shell: "fastp -i {input.r1} -I {input.r2} -o {output.r1} -O {output.r2} -j {output.json} -h {output.html} -w {threads}"

rule align:
    input: fa="results/ref/ref.fa", idx=multiext("results/ref/ref.fa", *IDX), r1="results/trimmed/{sample}_R1.fq", r2="results/trimmed/{sample}_R2.fq"
    output: "results/aligned/{sample}.sam"
    container: IMG["bwa"]
    threads: 4
    resources: mem_mb=3000
    shell: "bwa mem -t {threads} {input.fa} {input.r1} {input.r2} > {output}"

rule sort_index:
    input: "results/aligned/{sample}.sam"
    output: bam="results/aligned/{sample}.bam", bai="results/aligned/{sample}.bam.bai"
    container: IMG["samtools"]
    threads: 2
    resources: mem_mb=3000
    shell: "samtools sort -@ {threads} -o {output.bam} {input} && samtools index {output.bam}"

rule call:
    input: fa="results/ref/ref.fa", fai="results/ref/ref.fa.fai", bam="results/aligned/{sample}.bam", bai="results/aligned/{sample}.bam.bai"
    output: "results/calls/{sample}.vcf"
    container: IMG["bcftools"]
    resources: mem_mb=3000
    shell: "bcftools mpileup -f {input.fa} -Ou {input.bam} | bcftools call -mv -Ov -o {output}"

rule summary:
    input: expand("results/calls/{sample}.vcf", sample=SAMPLES)
    output: "results/summary.tsv"
    container: IMG["ref"]
    resources: mem_mb=256
    shell:
        """
        printf 'sample\\tvariants\\n' > {output}
        for f in {input}; do s=$(basename $f .vcf); n=$(grep -vc '^#' $f || true); printf '%s\\t%s\\n' $s $n >> {output}; done
        """
