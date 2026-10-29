#!/usr/bin/env bash
# Known Snakemake pipelines on real AWS Batch, each with its own test data, at a fixed release.
# Usage:  bash run.sh [--check] NAME [NAME ...]   (one after the other; `all` for every one)
#   --check   set each one up and dry-run it here with plain Snakemake: no AWS, nothing to pay
#   MODE=head bash run.sh NAME   run it with `smkflex run --wait` (Snakemake on Batch too)
#   PROFILE=...  the plugin's profile (default: ../conformance/profile)
#   WORK=...     where the pipelines are cloned (default: ~/smkflex-pipelines)
# Each is cloned into $WORK/NAME, set up as its own CI test does but run from the top of the
# repository (the plugin can't run with --directory .test), its inputs uploaded with smkflex push,
# then run, with --retries 2: a job that ran out of memory is retried with twice as much, as most
# pipelines set no mem_mb. The log is $WORK/NAME.log; results.tsv here gets one line per run.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
PROFILE=${PROFILE:-$HERE/../conformance/profile}
WORK=${WORK:-$HOME/smkflex-pipelines}
MODE=${MODE:-direct}
ALL="tutorial rnaseq kallisto nanopore varlociraptor zarp grenepipe"

# The test folder's contents at the top: its configs, sample sheets and data where its paths
# expect them (without the submodules' .git files, which only work at their own place). The
# repository's example config/ goes: its CI runs in .test, where there is none, and Snakemake
# would merge it into the test's --configfile.
overlay() { rm -rf config && tar -C .test --exclude=.git -cf - . | tar -xf -; }

# NAME: repository, release tag, setup (run in the clone), inputs to upload, snakemake arguments
recipe() {
  case $1 in
    tutorial)  # the Snakemake tutorial's workflow (docs: "Basics"), on its data
      REPO=snakemake/snakemake-tutorial-data TAG=v9.0.0
      setup() { cp -r "$HERE/tutorial/." .; }
      INPUTS="data" ARGS="--sdm conda -j 4";;
    rnaseq)
      REPO=snakemake-workflows/rna-seq-star-deseq2 TAG=v3.1.2
      setup() { overlay; }
      INPUTS="ngs-test-data" ARGS="--configfile config_basic/config.yaml --sdm conda -j 10 --default-resources mem_mb=4000 --retries 2";;
    kallisto)
      REPO=snakemake-workflows/rna-seq-kallisto-sleuth TAG=v4.1.1
      setup() { overlay; }
      INPUTS="ngs-test-data resources config" ARGS="--sdm conda -j 10 --default-resources mem_mb=4000 --retries 2";;
    nanopore)
      REPO=snakemake-workflows/transcriptome-differential-expression TAG=v2.6.0
      setup() { overlay; }
      INPUTS="ngs-test-data" ARGS="--configfile config-simple/config.yml --sdm conda -j 10 --default-resources mem_mb=4000 --retries 2";;
    varlociraptor)
      REPO=snakemake-workflows/dna-seq-varlociraptor TAG=v6.11.4
      setup() { overlay; }
      INPUTS="data" ARGS="--configfile config-simple/config.yaml --sdm conda -j 10 --default-resources mem_mb=4000 --retries 2";;
    zarp)  # a container: per rule (biocontainers and others); its tests name ../input_files
      REPO=zavolanlab/zarp TAG=v1.0.0-rc.1
      setup() { cp -r tests/input_files input_files
                sed -i 's#\.\./input_files/#input_files/#g; s#\.\./\.\./images/#images/#g' input_files/*.yaml input_files/*.tsv; }
      INPUTS="input_files" ARGS="--configfile input_files/config.yaml -j 10 --default-resources mem_mb=4000 --retries 2";;
    grenepipe)  # inputs by absolute path, as its example sets them up
      REPO=lczech/grenepipe TAG=v0.15.0
      setup() { bash example/prepare.sh && cp example/config.yaml config.yaml; }
      INPUTS="$WORK/grenepipe/example" ARGS="--sdm conda -j 10 --default-resources mem_mb=4000 --retries 2";;
    *) echo "unknown pipeline $1 (known: $ALL)" >&2; return 1;;
  esac
}

prepare() {  # a fresh clone at the tag, set up
  rm -rf "$WORK/$1" && mkdir -p "$WORK" || return 1
  git -c url."https://github.com/".insteadOf="git@github.com:" clone -q --depth 1 --branch "$TAG" \
    --recurse-submodules --shallow-submodules "https://github.com/$REPO" "$WORK/$1" || return 1
  cd "$WORK/$1" && setup && printf 'ngs-test-data/\ninput_files/\n' >> .gitignore
}

check=0; [ "${1:-}" = --check ] && { check=1; shift; }
if [ $# = 0 ]; then echo "Say which: bash run.sh [--check] NAME ...  ($ALL, or all)" >&2; exit 2; fi
[ "$*" = all ] && set -- $ALL
pass=(); fail=()
for name in "$@"; do
  recipe "$name" || { fail+=("$name"); continue; }
  echo "=== $name ($REPO $TAG)"
  log="$WORK/$name.log"
  mkdir -p "$WORK"
  if ! (prepare "$name") > "$log" 2>&1 || ! cd "$WORK/$name"; then
    echo "  setup FAILED (see $log)"; tail -5 "$log" | sed 's/^/    /'; fail+=("$name"); cd "$HERE"; continue
  fi
  if [ $check = 1 ]; then
    # shellcheck disable=SC2086
    if snakemake -n --workflow-profile none $ARGS >> "$log" 2>&1; then
      echo "  dry run OK: $(grep -E '^total +[0-9]+' "$log" | tail -1 | awk '{print $2}') jobs"; pass+=("$name")
    else echo "  dry run FAILED (see $log)"; fail+=("$name"); fi
    cd "$HERE"; continue
  fi
  start=$(date +%s)
  # shellcheck disable=SC2086
  if smkflex init --profile "$PROFILE" --name "pipelines-$name" >> "$log" 2>&1 \
     && smkflex push --profile "$PROFILE" $INPUTS >> "$log" 2>&1; then
    if [ "$MODE" = head ]; then
      smkflex run --profile "$PROFILE" --wait $ARGS >> "$log" 2>&1
    else
      snakemake --profile "$PROFILE" $ARGS >> "$log" 2>&1
    fi
    rc=$?
  else rc=setup; fi
  mins=$(( ($(date +%s) - start + 59) / 60 ))
  steps=$(grep -oE '[0-9]+ of [0-9]+ steps \([0-9]+%\) done' "$log" | tail -1)
  if [ "$rc" = 0 ]; then echo "  PASS in $mins min ($steps)"; pass+=("$name")
  else
    # failed jobs per rule (an earlier one may have been retried and passed), then the last
    # error, which is the one Snakemake gave up on
    echo "  FAILED ($rc) after $mins min${steps:+ at $steps}; failed jobs:"
    grep "^Error in rule" "$log" | sort | uniq -c | sed 's/^ */    /'
    echo "  last error:"
    awk '/^Error in rule|^MissingInputException|^WorkflowError/{buf=""} {buf=buf $0 "\n"} END{printf "%s", buf}' "$log" \
      | grep -E "^Error in rule|^[A-Za-z]+(Exception|Error)|message:|container reason|end of log file|hint:|[Ee]rror|Unable|denied|No such" \
      | grep -v -E "check log file|CalledProcessError|Exiting because|At least one job" | head -10 | cut -c1-220 | sed 's/^ */    /'
    fail+=("$name")
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s min\t%s\n' "$(date +%F)" "$name" "$TAG" "$MODE" \
    "$([ "$rc" = 0 ] && echo PASS || echo FAIL)" "$mins" "$steps" >> "$HERE/results.tsv"
  cd "$HERE"
done
echo; echo "PASS: ${pass[*]:-none}"; echo "FAIL: ${fail[*]:-none}"
