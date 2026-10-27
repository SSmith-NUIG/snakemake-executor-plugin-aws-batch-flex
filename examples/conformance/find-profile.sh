# Sourced by run.sh and orphan-test.sh: the profile the tests run with. profile/config.yaml here
# if there is one, otherwise the one setup.sh installs (SMKFLEX_PROFILE, default smkflex, in
# ~/.config/snakemake/). Sets PROFILE (for --profile), CFG (its config.yaml) and TEST_ARGS.
if [ -f profile/config.yaml ]; then
  PROFILE=profile
else
  PROFILE="${XDG_CONFIG_HOME:-$HOME/.config}/snakemake/${SMKFLEX_PROFILE:-smkflex}"
  if [ ! -f "$PROFILE/config.yaml" ]; then
    echo "No profile: run scripts/setup.sh first (it installs $PROFILE), or put one in profile/config.yaml here." >&2
    exit 1
  fi
fi
CFG="$PROFILE/config.yaml"
# small jobs and few at a time, unless a test asks for more (later options win)
TEST_ARGS=(--default-resources mem_mb=512 -j 5)
echo "Using profile $PROFILE"
