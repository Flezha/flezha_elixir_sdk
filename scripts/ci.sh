#!/usr/bin/env bash
# The three tiers this repository runs, and no controller anywhere.
#
#   unit        the package's own suite, including the fixture checks that read
#               the contract set
#   local mode  the declared graph run in process, in the same suite
#               (`test/flezha/sdk/local_test.exs`)
#   smoke       the fake controller on a socket directory, this repository's own
#               test app launched against it over UDS, and the binary's exit code
#
# Everything the core contributes is fetched from one of its releases and
# checked against that release's manifest: the fake controller, the contract set
# and the precompiled socket client. Nothing here builds the core, and nothing
# here needs a checkout of it.
#
#   scripts/ci.sh --core <dir|url> [--version 0.1.0]
#
# What this cannot see is the fifteen-capability vocabulary the controller
# owns: persistence, effectively-once, correction, partitioning, workflows,
# liveness, restarts. Those are end-to-end scenarios and they run in the core's
# own tree, against the commit it pins this repository at, as does the in-node
# `:native` transport.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CORE=""
VERSION="0.1.0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --core)    CORE="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
[[ -n "${CORE}" ]] || { echo "--core <dir|url> is required" >&2; exit 1; }

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

step "fetching the core ${VERSION}"
scripts/fetch-core.sh --from "${CORE}" --version "${VERSION}"

# The socket client is taken from the archive the fetch verified, never built.
export RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH="${PWD}/.core/nifs"

step "installing"
mix deps.get

step "compiling"
mix compile --warnings-as-errors

step "unit and local mode"
mix test

step "compiling the test app"
( cd test_app && mix compile --warnings-as-errors )

step "smoke"
# The fake controller launches the runner against a socket directory of its own,
# holds its standard input open for the run and prints its output on failure.
# The tier's verdict is the fake controller's, never the runner's: a terminal
# disconnect ends the runner non-zero on purpose, so the orchestrator that owns
# it restarts it.
exec .core/flz-fake-controller \
  --factory do_nothing --operative echo-1 --channel input \
  --input alpha --input beta --timeout-ms 60000 \
  -- sh -c 'cd test_app && FLEZHA_TRANSPORT=uds exec mix run --no-halt --no-deps-check -- smoke'
