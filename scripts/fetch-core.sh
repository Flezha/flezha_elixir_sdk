#!/usr/bin/env bash
# Fetch what this package needs from one core release: the fake controller, the
# contract set, and the precompiled socket client with the checksum file it is
# verified against.
#
# This package is proven against a core it does not contain. A release of the
# core publishes each of those as an asset with its SHA-256 in one manifest, so
# obtaining them is a download and a checksum rather than a checkout and a Rust
# toolchain, and nothing is used before its digest matches.
#
#   scripts/fetch-core.sh --from <dir|url> [--version 0.1.0]
#
# `--from` is a staged release directory (what `scripts/build-dist.sh
# --stage-release` writes in the core's tree) or the URL a GitHub release serves
# its assets from. Everything it installs is build output and is ignored by git:
# the fake controller at `.core/`, the contract set at `contract/`, the checksum
# file at the package root, and this host's socket client archive at
# `.core/nifs/`, the directory `RUSTLER_PRECOMPILED_GLOBAL_CACHE_PATH` names.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

FROM=""
VERSION="0.1.0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)    FROM="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

refuse() {
  echo "fetch-core: $*" >&2
  exit 1
}

[[ -n "${FROM}" ]] || refuse "--from <dir|url> is required"
command -v sha256sum >/dev/null 2>&1 || refuse "sha256sum is required"

# The platform a core release names its fake controller by, and the triple it
# names the socket client by: the directory column of the core's
# `scripts/native_triples.sh`, and the triple itself.
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)               platform=linux-x64;   triple=x86_64-unknown-linux-gnu ;;
  Linux-aarch64|Linux-arm64)  platform=linux-arm64; triple=aarch64-unknown-linux-gnu ;;
  Darwin-x86_64)              platform=macos-x64;   triple=x86_64-apple-darwin ;;
  Darwin-arm64)               platform=macos-arm64; triple=aarch64-apple-darwin ;;
  *) refuse "no core release asset is published for $(uname -s) $(uname -m)" ;;
esac

scratch=.core/download
rm -rf "${scratch}"
mkdir -p "${scratch}"

# One asset's bytes, from a directory or a release URL.
asset() {
  local name="$1" into="$2"
  if [[ "${FROM}" =~ ^https?:// ]]; then
    curl -fsSL "${FROM%/}/${name}" -o "${into}" || refuse "${name}: not downloadable from ${FROM}"
  else
    [[ -f "${FROM}/${name}" ]] || refuse "${FROM}/${name} is not there"
    cp "${FROM}/${name}" "${into}"
  fi
}

# One asset, checked against the manifest and moved to `into` only if it matches.
verified() {
  local name="$1" into="$2" expected actual
  expected="$(awk -v name="${name}" '$2 == name || $2 == "*" name { print $1 }' "${scratch}/SHA256SUMS.txt")"
  [[ -n "${expected}" ]] || refuse "${name} is not in SHA256SUMS.txt; it was not part of this release"
  asset "${name}" "${scratch}/${name}"
  actual="$(sha256sum "${scratch}/${name}" | awk '{ print $1 }')"
  [[ "${actual}" == "${expected}" ]] || refuse "${name} does not match its manifest digest"
  mkdir -p "$(dirname "${into}")"
  mv "${scratch}/${name}" "${into}"
  echo "  verified ${name} -> ${into}"
}

echo "fetch-core: taking the ${VERSION} core from ${FROM}"
asset SHA256SUMS.txt "${scratch}/SHA256SUMS.txt"

verified "flz-fake-controller-${platform}" .core/flz-fake-controller
chmod +x .core/flz-fake-controller

verified "flezha-fixtures-${VERSION}.tar.gz" "${scratch}/fixtures.tar.gz"
rm -rf contract
mkdir -p contract
tar -xzf "${scratch}/fixtures.tar.gz" -C contract
echo "  unpacked the contract set -> contract/"

verified checksum-Elixir.Flezha.SDK.Native.exs checksum-Elixir.Flezha.SDK.Native.exs
nif="libflz_sdk_nif-v${VERSION}-nif-2.16-${triple}.so.tar.gz"
rm -rf .core/nifs
verified "${nif}" ".core/nifs/${nif}"

rm -rf "${scratch}"
echo "fetch-core: done"
