#!/usr/bin/env bash
set -euo pipefail

# Main knob: sequence length for the exhaustive oracle generator.
CHAIN_LENGTH="${CHAIN_LENGTH:-4}"

# Disk-safety knob: binary reports are compact, but old run directories can
# still accumulate many case files. Set to 0 if you want to keep history.
CLEAN_OLD_RUNS_ON_START="${CLEAN_OLD_RUNS_ON_START:-0}"
CLEAN_CASE_FILES_ON_SUCCESS="${CLEAN_CASE_FILES_ON_SUCCESS:-1}"

# Output and mount layout. Edit these if you want to keep runs somewhere else.
RUN_ID="$(date +%Y%m%d-%H%M%S)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ROOT="${SCRIPT_DIR}/oracle-runs/${RUN_ID}"
NATIVE_ROOT="${RUN_ROOT}/native-root"
NATIVE_REPORT="${RUN_ROOT}/native-report.bin"
NATIVE_STDERR_LOG="${RUN_ROOT}/native-stderr.log"
VOLUME_ROOT="${RUN_ROOT}/volume"
WASIX_ROOT_GUEST="/data/fsx-oracle-wasix"
WASIX_REPORT_GUEST="${WASIX_ROOT_GUEST}/report.bin"
WASIX_REPORT_HOST="${VOLUME_ROOT}/fsx-oracle-wasix/report.bin"
WASIX_REPORT_ARCHIVE="${RUN_ROOT}/wasix-report.bin"
WASIX_STDERR_LOG="${RUN_ROOT}/wasix-stderr.log"
WASM="${SCRIPT_DIR}/target/wasm32-wasmer-wasi/debug/fsx-wasix.rustc.wasm"
NATIVE_BIN="${SCRIPT_DIR}/target/debug/fsx-wasix"
WASMER="${HOME}/wasmer/wasmer2/target/debug/wasmer"

cd "${SCRIPT_DIR}"

if [[ "${CLEAN_OLD_RUNS_ON_START}" == "1" ]]; then
  echo "==> Cleaning old oracle-runs"
  rm -rf "${SCRIPT_DIR}/oracle-runs"
fi

mkdir -p "${NATIVE_ROOT}" "${VOLUME_ROOT}/fsx-oracle-wasix"

echo "==> Building native and WASIX artifacts"
cargo build
cargo wasix build

if [[ ! -s "${WASM}" ]]; then
  echo "error: expected non-empty WASIX artifact at ${WASM}" >&2
  echo "       cargo-wasix may have produced an empty post-processed .wasm; check target/wasm32-wasmer-wasi/debug/*.wasm" >&2
  exit 1
fi

echo "==> Run directory: ${RUN_ROOT}"
echo "==> Chain length: ${CHAIN_LENGTH}"
echo "==> Wasmer: ${WASMER}"
echo "==> Wasm: ${WASM}"

echo "==> 1/3 Running native oracle"
"${NATIVE_BIN}" --oracle \
  -N "${CHAIN_LENGTH}" \
  --oracle-output "${NATIVE_REPORT}" \
  "${NATIVE_ROOT}" \
  2>"${NATIVE_STDERR_LOG}"

if [[ "${CLEAN_CASE_FILES_ON_SUCCESS}" == "1" ]]; then
  rm -rf "${NATIVE_ROOT}"
fi

echo "==> 2/3 Running Wasmer oracle on mounted volume"
"${WASMER}" run \
  --volume "${RUN_ROOT}:/work" \
  --volume "${VOLUME_ROOT}:/data" \
  "${WASM}" -- \
  --oracle \
  -N "${CHAIN_LENGTH}" \
  --oracle-expected /work/native-report.bin \
  --oracle-output "${WASIX_REPORT_GUEST}" \
  "${WASIX_ROOT_GUEST}" \
  2>"${WASIX_STDERR_LOG}"

echo "==> 3/3 Verifying files from host filesystem"
"${NATIVE_BIN}" --oracle-verify-files \
  "${NATIVE_REPORT}" \
  "${WASIX_REPORT_HOST}"

if [[ "${CLEAN_CASE_FILES_ON_SUCCESS}" == "1" ]]; then
  cp "${WASIX_REPORT_HOST}" "${WASIX_REPORT_ARCHIVE}"
  rm -rf "${VOLUME_ROOT}"
  WASIX_REPORT_HOST="${WASIX_REPORT_ARCHIVE}"
fi

echo "==> OK: no oracle mismatch or external file corruption found"
echo "==> Reports:"
echo "    native: ${NATIVE_REPORT}"
echo "    wasix:  ${WASIX_REPORT_HOST}"
echo "    native stderr: ${NATIVE_STDERR_LOG}"
echo "    wasix stderr:  ${WASIX_STDERR_LOG}"
