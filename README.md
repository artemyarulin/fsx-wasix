# fsx-wasix

`fsx-wasix` is a WASIX-focused filesystem exerciser for running under Wasmer.
It performs randomized file operations and verifies file contents after each
step. Multi-worker mode runs one independent exerciser per worker thread, with
each worker using its own isolated subdirectory.

Initially forked from https://github.com/asomers/fsx-rs, thanks! 


## CLI: Exercise `/tmp`

This exercises Wasmer's RAM filesystem at `/tmp`.

Create a config file for a 10 MiB max file and the same WASIX-safe operation
mix used in the Wasmer runs:

```sh
cat > fsx-10mb.toml <<'EOF'
flen = 10485760

[opsize]
min = 1
max = 65536

[weights]
close_open = 1
read = 1
write = 1
mapread = 1
mapwrite = 1
truncate = 1
fsync = 0
fdatasync = 0
EOF
```

The weights are relative. With the config above, `close_open`, `read`,
`write`, `mapread`, `mapwrite`, and `truncate` are selected evenly. `mapread`
and `mapwrite` are still in the distribution, but in this WASIX-focused build
they use positioned I/O. `fsync` and `fdatasync` are disabled unless you raise
their weights.

Run multi-worker mode, using Wasmer's logical CPU count by omitting `-j`, or
choose a specific count:

```sh
wasmer run --volume "$PWD:/work" \
  target/wasm32-wasmer-wasi/debug/fsx-wasix.wasm -- \
  -f /work/fsx-10mb.toml -N 10000 -S 1 -j4 /tmp/fsxfile
```

In multi-worker mode the tool creates isolated directories such as
`/tmp/thread-1/fsxfile`, `/tmp/thread-2/fsxfile`, and so on.

## CLI: Exercise a Mounted Volume

This exercises a host-backed volume mounted into the guest at `/data`.

```sh
mkdir -p wasmer-data
```

Run multi-worker mode:

```sh
wasmer run --volume "$PWD:/work" --volume "$PWD/wasmer-data:/data" \
  target/wasm32-wasmer-wasi/debug/fsx-wasix.wasm -- \
  -f /work/fsx-10mb.toml -N 10000 -S 1 -j4 /data/fsxfile
```

## Controlling Run Size

`-N` controls operations per worker. For example, `-N 10000 -j4` plans 40,000
total operations. Use a seed with `-S` to make the run reproducible.

## Oracle Mode

Oracle mode exhaustively runs generated sequences of `open`, `close`, `write`,
and `read` commands across two worker threads, two logical handles, and two
files. It does not prune invalid sequences. Instead, run it natively first to
capture the expected transcript, then run the same sequence under Wasmer and
compare against that transcript.

The current operation catalog is explicit in `tester_oracle.rs`:
`open(read_write_create)`, `open(append_create)`, `close`,
`write(zero_bytes)`, `write(32_bytes)`, `write(32_kb)`, `read(32_bytes)`,
`write_stderr`, and `delete`.
The generator expands each operation across both FD handles, and expands
file-scoped operations such as `open` across the files listed in the `FILES`
constant. The default is currently two files: `A` and `B`.

The easiest way to run the full native, Wasmer, and host-side verification flow
is:

```sh
./run.sh
```

Edit `CHAIN_LENGTH` at the top of `run.sh` to change the sequence length.
The script writes compact binary reports and cleans old `oracle-runs/` output
at startup by default. On successful runs it also removes the large case-file
trees and keeps only `native-report.bin` and `wasix-report.bin`.
Native oracle reports are cached under `oracle-cache/`; set
`USE_NATIVE_CACHE=0` to force regeneration.

Native oracle:

```sh
mkdir -p /tmp/fsx-oracle-native

cargo run -- --oracle -N 2 \
  --oracle-output /tmp/fsx-oracle-native/report.bin \
  /tmp/fsx-oracle-native

cp /tmp/fsx-oracle-native/report.bin fsx-oracle-native-report.bin
```

WASIX comparison on `/tmp`:

```sh
wasmer run --volume "$PWD:/work" \
  target/wasm32-wasmer-wasi/debug/fsx-wasix.wasm -- \
  --oracle -N 2 \
  --oracle-expected /work/fsx-oracle-native-report.bin \
  /tmp/fsx-oracle-wasix
```

WASIX comparison on a mounted `/data` volume:

```sh
mkdir -p wasmer-data/fsx-oracle-wasix

wasmer run --volume "$PWD:/work" --volume "$PWD/wasmer-data:/data" \
  target/wasm32-wasmer-wasi/debug/fsx-wasix.wasm -- \
  --oracle -N 2 \
  --oracle-expected /work/fsx-oracle-native-report.bin \
  --oracle-output /data/fsx-oracle-wasix/report.bin \
  /data/fsx-oracle-wasix
```

For mounted volumes, run an additional host-side verification pass after Wasmer
exits. This compares the native and WASIX reports, then re-reads the files
directly from the host mount so the final content check does not rely on WASIX:

```sh
cargo run -- --oracle-verify-files \
  /tmp/fsx-oracle-native/report.bin \
  wasmer-data/fsx-oracle-wasix/report.bin
```

For oracle mode, `-N` is the command sequence length. Length 2 is a quick smoke
run. Length 3 is much larger because each command position can be assigned to
either worker thread.

## HTTP Server

The same tool can run as a small HTTP server. Start it with `--server` and
Wasmer networking enabled:

```sh
mkdir -p wasmer-data

wasmer run --net --volume "$PWD:/work" --volume "$PWD/wasmer-data:/data" \
  target/wasm32-wasmer-wasi/debug/fsx-wasix.wasm -- \
  --server 3020
```

Health check:

```sh
curl 'http://127.0.0.1:3020/health'
```

### HTTP: Exercise `/tmp`

```sh
curl 'http://127.0.0.1:3020/run?cwd=/tmp&file=fsxfile&numops=10000&threads=4&seed=1&flen=10485760&opsize_min=1&opsize_max=65536&close_open=1&read=1&write=1&mapread=1&mapwrite=1&truncate=1&fsync=0&fdatasync=0'
```

The response is JSON. A successful run returns `ok: true`; a verification
failure returns HTTP 500 with `ok: false` and an error report.

### HTTP: Exercise a Mounted Volume

Start the server with `/data` mounted as shown above, then call:

```sh
curl 'http://127.0.0.1:3020/run?cwd=/data&file=fsxfile&numops=10000&threads=4&seed=1&flen=10485760&opsize_min=1&opsize_max=65536&close_open=1&read=1&write=1&mapread=1&mapwrite=1&truncate=1&fsync=0&fdatasync=0'
```

HTTP parameters mirror the CLI/config values:

- `cwd`: directory to run in, such as `/tmp` or `/data`.
- `file`: target file name under `cwd`.
- `numops` or `n`: operations per worker.
- `threads` or `j`: worker count.
- `seed`: deterministic RNG seed.
- `flen`: maximum file size.
- `opsize_min`, `opsize_max`, `opsize_align`: operation size controls.
- `close_open`, `read`, `write`, `mapread`, `mapwrite`, `truncate`, `fsync`,
  `fdatasync`: relative operation weights.
- `timeout_ms`: return HTTP 504 if the request takes too long.
