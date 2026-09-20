# Nix packaging for the TypeDB server

This directory adds Nix source-build packaging **alongside** the existing
Bazel build. Nothing about Bazel, CI, or the release process changes.

## Build

```sh
nix build .#typedb-server
./result/bin/typedb-server --version
```

The package builds the Cargo workspace (`typedb_server_bin`) with the
standard `rustPlatform.buildRustPackage`, installed as `typedb-server`.
The reported version comes from the `VERSION` file baked in at compile
time (`resource/constants.rs`), matching the release artifacts.

## Develop

```sh
nix develop
cargo build --bin typedb_server_bin
```

The shell provides the Rust toolchain plus the native build inputs
(`protobuf` for `tonic-build`, compression libraries for RocksDB).

## Run

```sh
nix run .#typedb-server -- \
  --config ./server/config.yml \
  --storage.data-directory ./data \
  --server.listen-address 127.0.0.1:1729
```

A reference `server/config.yml` ships as
`$out/share/typedb/config.yml.example`. The server mandates a config file
(a bare `config.yml` resolves against the executable directory, so pass
`--config` explicitly outside a checkout); CLI flags override file values.
The server never writes to the Nix store: point `--storage.data-directory`
(and logging `directory`) at a writable path. See `server/config.yml` for
every option, also settable as `--dotted.path` CLI flags.

## Checks

- `nix flake check` builds the package, runs the hermetic unit suites
  (`cargo test --locked --lib --bins`), and runs the `--version` /
  `--help` smoke checks.
- The integration suites (assembly, behaviour, crash recovery) start
  server processes and are excluded from the Nix gate; run them with
  Bazel or Cargo directly (see CONTRIBUTING.md).

## Test status

- `x86_64-linux`: built and smoke-tested via this flake (see the PR
  description for the exact run).
- `aarch64-linux`: declared; validated when a remote run reports back.

## Updating

- `VERSION` / `Cargo.lock` change: `nix flake lock` needs no input
  updates (only `nixpkgs` is an input), but the `cargoLock` vendor hash
  refreshes automatically on the next build.
- `nixpkgs` input: `nix flake update nixpkgs` (or `--update-input
  nixpkgs`), then `nix flake check` before committing the lock.
- Verify a clean checkout evaluates without dirtying the lock:
  `git status --porcelain -- flake.lock` must stay empty after
  `nix flake show`.
