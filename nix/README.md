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

- `nix flake check` builds the package and runs the workspace library and
  binary unit suites, excluding `typedb_server_bin`, `steps`, and
  `http_steps`. The root server binary is covered by the `--version` /
  `--help` smoke checks.
- The protocol dependency is patched to a pinned, complete source tree
  because its build script needs files outside the vendored Rust
  subdirectory. This path patch updates the lock offline during the build;
  the Nix build does not enforce `--locked` on the patched workspace.
- The integration suites (assembly, behaviour, crash recovery) start
  server processes and are excluded from the Nix gate; run them with
  Bazel or Cargo directly (see CONTRIBUTING.md).

## Test status

- `x86_64-linux` and `aarch64-linux` are declared targets. Full flake
  builds, unit suites, and smoke checks remain pending until a hosted run
  qualifies the exact PR revision on each platform.
- Downstream Nixpkgs builds validate its release-source recipe, not this
  flake or its pinned dependencies. See the PR description for current
  revision-bound evidence.

## Updating

- `VERSION` / `Cargo.lock` change: `nix flake lock` needs no input
  updates (only `nixpkgs` is an input). If a Git dependency changes,
  update its fixed-output hash in `cargoLock.outputHashes` in
  `nix/typedb-server.nix`. When the protocol revision changes, also update
  `protocolFull.rev` and `protocolFull.narHash` to match that revision.
  These hashes do not refresh automatically.
- `nixpkgs` input: `nix flake update nixpkgs` (or `--update-input
  nixpkgs`), then `nix flake check` before committing the lock.
- Verify a clean checkout evaluates without dirtying the lock:
  `git status --porcelain -- flake.lock` must stay empty after
  `nix flake show`.
