# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

{ pkgs, ... }:
let
  version = builtins.replaceStrings [ "\n" ] [ "" ] (builtins.readFile ../VERSION);
  # Full protocol tree: its build script compiles ../../proto/*.proto,
  # which subdirectory vendoring drops. Patched in as a path dependency;
  # hash recomputed whenever Cargo.lock moves the rev.
  protocolFull = builtins.fetchGit {
    url = "https://github.com/typedb/typedb-protocol.git";
    rev = "310fef4e4ed2e735ed0fb2dc3248b4a35e604806";
    narHash = "sha256-h3FzTYDvAnj/ma/rqT4nnvAqnKSMKNsKj5KDvnNFqp4=";
  };
in
pkgs.rustPlatform.buildRustPackage {
  pname = "typedb";
  inherit version;

  src = pkgs.lib.cleanSource ../.;

  cargoLock = {
    lockFile = ../Cargo.lock;
    # Git checkouts pinned by Cargo.lock revs, hashed as fetchgit worktrees
    # (worktree without .git). Recompute after any lock change touching them.
    outputHashes = {
      "typedb-protocol-0.0.0" = "sha256-h3FzTYDvAnj/ma/rqT4nnvAqnKSMKNsKj5KDvnNFqp4=";
      "typeql-0.0.0" = "sha256-VMYW5fmEN4oWMIOmmg6oR9txKSaybMBC837qLAayu8E=";
    };
  };

  nativeBuildInputs = with pkgs; [
    pkg-config
    # tonic-build (server/service/admin/proto) needs protoc at build time.
    protobuf
    # librocksdb-sys runs bindgen at build time; the hook provides libclang.
    rustPlatform.bindgenHook
  ];

  buildInputs = with pkgs; [
    # RocksDB (storage) C++ compression dependencies.
    lz4
    zlib
    zstd
    bzip2
    snappy
  ];

  # The git protocol dep cannot build from a subdirectory vendor copy, so
  # it is patched to the full tree above. The lock updates offline (path
  # sources are local); --locked is not enforced. Re-verify against remote
  # builds; see nix/README.md.
  postPatch = ''
    cat >> Cargo.toml <<EOF
    [patch."https://github.com/typedb/typedb-protocol"]
    typedb-protocol = { path = "${protocolFull}/grpc/rust" }
    EOF
  '';

  # Unit suites only: member libs are hermetic. Excluded: the root binary
  # (covered by the smoke checks), and the steps/http_steps behaviour-test
  # helpers, whose `bdd` imports postdate the pinned protocol (upstream
  # tests those through Bazel, not cargo). cargoTestFlags (not checkFlags:
  # those land after `--` as test-binary args). Re-verify against remote
  # builds.
  doCheck = true;
  cargoTestFlags = [
    "--workspace"
    "--exclude"
    "typedb_server_bin"
    "--exclude"
    "steps"
    "--exclude"
    "http_steps"
    "--lib"
    "--bins"
  ];

  postInstall = ''
    mv $out/bin/typedb_server_bin $out/bin/typedb-server
    mkdir -p $out/share/typedb
    cp ${../server/config.yml} $out/share/typedb/config.yml.example
  '';

  passthru = {
    # Server reports its release version from the VERSION file baked in at
    # compile time (resource/constants.rs), not the Cargo package version.
    inherit version;
  };

  meta = with pkgs.lib; {
    description = "TypeDB: a strongly-typed database with a rich and logical type system";
    homepage = "https://typedb.com";
    license = licenses.mpl20;
    mainProgram = "typedb-server";
    platforms = [
      "x86_64-linux"
      "aarch64-linux"
    ];
  };
}
