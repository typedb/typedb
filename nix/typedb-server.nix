# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

{ pkgs, ... }:
let
  version = builtins.replaceStrings [ "\n" ] [ "" ] (builtins.readFile ../VERSION);
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

  # Unit tests only for the build gate: lib/bins are hermetic, while the
  # integration suites (assembly, behaviour, crash recovery) start server
  # processes and need excluded infrastructure. Narrowed with evidence from
  # remote builds; see nix/README.md.
  doCheck = true;
  checkFlags = [
    "--locked"
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
