# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

{
  description = "TypeDB server (Nix source-build packaging alongside the Bazel build)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f pkgsFor.${system});
      pkgsFor = nixpkgs.lib.genAttrs systems (
        system:
        import nixpkgs {
          inherit system;
          # Upstream rustPlatform is stable enough for the pinned toolchain
          # floor (see rust-toolchain requirements in MODULE.bazel); no
          # overlay needed.
        }
      );
    in
    {
      packages = forAllSystems (pkgs: {
        typedb-server = pkgs.callPackage ./nix/typedb-server.nix { };
        default = self.packages.${pkgs.stdenv.hostPlatform.system}.typedb-server;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          inputsFrom = [ self.packages.${pkgs.stdenv.hostPlatform.system}.typedb-server ];
          packages = with pkgs; [
            cargo
            rustc
            clippy
            rustfmt
            protobuf
            pkg-config
          ];
        };
      });

      checks = forAllSystems (pkgs: {
        typedb-server = self.packages.${pkgs.stdenv.hostPlatform.system}.typedb-server;
        version =
          let
            bin = self.packages.${pkgs.stdenv.hostPlatform.system}.typedb-server;
            version = builtins.replaceStrings [ "\n" ] [ "" ] (builtins.readFile ./VERSION);
          in
          pkgs.runCommand "typedb-server-version"
            {
              nativeBuildInputs = [ bin ];
            }
            ''
              out=$(typedb-server --version)
              echo "typedb-server --version: $out"
              echo "$out" | grep -q "${version}" || (echo "version mismatch" >&2; exit 1)
              touch $out
            '';
        help = pkgs.runCommand "typedb-server-help" { nativeBuildInputs = [ self.packages.${pkgs.stdenv.hostPlatform.system}.typedb-server ]; } ''
          typedb-server --help | grep -q "storage.data-directory" || (echo "CLI shape changed" >&2; exit 1)
          touch $out
        '';
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt-rfc-style);
    };
}
