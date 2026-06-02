{
  description = "etl dev environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        rustToolchain =
          (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
            extensions = [ "rust-src" "rust-analyzer" ];
          };

        buildDeps = with pkgs; [
          cmake
          openssl
          pkg-config
          protobuf
        ];

        devTools = with pkgs; [
          cargo-deny
          cargo-edit
          cargo-llvm-cov
          cargo-sort
          cargo-udeps
          git-cliff
          python3
        ];

      in {
        devShells.default = pkgs.mkShell {
          packages = [ rustToolchain ] ++ buildDeps ++ devTools;

          shellHook = ''
            export RUST_SRC_PATH=${rustToolchain}/lib/rustlib/src/rust/library
          '';
        };
      });
}
