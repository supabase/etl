{
  description = "etl dev environment";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    # nixpkgs 26.11 dropped x86_64-darwin, so list supported systems explicitly.
    flake-utils.lib.eachSystem [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ] (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        # Toolchain and components from rust-toolchain.toml.
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

        # Dev shell toolchain: append editor and coverage components instead of
        # replacing the components listed in rust-toolchain.toml. cargo-llvm-cov
        # needs llvm-tools, and without rustup the toolchain must provide it.
        devToolchain = rustToolchain.override (old: {
          extensions = old.extensions ++ [ "llvm-tools" "rust-src" "rust-analyzer" ];
        });

        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };

        buildDeps = with pkgs; [
          cmake
          openssl
          pkg-config
          protobuf
        ];

        devTools = with pkgs; [
          cargo-llvm-cov
          cargo-nextest
          cargo-sort
          postgresql
          python3
        ];

        src = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter = path: type:
            let
              base = baseNameOf path;
            in
            pkgs.lib.cleanSourceFilter path type
            && base != "target"
            && base != "site"
            && base != "node_modules";
        };

        # Follow the crate's metadata, including `cargo set-version` releases.
        cargoPackage = (pkgs.lib.importTOML ./crates/etl-replicator/Cargo.toml).package;

        etl-replicator = rustPlatform.buildRustPackage {
          pname = cargoPackage.name;
          inherit (cargoPackage) version;
          inherit src;

          cargoLock = {
            lockFile = ./Cargo.lock;
            outputHashes = {
              "gcp-bigquery-client-0.28.0" = "sha256-nAkVZrzVFEm7MZdSJJZ/FEbFX7gclmFfMdzerOzoSOY=";
              "postgres-protocol-0.6.12" = "sha256-4NdccoaaQ4KQAMTLAxoiv6cg2hrKzYy5Nu8nBPOGvqo=";
              "postgres-replication-0.6.7" = "sha256-4NdccoaaQ4KQAMTLAxoiv6cg2hrKzYy5Nu8nBPOGvqo=";
              "postgres-types-0.2.14" = "sha256-4NdccoaaQ4KQAMTLAxoiv6cg2hrKzYy5Nu8nBPOGvqo=";
              "tokio-postgres-0.7.18" = "sha256-4NdccoaaQ4KQAMTLAxoiv6cg2hrKzYy5Nu8nBPOGvqo=";
            };
          };

          buildAndTestSubdir = "crates/etl-replicator";

          nativeBuildInputs = with pkgs; [
            cmake
            pkg-config
            protobuf
            python3
          ];
          buildInputs = with pkgs; [ openssl ];

          # Integration tests need a live Postgres cluster.
          doCheck = false;

          meta = {
            inherit (cargoPackage) description;
            mainProgram = "etl-replicator";
          };
        };

      in {
        packages = {
          default = etl-replicator;
          inherit etl-replicator;
        };

        devShells.default = pkgs.mkShell {
          # Debug builds compile jemalloc's configure probes at -O0, where Nix's
          # _FORTIFY_SOURCE breaks its -Werror `strerror_r` check
          # (tikv-jemalloc-sys). Release builds, like the package, are unaffected.
          hardeningDisable = [ "fortify" ];

          packages = [ devToolchain ] ++ buildDeps ++ devTools;

          shellHook = ''
            export RUST_SRC_PATH=${devToolchain}/lib/rustlib/src/rust/library
          '';
        };
      });
}
