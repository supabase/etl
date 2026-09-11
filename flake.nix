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
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        rustToolchain =
          (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
            extensions = [ "rust-src" "rust-analyzer" ];
          };

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

        # Nix enables `_FORTIFY_SOURCE` by default; jemalloc's configure runs
        # detection tests at `-O0`, which turns that into a hard error and
        # mis-detects `strerror_r` (tikv-jemalloc-sys).
        jemallocHardening = {
          hardeningDisable = [ "fortify" ];
        };

        devTools = with pkgs; [
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

        etl-replicator = rustPlatform.buildRustPackage (jemallocHardening // {
          pname = "etl-replicator";
          version = "0.1.0";
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
            description = "Standalone binary for Supabase ETL Postgres replication pipelines.";
            mainProgram = "etl-replicator";
          };
        });

      in {
        packages = {
          default = etl-replicator;
          inherit etl-replicator;
        };

        apps.default = {
          type = "app";
          program = "${etl-replicator}/bin/etl-replicator";
        };

        devShells.default = pkgs.mkShell (jemallocHardening // {
          packages = [ rustToolchain ] ++ buildDeps ++ devTools;

          shellHook = ''
            export RUST_SRC_PATH=${rustToolchain}/lib/rustlib/src/rust/library
          '';
        });
      });
}
