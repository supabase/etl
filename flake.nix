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

        # Native build tools and libraries, shared by the package and dev shell.
        nativeBuildDeps = with pkgs; [
          cmake
          pkg-config
          protobuf
          python3
        ];
        buildDeps = with pkgs; [ openssl ];

        devTools = with pkgs; [
          actionlint
          cargo-deny
          cargo-llvm-cov
          cargo-nextest
          cargo-sort
          postgresql
          # actionlint checks workflow `run:` scripts with shellcheck, as in CI.
          shellcheck
          # `cargo x init` runs SQLx migrations.
          sqlx-cli
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
            # Fetch git dependencies at their locked revisions, so changing a
            # revision in Cargo.toml needs no hand-updated hash here.
            allowBuiltinFetchGit = true;
          };

          buildAndTestSubdir = "crates/etl-replicator";

          # Match the release image's compiler flags (crates/etl-replicator/Dockerfile).
          # Unlike the image, this package bundles no DuckDB extensions, so DuckLake
          # downloads them at runtime unless ETL_DUCKDB_EXTENSION_ROOT points at a copy.
          env.RUSTFLAGS = "--cfg tokio_unstable -C panic=abort";

          nativeBuildInputs = nativeBuildDeps;
          buildInputs = buildDeps;

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

          packages = [ devToolchain ] ++ nativeBuildDeps ++ buildDeps ++ devTools;

          shellHook = ''
            export RUST_SRC_PATH=${devToolchain}/lib/rustlib/src/rust/library
          '';
        };
      });
}
