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

        toolchainFile = (pkgs.lib.importTOML ./rust-toolchain.toml).toolchain;

        # Dev shell toolchain: the release and components from rust-toolchain.toml,
        # plus editor and coverage components. cargo-llvm-cov needs llvm-tools, and
        # without rustup the toolchain must provide it. The minimal profile skips
        # the offline documentation.
        devToolchain = pkgs.rust-bin.fromRustupToolchain (toolchainFile // {
          profile = "minimal";
          components = (toolchainFile.components or [ ])
            ++ [ "llvm-tools" "rust-src" "rust-analyzer" ];
        });

        # The package needs only rustc, cargo, and the standard library.
        buildToolchain = pkgs.rust-bin.fromRustupToolchain {
          inherit (toolchainFile) channel;
          profile = "minimal";
        };

        rustPlatform = pkgs.makeRustPlatform {
          cargo = buildToolchain;
          rustc = buildToolchain;
        };

        # Native build tools and libraries, shared by the package and dev shell:
        # protoc for prost-build (gcp-bigquery-client), and pkg-config with
        # OpenSSL for openssl-sys (native-tls) on Linux. aws-lc-sys uses its cc
        # builder on these systems, so CMake is not needed.
        nativeBuildDeps = with pkgs; [
          pkg-config
          protobuf
        ];
        buildDeps = with pkgs; [ openssl ];

        # Keep the required runner without updating unrelated nixpkgs packages.
        cargoNextest = pkgs.cargo-nextest.overrideAttrs (finalAttrs: _: {
          version = (pkgs.lib.importTOML ./.config/nextest.toml).nextest-version.required;
          src = pkgs.fetchFromGitHub {
            owner = "nextest-rs";
            repo = "nextest";
            tag = "cargo-nextest-${finalAttrs.version}";
            hash = "sha256-JcDSd3VswTuAhmHibwFJjJMSEbSReQ/SONrEcyEGYD4=";
          };
          cargoDeps = pkgs.rustPlatform.fetchCargoVendor {
            inherit (finalAttrs) pname version src;
            hash = "sha256-MW2cM2XjfAquhdeDNwvizk6owl1sVwK5mfCiSpjJN6E=";
          };
        });

        devTools = with pkgs; [
          actionlint
          cargo-deny
          cargo-llvm-cov
          cargoNextest
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
