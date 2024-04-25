{
  description = "nativelink";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    nix2container = {
      url = "github:nlewo/nix2container";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
  };

  outputs = inputs @ {
    self,
    flake-parts,
    crane,
    rust-overlay,
    nix2container,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = [
        "x86_64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      imports = [inputs.pre-commit-hooks.flakeModule];
      perSystem = {
        config,
        pkgs,
        system,
        ...
      }: let
        stable-rust-version = "1.77.2";
        nightly-rust-version = "2024-04-03";

        # TODO(aaronmondal): Make musl builds work on Darwin.
        # See: https://github.com/TraceMachina/nativelink/issues/751
        stable-rust =
          if pkgs.stdenv.isDarwin
          then pkgs.rust-bin.stable.${stable-rust-version}
          else pkgs.pkgsMusl.rust-bin.stable.${stable-rust-version};
        nightly-rust =
          if pkgs.stdenv.isDarwin
          then pkgs.rust-bin.nightly.${nightly-rust-version}
          else pkgs.pkgsMusl.rust-bin.nightly.${nightly-rust-version};

        # TODO(aaronmondal): Tools like rustdoc don't work with the `pkgsMusl`
        # package set because of missing libgcc_s. Fix this upstream and use the
        # `stable-rust` toolchain in the devShell as well.
        # See: https://github.com/oxalica/rust-overlay/issues/161
        stable-rust-native = pkgs.rust-bin.stable.${stable-rust-version};

        maybeDarwinDeps = pkgs.lib.optionals pkgs.stdenv.isDarwin [
          pkgs.darwin.apple_sdk.frameworks.Security
          pkgs.libiconv
        ];

        customStdenv = import ./tools/llvmStdenv.nix {inherit pkgs;};

        # TODO(aaronmondal): This doesn't work with rules_rust yet.
        # Tracked in https://github.com/TraceMachina/nativelink/issues/477.
        customClang = pkgs.callPackage ./tools/customClang.nix {
          inherit pkgs;
          stdenv = customStdenv;
        };

        craneLib =
          if pkgs.stdenv.isDarwin
          then crane.lib.${system}
          else
            (crane.mkLib pkgs).overrideToolchain (stable-rust.default.override {
              targets = ["x86_64-unknown-linux-musl"];
            });

        src = pkgs.lib.cleanSourceWith {
          src = craneLib.path ./.;
          filter = path: type:
            (builtins.match "^.+/data/SekienAkashita\\.jpg" path != null)
            || (craneLib.filterCargoSources path type);
        };

        commonArgs =
          {
            inherit src;
            stdenv =
              if pkgs.stdenv.isDarwin
              then customStdenv
              else pkgs.pkgsMusl.stdenv;
            strictDeps = true;
            buildInputs = [pkgs.cacert] ++ maybeDarwinDeps;
            nativeBuildInputs = maybeDarwinDeps;
          }
          // pkgs.lib.optionalAttrs (!pkgs.stdenv.isDarwin) {
            CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";
            CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
          };

        # Additional target for external dependencies to simplify caching.
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        nativelink = craneLib.buildPackage (commonArgs
          // {
            inherit cargoArtifacts;
          });

        nativelink-debug = craneLib.buildPackage (commonArgs
          // {
            inherit cargoArtifacts;
            CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static --cfg tokio_unstable";
            CARGO_PROFILE = "smol";
            cargoExtraArgs = "--features enable_tokio_console";
          });

        hooks = import ./tools/pre-commit-hooks.nix {inherit pkgs nightly-rust;};

        publish-ghcr = import ./tools/publish-ghcr.nix {inherit pkgs;};

        local-image-test = import ./tools/local-image-test.nix {inherit pkgs;};

        generate-toolchains = import ./tools/generate-toolchains.nix {inherit pkgs;};

        native-cli = import ./native-cli/default.nix {inherit pkgs;};

        inherit (nix2container.packages.${system}.nix2container) pullImage;
        inherit (nix2container.packages.${system}.nix2container) buildImage;

        rbe-autogen = import ./local-remote-execution/rbe-autogen.nix {inherit pkgs nativelink buildImage;};
        createWorker = import ./tools/create-worker.nix {inherit pkgs nativelink buildImage self;};
        siso-chromium = buildImage {
          name = "siso-chromium";
          fromImage = pullImage {
            imageName = "gcr.io/chops-public-images-prod/rbe/siso-chromium/linux";
            imageDigest = "sha256:26de99218a1a8b527d4840490bcbf1690ee0b55c84316300b60776e6b3a03fe1";
            sha256 = "sha256-v2wctuZStb6eexcmJdkxKcGHjRk2LuZwyJvi/BerMyw=";
            tlsVerify = true;
            arch = "amd64";
            os = "linux";
          };
        };
      in rec {
        _module.args.pkgs = import self.inputs.nixpkgs {
          inherit system;
          overlays = [(import rust-overlay)];
        };
        apps = {
          default = {
            type = "app";
            program = "${nativelink}/bin/nativelink";
          };
          native = {
            type = "app";
            program = "${native-cli}/bin/native";
          };
        };
        packages = rec {
          inherit publish-ghcr local-image-test nativelink nativelink-debug native-cli;
          default = nativelink;

          lre-cc = import ./local-remote-execution/lre-cc.nix {inherit pkgs buildImage;};
          rbe-autogen-lre-cc = rbe-autogen lre-cc;
          nativelink-worker-lre-cc = createWorker lre-cc;
          lre-java = import ./local-remote-execution/lre-java.nix {inherit pkgs buildImage;};
          rbe-autogen-lre-java = rbe-autogen lre-java;
          nativelink-worker-lre-java = createWorker lre-java;
          nativelink-worker-siso-chromium = createWorker siso-chromium;
          image = buildImage {
            name = "nativelink";
            config = {
              Entrypoint = [(pkgs.lib.getExe' nativelink "nativelink")];
              Labels = {
                "org.opencontainers.image.description" = "An RBE compatible, high-performance cache and remote executor.";
                "org.opencontainers.image.documentation" = "https://github.com/TraceMachina/nativelink";
                "org.opencontainers.image.licenses" = "Apache-2.0";
                "org.opencontainers.image.revision" = "${self.rev or self.dirtyRev or "dirty"}";
                "org.opencontainers.image.source" = "https://github.com/TraceMachina/nativelink";
                "org.opencontainers.image.title" = "NativeLink";
                "org.opencontainers.image.vendor" = "Trace Machina, Inc.";
              };
            };
          };
        };
        checks = {
          # TODO(aaronmondal): Fix the tests.
          # tests = craneLib.cargoNextest (commonArgs
          #   // {
          #   inherit cargoArtifacts;
          #   cargoNextestExtraArgs = "--all";
          #   partitions = 1;
          #   partitionType = "count";
          # });
        };
        pre-commit.settings = {inherit hooks;};
        devShells.default = pkgs.mkShell {
          nativeBuildInputs =
            [
              # Development tooling goes here.
              stable-rust-native.default
              pkgs.pre-commit
              pkgs.bazel_7
              pkgs.awscli2
              pkgs.skopeo
              pkgs.dive
              pkgs.cosign
              pkgs.kubectl
              pkgs.kubernetes-helm
              pkgs.cilium-cli
              pkgs.yarn
              pkgs.vale
              pkgs.trivy
              pkgs.docker-client
              pkgs.kind
              pkgs.tektoncd-cli
              (pkgs.pulumi.withPackages (ps: [ps.pulumi-language-go]))
              pkgs.go
              pkgs.kustomize

              # Additional tools from within our development environment.
              local-image-test
              generate-toolchains
              customClang
              native-cli
            ]
            ++ maybeDarwinDeps;
          shellHook = ''
            # Generate the .pre-commit-config.yaml symlink when entering the
            # development shell.
            ${config.pre-commit.installationScript}

            # The Bazel and Cargo builds in nix require a Clang toolchain.
            # TODO(aaronmondal): The Bazel build currently uses the
            #                    irreproducible host C++ toolchain. Provide
            #                    this toolchain via nix for bitwise identical
            #                    binaries across machines.
            export CC=clang
          '';
        };
      };
    };
}
