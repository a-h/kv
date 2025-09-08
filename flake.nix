{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    gitignore = {
      url = "github:hercules-ci/gitignore.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    version = {
      url = "github:a-h/version";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    xc = {
      url = "github:joerdav/xc";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, gitignore, version, xc }:
    let
      allSystems = [
        "x86_64-linux" # 64-bit Intel/AMD Linux
        "aarch64-linux" # 64-bit ARM Linux
        "x86_64-darwin" # 64-bit Intel macOS
        "aarch64-darwin" # 64-bit ARM macOS
      ];

      forAllSystems = f: nixpkgs.lib.genAttrs allSystems (system: f {
        system = system;
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            (final: prev: {
              rqlite = prev.rqlite.overrideAttrs (oldAttrs: {
                version = "8.36.4";
                src = prev.fetchFromGitHub {
                  owner = "rqlite";
                  repo = "rqlite";
                  rev = "v8.36.4";
                  hash = "sha256-kscSjT83wLiTgY6fOuvP2KnIXDTwmgHIAvNRq4IMawg=";
                };
                vendorHash = "sha256-lMDE8M8O6HIJE585OaI1islvffVHncr5CwLoVVSCOh4=";
              });
              version = version.packages.${system}.default;
              xc = xc.packages.${system}.xc;
            })
          ];
        };
      });

      # Build app.
      app = { name, pkgs, system }: pkgs.buildGoModule {
        name = name;
        pname = name;
        src = gitignore.lib.gitignoreSource ./.;
        go = pkgs.go;
        subPackages = [ "cmd/kv" ];
        vendorHash = "sha256-fMAJSfdpSlZngvZpVgj4odlyfRXmkibG09G2ECwaUa8=";
        CGO_ENABLED = 0;
        flags = [
          "-trimpath"
        ];
        ldflags = [
          "-s"
          "-w"
          "-extldflags -static"
        ];
      };

      # Build Docker containers.
      dockerUser = pkgs: pkgs.runCommand "user" { } ''
        mkdir -p $out/etc
        echo "user:x:1000:1000:user:/home/user:/bin/false" > $out/etc/passwd
        echo "user:x:1000:" > $out/etc/group
        echo "user:!:1::::::" > $out/etc/shadow
      '';
      dockerImage = { name, pkgs, system }:
        let
          versionNumber = builtins.readFile ./.version;
        in
        pkgs.dockerTools.buildImage {
          name = name;
          tag = versionNumber;

          copyToRoot = [
            # Remove coreutils and bash for a smaller container.
            pkgs.coreutils
            pkgs.bash
            (dockerUser pkgs)
            (app { inherit name pkgs system; })
          ];
          config = {
            Cmd = [ "kv" ];
            User = "user:user";
          };
        };

      # Development tools used.
      devTools = { system, pkgs }: [
        pkgs.sqlite
        pkgs.crane
        pkgs.gh
        pkgs.git
        # To visualize graph output.
        pkgs.graphviz
        # Go tooling.
        pkgs.go
        pkgs.gopls
        pkgs.golangci-lint
        pkgs.gosec
        # Task runner.
        pkgs.xc
        pkgs.version
        # Database tools.
        pkgs.rqlite # Distributed sqlite.
        pkgs.pgcli # PostgreSQL CLI. Use `pgcli -h localhost -u postgres` and password `secret` as per the README.md file.
        pkgs.natscli # NATS CLI for testing and monitoring NATS server.
      ];

      name = "kv";
    in
    {
      # `nix build .#rqlite-docker-image` builds the Docker container.
      packages = forAllSystems ({ system, pkgs }: {
        default = app { name = name; pkgs = pkgs; system = system; };
        docker-image = dockerImage { pkgs = pkgs; system = system; name = name; };
      });
      # `nix develop` provides a shell containing required tools.
      devShells = forAllSystems ({ system, pkgs }: {
        default = pkgs.mkShell {
          buildInputs = (devTools { system = system; pkgs = pkgs; });
        };
      });
    };
}
