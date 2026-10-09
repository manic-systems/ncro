{
  inputs.nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";

  outputs = {
    self,
    nixpkgs,
    ...
  }: let
    inherit (nixpkgs.lib) genAttrs optionalAttrs;
    inherit (nixpkgs.lib.systems) doubles;
    forEachSystem = genAttrs (doubles.linux ++ doubles.darwin);
    pkgsForEach = system: nixpkgs.legacyPackages.${system} or (import nixpkgs {inherit system;});
  in {
    nixosModules = {
      ncro = {
        pkgs,
        lib,
        ...
      }: {
        imports = [./nix/module.nix];
        services.ncro.package = lib.mkDefault (pkgs.callPackage ./nix/package.nix {});
      };

      default = self.nixosModules.ncro;
    };

    darwinModules = {
      ncro = ./nix/darwin-module.nix;
      default = self.darwinModules.ncro;
    };

    packages = forEachSystem (system: let
      pkgs = pkgsForEach system;
    in {
      ncro = pkgs.callPackage ./nix/package.nix {};
      default = self.packages.${system}.ncro;
    });

    devShells = forEachSystem (system: let
      pkgs = pkgsForEach system;
    in {
      default = pkgs.callPackage ./nix/shell.nix {};
    });

    checks = forEachSystem (system: let
      pkgs = pkgsForEach system;
    in
      {
        darwin-module = pkgs.callPackage ./nix/tests/darwin-module.nix {inherit self;};
      }
      // optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
        p2p-discovery = pkgs.callPackage ./nix/tests/p2p.nix {inherit self;};
        e2e = pkgs.callPackage ./nix/tests/e2e.nix {inherit self;};
        s3 = pkgs.callPackage ./nix/tests/s3.nix {inherit self;};
        netrc = pkgs.callPackage ./nix/tests/netrc.nix {inherit self;};
        socket-activation = pkgs.callPackage ./nix/tests/socket-activation.nix {inherit self;};
        multi-instance = pkgs.callPackage ./nix/tests/multi-instance.nix {inherit self;};
        public-keys = pkgs.callPackage ./nix/tests/public-keys.nix {inherit self;};
      });

    # Provides the default formatter for 'nix fmt'.
    formatter = forEachSystem (
      system: let
        pkgs = pkgsForEach system;
      in
        pkgs.writeShellApplication {
          name = "nix3-fmt-wrapper";
          runtimeInputs = [
            pkgs.alejandra
            pkgs.fd
          ];

          text = ''
            # Format Nix files with nixfmt
            fd "$@" -t f -e nix -x alejandra -q '{}'
          '';
        }
    );

    hydraJobs = {inherit (self.packages) x86_64-linux aarch64-linux aarch64-darwin;};
  };
}
