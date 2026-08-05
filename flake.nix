{
  inputs.nixpkgs.url = "https://channels.nixos.org/nixos-unstable/nixexprs.tar.xz";

  outputs = {
    self,
    nixpkgs,
    ...
  }: let
    systems = ["x86_64-linux" "aarch64-linux"];
    forEachSystem = nixpkgs.lib.genAttrs systems;
    pkgsForEach = system: nixpkgs.legacyPackages.${system};
  in {
    nixosModules = {
      ncro = ./nix/module.nix;
      default = self.nixosModules.ncro;
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
    in {
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

    hydraJobs = self.packages;
  };
}
