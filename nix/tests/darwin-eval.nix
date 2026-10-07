# Evaluates the nix-darwin module against a nix-darwin branch. Needs --impure:
#   nix eval --impure --raw --expr 'import ./nix/tests/darwin-eval.nix {darwinRef = "master";}'
{darwinRef ? "master"}: let
  ncro = builtins.getFlake (toString ../..);
  darwin = builtins.getFlake "github:nix-darwin/nix-darwin/${darwinRef}";
  system = darwin.lib.darwinSystem {
    modules = [
      ncro.darwinModules.default
      {
        nixpkgs.hostPlatform = "aarch64-darwin";
        system.stateVersion = 6;
        services.ncro = {
          enable = true;
          netrcFile = "/var/lib/ncro/netrc";
          settings.upstreams = [
            {
              url = "https://cache.nixos.org";
              public_key = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=";
            }
          ];
        };
      }
    ];
  };
in
  system.system.drvPath
