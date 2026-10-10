{
  lib,
  path,
  writeText,
  self,
  nix-darwin ? builtins.fetchTarball {
    url = "https://github.com/nix-darwin/nix-darwin/archive/4cff07de74b50e64bdd68cd4e722ab5b6b35ee48.tar.gz";
    sha256 = "sha256-oQFip+v0luP8NIxJzmiW4Wu8bILsbFWom5l0zonl8hQ=";
  },
}: let
  inherit (builtins) unsafeDiscardStringContext;

  darwin = import "${nix-darwin}/eval-config.nix" {
    inherit lib;
    modules = [
      self.darwinModules.default
      {
        nixpkgs.source = path;
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
  # Evaluates the darwin system without building it.
  writeText "ncro-darwin-module" (unsafeDiscardStringContext darwin.system.drvPath)
