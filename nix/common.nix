{
  lib,
  pkgs,
}: let
  inherit (builtins) isAttrs;
  inherit (lib.options) mkOption literalExpression;
  inherit (lib.types) bool package nullOr path port;
  inherit (lib.lists) optional filter flatten unique;
  inherit (lib.attrsets) recursiveUpdate;

  tomlFormat = pkgs.formats.toml {};
  tomlType = tomlFormat.type;
  defaultServerPort = 8080;
  defaultMeshPort = 7946;
  checkable = pkgs.stdenv.buildPlatform.canExecute pkgs.stdenv.hostPlatform;

  publicKeysFor = upstream:
    optional ((upstream.public_key or "") != "") upstream.public_key
    ++ (upstream.public_keys or []);

  listenAddrFor = fallbackPort: settings:
    if (settings.server.listen or "") != ""
    then settings.server.listen
    else "localhost:${toString fallbackPort}";

  meshAddrFor = fallbackPort: settings:
    if (settings.mesh.bind_addr or "") != ""
    then settings.mesh.bind_addr
    else "0.0.0.0:${toString fallbackPort}";
in {
  inherit tomlType defaultServerPort defaultMeshPort;

  options = {
    addUpstreamPublicKeys = mkOption {
      type = bool;
      default = true;
      description = ''
        Append non-empty upstream public_key and public_keys values from {option}`services.ncro.settings`
        to {option}`nix.settings.trusted-public-keys`.

        This keeps Nix client signature validation aligned with the upstream
        caches that ncro is allowed to route to. Disable this if you manage Nix
        trusted public keys separately.
      '';
    };

    port = mkOption {
      type = port;
      default = defaultServerPort;
      description = ''
        TCP port for the ncro HTTP listener, bound on `localhost` (both
        127.0.0.1 and ::1) unless overridden. Reach for
        {option}`services.ncro.settings.server.listen` when you need to pin the
        bind address too, since it overrides this option.
      '';
    };

    meshPort = mkOption {
      type = port;
      default = defaultMeshPort;
      description = ''
        UDP port for mesh gossip. Reach for
        {option}`services.ncro.settings.mesh.bind_addr` when you need to pin
        the bind address too, since it overrides this option.
      '';
    };

    package = mkOption {
      type = package;
      default = pkgs.callPackage ./package.nix {};
      defaultText = literalExpression "pkgs.callPackage ./package.nix { }";
      description = "The ncro package to use.";
    };

    netrcFile = mkOption {
      type = nullOr path;
      default = null;
      example = "/etc/nix/netrc";
      description = ''
        The path to netrc file for upstream authentication.
        If null, ncro will not use netrc for upstream authentication.
      '';
    };

    settings = mkOption {
      type = tomlType;
      default = {};
      description = ''
        ncro configuration as an attribute set.

        Keys are the TOML field names, and anything left out keeps ncro's
        own default. The generated file is checked with `ncro --check` at
        build time, so a misspelt key or an out-of-range value fails the
        build rather than the running service.
      '';
      example = {
        logging.level = "info";
        server = {
          listen = ":8080";
          cache_priority = 20;
        };

        upstreams = [
          {
            url = "https://cache.nixos.org";
            priority = 10;
          }
          {
            url = "https://nix-community.cachix.org";
            priority = 20;
          }
        ];

        cache = {
          ttl = "2h";
          negative_ttl = "15m";
        };
      };
    };
  };

  generateConfig = package: name: settings: let
    file = tomlFormat.generate name settings;
  in
    if checkable
    then
      pkgs.runCommand name {} ''
        ${lib.getExe' package "ncro"} --config ${file} --check
        cp ${file} $out
      ''
    else file;

  upstreamPublicKeysFor = settings: let
    fallbackPublicKeys =
      if settings.fallback_cache.enabled or false
      then
        publicKeysFor (
          settings.fallback_cache
          // {
            public_key =
              settings.fallback_cache.public_key
                or "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=";
          }
        )
      else [];
  in
    lib.pipe ((settings.upstreams or []) ++ [fallbackPublicKeys]) [
      (map (upstream:
        if isAttrs upstream
        then publicKeysFor upstream
        else upstream))
      flatten
      (filter (key: key != ""))
      unique
    ];

  effectiveSettingsFor = serverPort: meshPort: settings:
    recursiveUpdate settings {
      server.listen = listenAddrFor serverPort settings;
      mesh.bind_addr = meshAddrFor meshPort settings;
    };
}
