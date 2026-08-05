{
  config,
  pkgs,
  lib,
  ...
}: let
  inherit (lib.modules) mkIf;
  inherit (lib.options) mkOption mkEnableOption literalExpression;
  inherit (lib.types) attrsOf bool package nullOr path submodule;
  inherit (lib.lists) optional optionals;
  inherit (lib.attrsets) optionalAttrs;

  tomlFormat = pkgs.formats.toml {};
  tomlType = tomlFormat.type;

  cfg = config.services.ncro;
  configFile = tomlFormat.generate "ncro.toml" cfg.settings;

  # Normalize a ncro listen address (`:port` shorthand) to the
  # `host:port` format expected by systemd's ListenStream.
  toListenStream = addr: let
    raw =
      if addr == ""
      then ":8080"
      else addr;
  in
    if lib.hasPrefix ":" raw
    then "0.0.0.0${raw}"
    else raw;

  publicKeysFor = upstream:
    optional ((upstream.public_key or "") != "") upstream.public_key
    ++ (upstream.public_keys or []);

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
      (builtins.map (upstream:
        if builtins.isAttrs upstream
        then publicKeysFor upstream
        else upstream))
      lib.flatten
      (builtins.filter (key: key != ""))
      lib.unique
    ];

  instanceConfigFile = name: instance:
    tomlFormat.generate "ncro-${name}.toml" instance.settings;

  instanceWrapper = pkgs.writeShellScript "ncro-instance" ''
    if [ -s "$CREDENTIALS_DIRECTORY/netrc" ]; then
      export NETRC="$CREDENTIALS_DIRECTORY/netrc"
    fi
    exec ${lib.getExe' cfg.package "ncro"} --config "/etc/ncro/$1.toml"
  '';

  instanceService = {
    description = "Nix Cache Route Optimizer instance %i";
    after = ["network.target"];
    environment.NCRO_DB_PATH = "/var/lib/ncro-%i/routes.db";
    serviceConfig = {
      ExecStart = "${instanceWrapper} %i";
      Type = "notify";
      DynamicUser = true;
      StateDirectory = "ncro-%i";
      LoadCredential = ["netrc:/etc/ncro/%i.netrc"];
      Restart = "on-failure";
      RestartSec = "5s";

      NoNewPrivileges = true;
      PrivateTmp = true;
      PrivateDevices = true;
      ProtectSystem = "strict";
      ProtectHome = true;
      ProtectProc = "invisible";
      ProtectHostname = true;
      ProtectClock = true;
      ProtectControlGroups = true;
      ProtectKernelLogs = true;
      ProtectKernelTunables = true;
      RestrictRealtime = true;
      CapabilityBoundingSet = "";
      RestrictAddressFamilies = ["AF_INET" "AF_INET6" "AF_NETLINK" "AF_UNIX"];
      RestrictNamespaces = true;
      LockPersonality = true;
      MemoryDenyWriteExecute = true;
      SystemCallFilter = ["@system-service"];
      SystemCallArchitectures = "native";
    };
  };

  instanceSocket = name: instance: {
    wantedBy = ["sockets.target"];
    socketConfig.ListenStream = toListenStream instance.settings.server.listen;
  };

  instanceListenAddresses =
    builtins.map
    (instance: instance.settings.server.listen)
    (builtins.attrValues cfg.instances);

  upstreamPublicKeys = lib.unique (
    upstreamPublicKeysFor cfg.settings
    ++ lib.flatten (builtins.map
      (instance: upstreamPublicKeysFor instance.settings)
      (builtins.attrValues cfg.instances))
  );

  instanceEtc =
    lib.mapAttrs' (
      name: instance:
        lib.nameValuePair "ncro/${name}.toml" {source = instanceConfigFile name instance;}
    )
    cfg.instances
    // lib.mapAttrs' (
      name: instance:
        lib.nameValuePair "ncro/${name}.netrc" {
          source =
            if instance.netrcFile != null
            then instance.netrcFile
            else pkgs.writeText "empty-netrc" "";
        }
    )
    cfg.instances;
in {
  options.services.ncro = {
    enable = mkEnableOption "ncro, the Nix cache route optimizer";

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

    socketActivation = mkOption {
      type = bool;
      default = false;
      description = ''
        Enable systemd socket activation for ncro. When enabled, systemd
        creates and holds the listening TCP socket, starting ncro on the first
        incoming connection.

        ncro signals readiness via {manpage}`sd_notify(3)`, so downstream units
        that declare `After = ncro.service` will not start until ncro is actually
        accepting connections.

        A {manpage}`systemd.socket(5)` unit `ncro.socket` is created automatically.
        The listen address is taken from {option}`services.ncro.settings.server.listen`
        if set, defaulting to `:8080`.
      '';
    };

    package = mkOption {
      type = package;
      default = pkgs.callPackage ./package.nix {};
      defaultText = literalExpression "inputs.ncro.packages.$${system}.ncro";
      description = "The ncro package to use.";
      example = literalExpression "inputs.ncro.packages.$${system}.ncro";
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

        Keys and structure match the TOML config file format; all defaults are
        handled by the ncro binary.
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

    instances = mkOption {
      type = attrsOf (submodule ({...}: {
        options = {
          settings = mkOption {
            type = tomlType;
            description = "Configuration for this ncro instance.";
          };

          socketActivation = mkOption {
            type = bool;
            default = false;
            description = "Enable systemd socket activation for this instance.";
          };

          netrcFile = mkOption {
            type = nullOr path;
            default = null;
            description = "Netrc file for upstream authentication in this instance.";
          };
        };
      }));
      default = {};
      description = ''
        Named ncro instances. Each instance starts the `ncro@.service` template
        as `ncro@<name>.service`, with its own
        state directory, SQLite route cache, and optionally socket unit.

        Every instance must set `settings.server.listen` to a unique address.
      '';
      example.project = {
        settings = {
          server.listen = "127.0.0.1:8081";
          upstreams = [{url = "https://cache.nixos.org";}];
        };
      };
    };
  };

  config = mkIf cfg.enable {
    nix.settings.trusted-public-keys =
      mkIf cfg.addUpstreamPublicKeys (lib.mkAfter upstreamPublicKeys);

    assertions =
      (lib.mapAttrsToList (name: instance: {
          assertion = instance.settings ? server && instance.settings.server ? listen;
          message = "services.ncro.instances.${name}.settings.server.listen must be set";
        })
        cfg.instances)
      ++ (lib.mapAttrsToList (name: _: {
          assertion = builtins.match "[a-zA-Z0-9_.-]+" name != null;
          message = "services.ncro.instances names may contain only letters, numbers, '.', '_', and '-'";
        })
        cfg.instances)
      ++ [
        {
          assertion = builtins.length instanceListenAddresses == builtins.length (lib.unique instanceListenAddresses);
          message = "services.ncro.instances must use unique settings.server.listen addresses";
        }
      ];

    systemd.sockets =
      {
        ncro = mkIf (cfg.instances == {} && cfg.socketActivation) {
          wantedBy = ["sockets.target"];
          socketConfig.ListenStream =
            toListenStream (cfg.settings.server.listen or "");
        };
      }
      // lib.mapAttrs' (
        name: instance:
          lib.nameValuePair "ncro@${name}" (instanceSocket name instance)
      ) (lib.filterAttrs (_: instance: instance.socketActivation) cfg.instances);

    systemd.services = {
      ncro = mkIf (cfg.instances == {}) {
        description = "Nix Cache Route Optimizer";
        wantedBy = ["multi-user.target"];
        after =
          if cfg.socketActivation
          then ["ncro.socket"]
          else ["network.target"];
        requires = optionals cfg.socketActivation ["ncro.socket"];
        environment = optionalAttrs (cfg.netrcFile != null) {
          NETRC = "%d/netrc";
        };
        serviceConfig =
          {
            ExecStart = "${lib.getExe' cfg.package "ncro"} --config ${configFile}";
            DynamicUser = true;
            StateDirectory = "ncro";
            Restart = "on-failure";
            RestartSec = "5s";

            # Hardening
            NoNewPrivileges = true;
            PrivateTmp = true;
            PrivateDevices = true;
            ProtectSystem = "strict";
            ProtectHome = true;
            ProtectProc = "invisible";
            ProtectHostname = true;
            ProtectClock = true;
            ProtectControlGroups = true;
            ProtectKernelLogs = true;
            ProtectKernelTunables = true;
            RestrictRealtime = true;
            CapabilityBoundingSet = "";
            RestrictAddressFamilies =
              [
                "AF_INET"
                "AF_INET6"
                "AF_NETLINK" # required by mdns-sd and system resolver
              ]
              # sd_notify uses a Unix datagram socket to signal readiness.
              ++ optionals cfg.socketActivation ["AF_UNIX"];
            RestrictNamespaces = true;
            LockPersonality = true;
            MemoryDenyWriteExecute = true;
            SystemCallFilter = ["@system-service"];
            SystemCallArchitectures = "native";
          }
          // optionalAttrs cfg.socketActivation {Type = "notify";}
          // optionalAttrs (cfg.netrcFile != null) {LoadCredential = ["netrc:${cfg.netrcFile}"];};
      };
      "ncro@" = mkIf (cfg.instances != {}) instanceService;
    };

    environment.etc = instanceEtc;

    systemd.targets.multi-user.wants =
      builtins.map
      (name: "ncro@${name}.service")
      (builtins.attrNames (lib.filterAttrs (_: instance: !instance.socketActivation) cfg.instances));
  };
}
