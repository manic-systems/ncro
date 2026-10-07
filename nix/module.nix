{
  config,
  pkgs,
  lib,
  ...
}: let
  inherit (builtins) attrValues attrNames;
  inherit (lib.modules) mkIf;
  inherit (lib.options) mkOption mkEnableOption;
  inherit (lib.types) attrsOf bool nullOr path port submodule;
  inherit (lib.lists) optionals filter elemAt map flatten unique;
  inherit (lib.attrsets) optionalAttrs mapAttrsToList mapAttrs' nameValuePair filterAttrs;
  inherit (lib.strings) hasPrefix match toInt;
  inherit (lib.trivial) defaultTo;

  common = import ./common.nix {inherit lib pkgs;};
  inherit (common) tomlType defaultServerPort defaultMeshPort upstreamPublicKeysFor effectiveSettingsFor;
  generateConfig = common.generateConfig cfg.package;

  cfg = config.services.ncro;
  configFile = generateConfig "ncro.toml" effectiveSettings;

  # Normalize a ncro listen address (`:port` shorthand) to the
  # `host:port` format expected by systemd's ListenStream.
  normalizeAddr = addr:
    if lib.hasPrefix ":" addr
    then "0.0.0.0${addr}"
    else addr;

  addrParts = addr: let
    matches = match "(.*):([0-9]+)" addr;
  in
    if matches == null
    then throw "ncro address ${addr} must end in a numeric port"
    else {
      host = elemAt matches 0;
      port = toInt (elemAt matches 1);
    };

  portOfAddr = addr: (addrParts addr).port;

  # systemd's ListenStream takes neither a hostname nor the `:port` shorthand,
  # so spell out the sockets ncro would otherwise bind itself.
  listenStreamsFor = addr: let
    port = toString (portOfAddr addr);
  in
    if hasPrefix "localhost:" addr
    then ["127.0.0.1:${port}" "[::1]:${port}"]
    else if hasPrefix ":" addr
    then [port]
    else [addr];

  isLoopbackAddr = addr: let
    inherit ((addrParts addr)) host;
  in
    host == "localhost" || host == "::1" || host == "[::1]" || lib.hasPrefix "127." host;

  instanceConfigFile = name: instance:
    generateConfig "ncro-${name}.toml" (effectiveInstanceSettings instance);

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

      # NAR proxying is not concurrency-gated: every in-flight NAR holds an
      # inbound and an upstream socket for the duration of the transfer, so a
      # single busy nix client can exceed systemd's 1024 soft limit.
      LimitNOFILE = 65536;

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

  effectiveSettings = effectiveSettingsFor cfg.port cfg.meshPort cfg.settings;
  effectiveInstanceSettings = instance:
    effectiveSettingsFor
    (defaultTo defaultServerPort instance.port)
    (defaultTo defaultMeshPort instance.meshPort)
    instance.settings;

  instanceSocket = _: instance: {
    wantedBy = ["sockets.target"];
    socketConfig.ListenStream = listenStreamsFor (effectiveInstanceSettings instance).server.listen;
  };

  activeListeners =
    if cfg.instances == {}
    then [
      {
        settings = effectiveSettings;
        openFirewall = cfg.openFirewall;
      }
    ]
    else
      mapAttrsToList (_: instance: {
        settings = effectiveInstanceSettings instance;
        openFirewall = cfg.openFirewall || instance.openFirewall;
      })
      cfg.instances;

  firewallListeners = filter (listener: listener.openFirewall) activeListeners;

  firewallTCPPorts = unique (
    map (listener: portOfAddr listener.settings.server.listen)
    (filter (listener: !isLoopbackAddr listener.settings.server.listen) firewallListeners)
  );

  firewallUDPPorts = lib.unique (
    map (listener: portOfAddr listener.settings.mesh.bind_addr)
    (filter (
        listener:
          (listener.settings.mesh.enabled or false)
          && !isLoopbackAddr listener.settings.mesh.bind_addr
      )
      firewallListeners)
  );

  instanceListenAddresses = flatten (
    lib.mapAttrsToList
    (_: instance: listenStreamsFor (effectiveInstanceSettings instance).server.listen)
    cfg.instances
  );

  instanceMeshAddresses =
    map (settings: normalizeAddr settings.mesh.bind_addr)
    (filter (settings: settings.mesh.enabled or false)
      (mapAttrsToList (_: effectiveInstanceSettings) cfg.instances));

  upstreamPublicKeys = unique (
    upstreamPublicKeysFor effectiveSettings
    ++ flatten (builtins.map
      (instance: upstreamPublicKeysFor (effectiveInstanceSettings instance))
      (attrValues cfg.instances))
  );

  instanceEtc =
    mapAttrs' (
      name: instance:
        nameValuePair "ncro/${name}.toml" {source = instanceConfigFile name instance;}
    )
    cfg.instances
    // mapAttrs' (
      name: instance:
        nameValuePair "ncro/${name}.netrc" {
          source =
            if instance.netrcFile != null
            then instance.netrcFile
            else pkgs.writeText "empty-netrc" "";
        }
    )
    cfg.instances;
in {
  options.services.ncro =
    common.options
    // {
      enable = mkEnableOption "ncro, the Nix cache route optimizer";

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
          if set, otherwise from {option}`services.ncro.port`.
        '';
      };

      openFirewall = mkOption {
        type = bool;
        default = false;
        description = ''
          Open the firewall for whichever ports the listeners actually landed
          on, across every named instance as well. Mesh gossip is included only
          when mesh is enabled, and a listener bound to loopback is skipped
          since nothing outside the machine can reach one anyway.
        '';
      };

      instances = mkOption {
        type = attrsOf (submodule ({...}: {
          options = {
            port = mkOption {
              type = nullOr port;
              default = null;
              example = 8081;
              description = ''
                TCP port for this instance, which is enough on its own to give
                the instance a listen address. Setting `settings.server.listen`
                overrides it.
              '';
            };

            meshPort = mkOption {
              type = nullOr port;
              default = null;
              example = 7947;
              description = ''
                UDP port for this instance's mesh gossip. Setting
                `settings.mesh.bind_addr` overrides it. Instances that enable
                mesh each need a port of their own.
              '';
            };

            openFirewall = mkOption {
              type = bool;
              default = false;
              description = ''
                Open the firewall for this instance alone, on the same terms as
                the toplevel {option}`services.ncro.openFirewall`, which covers
                every instance at once.
              '';
            };

            settings = mkOption {
              type = tomlType;
              default = {};
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

          Every instance must set `port` or `settings.server.listen` to a unique
          address, and mesh-enabled instances must likewise use unique
          `meshPort` or `settings.mesh.bind_addr` values.
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
          assertion = instance.port != null || (instance.settings.server.listen or "") != "";
          message = "services.ncro.instances.${name} needs either port or settings.server.listen";
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
          message = "services.ncro.instances must use unique listen addresses (port or settings.server.listen)";
        }
        {
          assertion = builtins.length instanceMeshAddresses == builtins.length (lib.unique instanceMeshAddresses);
          message = "services.ncro.instances with mesh enabled must use unique mesh addresses (meshPort or settings.mesh.bind_addr)";
        }
      ];

    networking.firewall = mkIf (firewallTCPPorts != [] || firewallUDPPorts != []) {
      allowedTCPPorts = firewallTCPPorts;
      allowedUDPPorts = firewallUDPPorts;
    };

    environment.etc = instanceEtc;
    systemd = {
      sockets =
        {
          ncro = mkIf (cfg.instances == {} && cfg.socketActivation) {
            wantedBy = ["sockets.target"];
            socketConfig.ListenStream = listenStreamsFor effectiveSettings.server.listen;
          };
        }
        // lib.mapAttrs' (
          name: instance:
            lib.nameValuePair "ncro@${name}" (instanceSocket name instance)
        ) (lib.filterAttrs (_: instance: instance.socketActivation) cfg.instances);

      services = {
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

              # NAR proxying is not concurrency-gated: every in-flight NAR holds an
              # inbound and an upstream socket for the duration of the transfer, so a
              # single busy nix client can exceed systemd's 1024 soft limit.
              LimitNOFILE = 65536;

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

      targets.multi-user.wants =
        map
        (name: "ncro@${name}.service")
        (attrNames (filterAttrs (_: instance: !instance.socketActivation) cfg.instances));
    };
  };
}
