{
  config,
  pkgs,
  lib,
  ...
}: let
  inherit (lib.modules) mkIf mkDefault mkAfter;
  inherit (lib.options) mkEnableOption;
  inherit (lib.attrsets) optionalAttrs;
  inherit (lib.meta) getExe';

  common = import ./common.nix {inherit lib pkgs;};
  cfg = config.services.ncro;
  stateDir = "/var/lib/ncro";
  logFile = "${stateDir}/ncro.log";
  settings = common.effectiveSettingsFor cfg.port cfg.meshPort cfg.settings;
  configFile = common.generateConfig cfg.package "ncro.toml" settings;
in {
  options.services.ncro =
    common.options
    // {
      enable = mkEnableOption "ncro, the Nix cache route optimizer";
    };

  config = mkIf cfg.enable {
    nix.settings.trusted-public-keys =
      mkIf (cfg.addUpstreamPublicKeys && config.nix.enable)
      (mkAfter (common.upstreamPublicKeysFor settings));

    users = {
      users._ncro = {
        uid = mkDefault 536;
        gid = config.users.groups._ncro.gid;
        home = stateDir;
        shell = "/usr/bin/false";
        description = "System user for ncro";
      };
      groups._ncro = {
        gid = mkDefault 536;
        description = "System group for ncro";
      };
      knownUsers = ["_ncro"];
      knownGroups = ["_ncro"];
    };

    system.activationScripts.extraActivation.text =
      mkAfter
      # sh
      ''
        mkdir -p ${stateDir}
        chown ${toString config.users.users._ncro.uid}:${toString config.users.groups._ncro.gid} ${stateDir}
        chmod 0750 ${stateDir}
      '';

    launchd.daemons.ncro = {
      command = "${getExe' cfg.package "ncro"} --config ${configFile}";
      environment = optionalAttrs (cfg.netrcFile != null) {NETRC = toString cfg.netrcFile;};
      serviceConfig = {
        UserName = "_ncro";
        GroupName = "_ncro";
        KeepAlive = true;
        RunAtLoad = true;
        ThrottleInterval = 10;
        SoftResourceLimits.NumberOfFiles = 65536;
        StandardErrorPath = logFile;
        StandardOutPath = logFile;
      };
    };
  };
}
