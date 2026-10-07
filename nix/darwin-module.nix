{
  config,
  pkgs,
  lib,
  ...
}: let
  inherit (lib.modules) mkIf mkDefault;
  inherit (lib.options) mkEnableOption;
  inherit (lib.attrsets) optionalAttrs recursiveUpdate;

  common = import ./common.nix {inherit lib pkgs;};
  cfg = config.services.ncro;
  stateDir = "/var/lib/ncro";
  logFile = "${stateDir}/ncro.log";
  settings = recursiveUpdate {cache.db_path = "${stateDir}/routes.db";} (
    common.effectiveSettingsFor cfg.port cfg.meshPort cfg.settings
  );
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
      (lib.mkAfter (common.upstreamPublicKeysFor settings));

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

    # Written directly rather than through system.newsyslog, which is not
    # available in nix-darwin 26.05. Size is in KB.
    environment.etc."newsyslog.d/ncro.conf".text = ''
      ${logFile} _ncro:_ncro 640 5 10240 * Z
    '';

    # createHome only runs when the user is first created and does not create
    # /private/var/lib, so ensure the state directory on every activation.
    # Numeric ids are used because this runs before the users step.
    system.activationScripts.extraActivation.text = lib.mkAfter ''
      mkdir -p ${stateDir}
      chown ${toString config.users.users._ncro.uid}:${toString config.users.groups._ncro.gid} ${stateDir}
      chmod 0750 ${stateDir}
    '';

    launchd.daemons.ncro = {
      command = "${lib.getExe' cfg.package "ncro"} --config ${configFile}";
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
