{
  pkgs,
  self,
}:
pkgs.testers.runNixOSTest {
  name = "ncro-public-keys";

  nodes.machine = {
    imports = [self.nixosModules.ncro];

    virtualisation.memorySize = 512;
    networking.firewall.enable = false;

    services.ncro = {
      enable = true;
      # The test only needs the module-generated config and nix.conf. Avoid
      # building or starting the real proxy.
      package = pkgs.writeShellScriptBin "ncro" ''
        exec ${pkgs.coreutils}/bin/sleep infinity
      '';
      settings = {
        upstreams = [
          {
            url = "https://pull-through.example";
            public_key = "pull-through-1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
            public_keys = [
              "origin-1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="
              "cache-1:AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI="
            ];
          }
        ];

        fallback_cache = {
          enabled = true;
          public_key = "fallback-1:AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM=";
          public_keys = [
            "fallback-origin-1:BAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ="
          ];
        };
      };
    };
  };

  testScript = ''
    with subtest("nix trusts all configured upstream signing keys"):
        machine.start()

        nix_conf = machine.succeed("cat /etc/nix/nix.conf")
        for key in [
            "pull-through-1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "origin-1:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
            "cache-1:AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI=",
            "fallback-1:AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM=",
            "fallback-origin-1:BAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQ=",
        ]:
            assert key in nix_conf, \
                f"expected trusted-public-keys to include {key!r}; nix.conf was: {nix_conf!r}"
  '';
}
