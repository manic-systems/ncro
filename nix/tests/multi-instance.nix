{
  pkgs,
  self,
}:
pkgs.testers.runNixOSTest {
  name = "ncro-multi-instance";

  nodes.machine = {
    imports = [self.nixosModules.ncro];

    virtualisation.memorySize = 512;
    networking.firewall.enable = false;

    services.ncro = {
      enable = true;
      instances = {
        public.settings.server.listen = "127.0.0.1:8081";
        private = {
          socketActivation = true;
          settings.server.listen = "127.0.0.1:8082";
        };
      };
    };
  };

  testScript = ''
    machine.start()

    with subtest("named instances start independently"):
        machine.wait_for_unit("ncro@public.service")
        machine.wait_for_unit("ncro@private.socket")
        machine.succeed("! systemctl cat ncro.service")

    with subtest("each instance listens on its configured address"):
        for port in (8081, 8082):
            out = machine.succeed(f"curl -sf http://127.0.0.1:{port}/nix-cache-info")
            assert "StoreDir" in out, f"unexpected response on {port}: {out!r}"
        machine.wait_for_unit("ncro@private.service")

    with subtest("each instance has an isolated route database"):
        machine.succeed("test -f /var/lib/ncro-public/routes.db")
        machine.succeed("test -f /var/lib/ncro-private/routes.db")
  '';
}
