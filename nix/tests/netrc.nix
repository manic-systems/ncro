{
  pkgs,
  self,
}: let
  # Payload served by the Basic-Auth-protected nix-serve backend. The distinct
  # content lets us prove ncro actually fetched it through the upstream.
  authPayload = pkgs.runCommandLocal "ncro-netrc-payload" {} ''
    mkdir -p "$out"
    echo "netrc upstream test payload" > "$out/data"
  '';

  # Credentials for the nginx-protected nix-serve. These are provided to ncro
  # exclusively through a netrc file (never in services.ncro.settings), so the
  # test exercises the netrc credential fallback in Config::load.
  authUser = "ncro";
  authPass = "testpassword";

  cacheKeyName = "ncro-netrc-test";

  # netrc file consumed by ncro. The machine name matches the upstream host
  # (`backend`, derived from http://backend:8081 via Url::host_str), so the
  # credential resolver fills the empty username/password for that upstream.
  netrcFile = pkgs.writeText "ncro-netrc" ''
    machine backend login ${authUser} password ${authPass}
  '';

  commonBase = {
    virtualisation.memorySize = 1024;
    virtualisation.diskSize = 4096;
    networking.firewall.enable = false;
    nix.settings.experimental-features = ["nix-command"];
  };

  # ncro upstream list shared by both proxy nodes: a single Basic-Auth upstream
  # with NO inline credentials. The only way to authenticate is via netrc.
  authUpstreams = [
    {
      url = "http://backend:8081";
      priority = 1;
    }
  ];
in
  pkgs.testers.runNixOSTest {
    name = "ncro-netrc";

    nodes = {
      # nix-serve behind an nginx Basic Auth proxy on port 8081.
      backend = {
        config,
        pkgs,
        ...
      }: {
        imports = [commonBase];

        system.extraDependencies = [authPayload];

        systemd.services = {
          gen-cache-key = {
            description = "Generate Nix binary cache signing key";
            wantedBy = ["multi-user.target"];
            before = ["nix-serve.service"];
            after = ["nix-daemon.service"];
            requires = ["nix-daemon.service"];
            serviceConfig = {
              Type = "oneshot";
              RemainAfterExit = true;
              ExecStart = pkgs.writeShellScript "gen-cache-key" ''
                set -euo pipefail
                mkdir -p /etc/nix
                if [ ! -f /etc/nix/cache-key.sec ]; then
                  ${config.nix.package}/bin/nix-store \
                    --generate-binary-cache-key "${cacheKeyName}" \
                    /etc/nix/cache-key.sec \
                    /etc/nix/cache-key.pub
                fi
                chmod 644 /etc/nix/cache-key.pub /etc/nix/cache-key.sec
                ${config.nix.package}/bin/nix store sign \
                  --key-file /etc/nix/cache-key.sec \
                  "${authPayload}"
              '';
            };
          };

          nix-serve = {
            description = "nix-serve binary cache (port 5000, plain HTTP)";
            wantedBy = ["multi-user.target"];
            after = ["gen-cache-key.service" "network.target"];
            requires = ["gen-cache-key.service"];
            environment.NIX_SECRET_KEY_FILE = "/etc/nix/cache-key.sec";
            serviceConfig = {
              ExecStart = "${pkgs.nix-serve}/bin/nix-serve --port 5000";
              Restart = "on-failure";
            };
          };
        };

        services.nginx = {
          enable = true;
          virtualHosts = {
            # Basic Auth proxy in front of nix-serve.
            auth-cache = {
              listen = [
                {
                  addr = "0.0.0.0";
                  port = 8081;
                }
              ];
              basicAuth = {"${authUser}" = authPass;};
              locations."/" = {proxyPass = "http://127.0.0.1:5000";};
            };
          };
        };
      };

      # ncro with the auth upstream and credentials supplied only via netrc.
      proxy = {
        imports = [self.nixosModules.ncro commonBase];

        nix.settings.trusted-substituters = ["http://localhost:8080"];

        services.ncro = {
          enable = true;
          netrcFile = netrcFile;
          settings = {
            server.listen = ":8080";
            upstreams = authUpstreams;
            cache = {
              ttl = "5m";
              negative_ttl = "30s";
            };
          };
        };
      };

      # Negative control: identical upstream, but no netrc and no inline
      # credentials. Every authenticated fetch must fail.
      noNetrcProxy = {
        imports = [self.nixosModules.ncro commonBase];

        services.ncro = {
          enable = true;
          settings = {
            server.listen = ":8080";
            upstreams = authUpstreams;
            cache = {
              ttl = "5m";
              negative_ttl = "30s";
            };
          };
        };
      };
    };

    testScript = ''
      import json

      def ncro_status(node):
          out = node.succeed("curl -sf http://localhost:8080/status")
          return json.loads(out)

      def store_hash(path):
          # /nix/store/<hash>-<name> -> <hash>
          return path.split("/")[3].split("-")[0]

      auth_path = "${authPayload}"
      auth_hash = store_hash(auth_path)

      with subtest("boot all nodes"):
          start_all()

          backend.wait_for_unit("gen-cache-key.service")
          backend.wait_for_unit("nix-serve.service")
          backend.wait_for_open_port(5000)
          backend.wait_for_unit("nginx.service")
          backend.wait_for_open_port(8081)

          proxy.wait_for_unit("ncro.service")
          proxy.wait_for_open_port(8080)
          noNetrcProxy.wait_for_unit("ncro.service")
          noNetrcProxy.wait_for_open_port(8080)

      with subtest("auth backend rejects unauthenticated requests"):
          backend.fail(
              f"curl -sf http://127.0.0.1:8081/{auth_hash}.narinfo"
          )

      with subtest("auth backend accepts requests with correct credentials"):
          out = backend.succeed(
              f"curl -sf -u ${authUser}:${authPass} http://127.0.0.1:8081/{auth_hash}.narinfo"
          )
          assert "StorePath" in out, \
              f"auth backend did not serve narinfo with credentials: {out!r}"

      with subtest("ncro status lists the auth upstream"):
          h = ncro_status(proxy)
          urls = [u["url"] for u in h.get("upstreams", [])]
          assert any("backend:8081" in u for u in urls), \
              f"auth upstream missing from /status: {urls}"

      with subtest("ncro proxies narinfo using netrc credentials"):
          out = proxy.succeed(
              f"curl -sf http://localhost:8080/{auth_hash}.narinfo"
          )
          assert "StorePath" in out, \
              f"ncro did not proxy auth narinfo via netrc: {out!r}"
          assert "Sig: ${cacheKeyName}:" in out, \
              f"auth narinfo missing signature: {out!r}"

      with subtest("nix copy through ncro using netrc credentials"):
          cache_public_key = backend.succeed("cat /etc/nix/cache-key.pub").strip()
          proxy.fail(f"nix store ls {auth_path} 2>/dev/null")
          proxy.succeed(
              f"nix copy --from http://localhost:8080 --extra-trusted-public-keys '{cache_public_key}' {auth_path}"
          )
          proxy.succeed(f"test -f {auth_path}/data")
          proxy.succeed(f"grep -q 'netrc upstream' {auth_path}/data")

      with subtest("ncro without netrc cannot authenticate to the upstream"):
          noNetrcProxy.fail(
              f"curl -sf http://localhost:8080/{auth_hash}.narinfo"
          )
    '';
  }
