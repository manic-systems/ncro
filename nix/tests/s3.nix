{
  pkgs,
  self,
}: let
  # Two payloads with distinct content so we can tell which backend served each.
  s3Payload = pkgs.runCommandLocal "ncro-s3-payload" {} ''
    mkdir -p "$out"
    echo "s3 upstream test payload" > "$out/data"
  '';

  authPayload = pkgs.runCommandLocal "ncro-auth-payload" {} ''
    mkdir -p "$out"
    echo "auth upstream test payload" > "$out/data"
  '';

  # Garage cluster settings.
  # Those are fixed values for a single-node ephemeral cluster.
  # XXX: rpc_secret must be 32 bytes, hex-encoded (64 chars).
  garageRpcSecret = "c8b325ea88a9ad61e58d0ed9ba91fd0db70699e3a30ca2d8a70ee0c4a09ceaf2";
  garageRegion = "garage";
  garageBucket = "nix-cache";
  garageAccessKey = "GK000000000000000000000000";
  garageSecretKey = "0000000000000000000000000000000000000000000000000000000000000001";

  # Credentials for the nginx-protected nix-serve (auth subtest).
  authUser = "ncro";
  authPass = "testpassword";

  cacheKeyName = "ncro-s3-test";

  commonBase = {
    virtualisation.memorySize = 1536;
    virtualisation.diskSize = 4096;
    networking.firewall.enable = false;
    nix.settings.experimental-features = ["nix-command"];
  };
in
  pkgs.testers.runNixOSTest {
    name = "ncro-s3";

    nodes = {
      # Runs Garage (S3-compatible object store) pre-populated with s3Payload.
      # Also runs nix-serve behind nginx with Basic Auth for the auth subtest.
      # Garage's web endpoint is also enabled for direct bucket assertions; ncro
      # itself talks to the authenticated S3 API on port 3900.
      backend = {
        config,
        pkgs,
        ...
      }: {
        imports = [commonBase];

        system.extraDependencies = [s3Payload authPayload];

        services.garage = {
          enable = true;
          package = pkgs.garage;
          settings = {
            rpc_bind_addr = "[::]:3901";
            rpc_public_addr = "127.0.0.1:3901";
            rpc_secret = garageRpcSecret;
            # v1.x single-node mode.
            replication_mode = "none";
            s3_api = {
              s3_region = garageRegion;
              api_bind_addr = "[::]:3900";
            };
            s3_web = {
              bind_addr = "[::]:3902";
              root_domain = ".web.garage";
              index = "index.html";
            };
          };
        };

        environment.systemPackages = [pkgs.garage];

        systemd.services = {
          # Bootstrap the Garage cluster, upload s3Payload as a Nix binary cache,
          # and enable anonymous web access for the bucket.
          setup-garage = {
            description = "Initialise Garage layout, bucket, and upload Nix store paths";
            wantedBy = ["multi-user.target"];
            after = ["garage.service" "nix-daemon.service"];
            requires = ["garage.service" "nix-daemon.service"];
            path = [pkgs.garage pkgs.gawk pkgs.coreutils];
            serviceConfig = {
              Type = "oneshot";
              RemainAfterExit = true;
              ExecStart = pkgs.writeShellScript "setup-garage" ''
                set -euo pipefail

                # Wait until the Garage daemon accepts admin requests.
                until ${pkgs.garage}/bin/garage status >/dev/null 2>&1; do
                  sleep 1
                done

                # Apply a single-node layout (zone dc1, 1 GiB capacity).
                # Capture full output before filtering to avoid Garage panicking
                # on SIGPIPE from short-reading consumers like cut/awk.
                node_fqn=$(${pkgs.garage}/bin/garage node id)
                node_id=$(printf '%s\n' "$node_fqn" | cut -d@ -f1)
                ${pkgs.garage}/bin/garage layout assign -z dc1 -c 1G "$node_id"
                layout_show=$(${pkgs.garage}/bin/garage layout show)
                version=$(printf '%s\n' "$layout_show" \
                  | awk '/Current cluster layout version:/{print $NF+1}')
                ${pkgs.garage}/bin/garage layout apply --version "$version"

                # Create the bucket and enable anonymous reads via the web endpoint.
                ${pkgs.garage}/bin/garage bucket create ${garageBucket}
                ${pkgs.garage}/bin/garage bucket website --allow ${garageBucket}

                # Create a deterministic key so the proxy node can use the
                # native authenticated S3 API path.
                ${pkgs.garage}/bin/garage key import \
                  --yes \
                  -n nix-upload \
                  ${garageAccessKey} \
                  ${garageSecretKey}
                ${pkgs.garage}/bin/garage bucket allow \
                  --read --write ${garageBucket} --key nix-upload

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
                  "${s3Payload}"

                # Upload the store path as a Nix binary cache.
                export AWS_ACCESS_KEY_ID=${garageAccessKey}
                export AWS_SECRET_ACCESS_KEY=${garageSecretKey}
                export AWS_REGION=${garageRegion}
                ${config.nix.package}/bin/nix copy \
                    --to 's3://${garageBucket}?endpoint=127.0.0.1:3900&scheme=http&region=${garageRegion}' \
                    "${s3Payload}"
              '';
            };
          };

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
            # Port 3903: strips /nix-cache/ prefix and proxies to Garage web
            # endpoint at port 3902 for direct test assertions.
            garage-web-adapter = {
              listen = [
                {
                  addr = "0.0.0.0";
                  port = 3903;
                }
              ];
              locations."~ ^/${garageBucket}/" = {
                extraConfig = ''
                  rewrite ^/${garageBucket}/(.+)$ /$1 break;
                  proxy_pass http://127.0.0.1:3902;
                  proxy_set_header Host ${garageBucket}.web.garage;
                '';
              };
            };

            # Port 8081 is for Basic Auth proxy to nix-serve for the auth subtest.
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

      # ncro node with two upstreams:
      #  1. s3://nix-cache?endpoint=backend:3900&scheme=http  (native S3 API)
      #  2. http://backend:8081 with username/password         (auth subtest)
      proxy = {
        imports = [self.nixosModules.ncro commonBase];

        nix.settings.trusted-substituters = ["http://localhost:8080"];

        services.ncro = {
          enable = true;
          settings = {
            server.listen = ":8080";
            upstreams = [
              {
                url = "s3://${garageBucket}?endpoint=backend:3900&scheme=http&region=${garageRegion}";
                priority = 1;
              }
              {
                url = "http://backend:8081";
                priority = 2;
                username = authUser;
                password = authPass;
              }
            ];
            cache = {
              ttl = "5m";
              negative_ttl = "30s";
            };
          };
        };

        systemd.services.ncro.environment = {
          AWS_ACCESS_KEY_ID = garageAccessKey;
          AWS_SECRET_ACCESS_KEY = garageSecretKey;
          AWS_REGION = garageRegion;
        };
      };

      badS3Proxy = {
        imports = [self.nixosModules.ncro commonBase];

        services.ncro = {
          enable = true;
          settings = {
            server.listen = ":8080";
            upstreams = [
              {
                url = "s3://${garageBucket}?endpoint=backend:3900&scheme=http&region=${garageRegion}";
                priority = 1;
              }
            ];
            cache = {
              ttl = "5m";
              negative_ttl = "30s";
              mass_query.upstream_cooldown = "1s";
            };
          };
        };

        systemd.services.ncro.environment = {
          AWS_ACCESS_KEY_ID = garageAccessKey;
          AWS_SECRET_ACCESS_KEY = "wrong-secret-key";
          AWS_REGION = garageRegion;
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

      def nar_url_from_narinfo(narinfo):
          for line in narinfo.splitlines():
              if line.startswith("URL: "):
                  return line.split("URL: ", 1)[1]
          raise AssertionError(f"narinfo missing URL field: {narinfo!r}")

      s3_path   = "${s3Payload}"
      auth_path = "${authPayload}"
      s3_hash   = store_hash(s3_path)
      auth_hash = store_hash(auth_path)

      with subtest("boot all nodes"):
          start_all()

          backend.wait_for_unit("garage.service")
          backend.wait_for_open_port(3900)
          backend.wait_for_unit("setup-garage.service")
          backend.wait_for_open_port(3902)
          backend.wait_for_open_port(3903)

          backend.wait_for_unit("gen-cache-key.service")
          backend.wait_for_unit("nix-serve.service")
          backend.wait_for_open_port(5000)
          backend.wait_for_unit("nginx.service")
          backend.wait_for_open_port(8081)

          proxy.wait_for_unit("ncro.service")
          proxy.wait_for_open_port(8080)
          badS3Proxy.wait_for_unit("ncro.service")
          badS3Proxy.wait_for_open_port(8080)

      with subtest("Garage bucket contains nix-cache-info"):
          out = backend.succeed(
              "curl -sf -H 'Host: ${garageBucket}.web.garage' "
              "http://127.0.0.1:3902/nix-cache-info"
          )
          assert "StoreDir" in out, \
              f"nix-cache-info missing StoreDir: {out!r}"

      with subtest("Garage bucket contains narinfo for s3Payload"):
          out = backend.succeed(
              f"curl -sf -H 'Host: ${garageBucket}.web.garage' "
              f"http://127.0.0.1:3902/{s3_hash}.narinfo"
          )
          assert "StorePath" in out, \
              f"Garage narinfo missing StorePath: {out!r}"

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

      with subtest("ncro status lists both upstreams"):
          h = ncro_status(proxy)
          assert "upstreams" in h, f"/status missing upstreams: {h!r}"
          urls = [u["url"] for u in h["upstreams"]]
          assert any("backend:3900" in u for u in urls), \
              f"S3 upstream missing from /status: {urls}"
          assert any("backend:8081" in u for u in urls), \
              f"auth upstream missing from /status: {urls}"

      with subtest("ncro proxies narinfo from S3 upstream"):
          cache_public_key = backend.succeed("cat /etc/nix/cache-key.pub").strip()
          out = proxy.succeed(
              f"curl -sf http://localhost:8080/{s3_hash}.narinfo"
          )
          assert "StorePath" in out, \
              f"ncro did not proxy S3 narinfo: {out!r}"
          assert "Sig: ${cacheKeyName}:" in out, \
              f"S3 narinfo missing signature: {out!r}"

      with subtest("ncro preserves byte-range responses from S3 NARs"):
          narinfo = proxy.succeed(
              f"curl -sf http://localhost:8080/{s3_hash}.narinfo"
          )
          nar_url = nar_url_from_narinfo(narinfo)
          headers = proxy.succeed(
              "curl -sS -D - -o /dev/null "
              "-H 'Range: bytes=0-15' "
              f"http://localhost:8080/{nar_url}"
          )
          header_lines = [line.rstrip("\r") for line in headers.splitlines()]
          lowered = [line.lower() for line in header_lines]
          assert any(line.startswith("HTTP/") and " 206 " in line for line in header_lines), \
              f"range request did not return 206 Partial Content: {headers!r}"
          assert any(line.startswith("content-range: bytes 0-15/") for line in lowered), \
              f"Content-Range header missing or wrong: {headers!r}"
          assert "accept-ranges: bytes" in lowered, \
              f"Accept-Ranges header missing: {headers!r}"

      with subtest("ncro supports HEAD for S3 NARs"):
          narinfo = proxy.succeed(
              f"curl -sf http://localhost:8080/{s3_hash}.narinfo"
          )
          nar_url = nar_url_from_narinfo(narinfo)
          headers = proxy.succeed(
              f"curl -sS -I http://localhost:8080/{nar_url}"
          )
          header_lines = [line.rstrip("\r") for line in headers.splitlines()]
          lowered = [line.lower() for line in header_lines]
          assert any(line.startswith("HTTP/") and " 200 " in line for line in header_lines), \
              f"HEAD request did not return 200 OK: {headers!r}"
          assert any(line.startswith("content-length: ") for line in lowered), \
              f"Content-Length header missing from S3 HEAD response: {headers!r}"
          assert "accept-ranges: bytes" in lowered, \
              f"Accept-Ranges header missing from S3 HEAD response: {headers!r}"

      with subtest("bad S3 credentials fail instead of falling back silently"):
          badS3Proxy.fail(
              f"curl -sf http://localhost:8080/{s3_hash}.narinfo"
          )

      with subtest("nix copy through ncro from S3 upstream"):
          proxy.fail(f"nix store ls {s3_path} 2>/dev/null")
          proxy.succeed(
              f"nix copy --from http://localhost:8080 --extra-trusted-public-keys '{cache_public_key}' {s3_path}"
          )
          proxy.succeed(f"test -f {s3_path}/data")
          proxy.succeed(f"grep -q 's3 upstream' {s3_path}/data")

      with subtest("ncro falls back from S3 miss to authenticated upstream"):
          out = proxy.succeed(
              f"curl -sf http://localhost:8080/{auth_hash}.narinfo"
          )
          assert "StorePath" in out, \
              f"ncro did not proxy auth narinfo: {out!r}"

      with subtest("nix copy through ncro from authenticated upstream"):
          proxy.fail(f"nix store ls {auth_path} 2>/dev/null")
          proxy.succeed(
              f"nix copy --from http://localhost:8080 --extra-trusted-public-keys '{cache_public_key}' {auth_path}"
          )
          proxy.succeed(f"test -f {auth_path}/data")
          proxy.succeed(f"grep -q 'auth upstream' {auth_path}/data")

      with subtest("ncro records cache hits after repeated requests"):
          proxy.succeed(f"curl -sf http://localhost:8080/{s3_hash}.narinfo > /dev/null")
          proxy.succeed(f"curl -sf http://localhost:8080/{auth_hash}.narinfo > /dev/null")
          metrics = proxy.succeed("curl -sf http://localhost:8080/metrics")
          assert "ncro_narinfo_cache" in metrics, \
              f"narinfo cache metric not in /metrics: {metrics[:400]!r}"
    '';
  }
