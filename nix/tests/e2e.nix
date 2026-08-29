{
  pkgs,
  self,
}: let
  # Two distinct payloads. One is served by nix-serve-ng, and the other is served
  # by harmonia. We embed distinct strings so we can verify which backend actually
  # served each in tests
  payload1 = pkgs.runCommandLocal "ncro-e2e-payload1" {} ''
    mkdir -p "$out"
    echo "e2e payload 1: nix-serve-ng backend" > "$out/data"
  '';

  payload2 = pkgs.runCommandLocal "ncro-e2e-payload2" {} ''
    mkdir -p "$out"
    echo "e2e payload 2: harmonia backend" > "$out/data"
  '';

  # Present in both backends. The filtered ncro node rejects this path on the
  # priority-1 backend, so a successful response must come from backend 2.
  filterPayload = pkgs.runCommandLocal "ncro-e2e-filter-payload" {} ''
    mkdir -p "$out"
    echo "e2e filter payload: available on both backends" > "$out/data"
  '';

  cacheKey1Name = "ncro-e2e-cache1";
  cacheKey2Name = "ncro-e2e-cache2";
  cacheKeyBadUrlName = "ncro-e2e-badurl-cache";

  # Shared NixOS module applied to every node.
  commonBase = {
    virtualisation.memorySize = 1024;
    virtualisation.diskSize = 4096;
    networking.firewall.enable = false;
    nix.settings.experimental-features = ["nix-command"];
  };
in
  pkgs.testers.runNixOSTest {
    name = "ncro-e2e";

    nodes = {
      # Runs nix-serve-ng. Generates a signing key at boot, signs payload1,
      # then starts the server.
      bincache1 = {
        config,
        pkgs,
        ...
      }: {
        imports = [commonBase];

        system.extraDependencies = [payload1 filterPayload];

        systemd.services.setup-cache = {
          description = "Generate signing key and sign e2e payload 1";
          wantedBy = ["multi-user.target"];
          before = ["nix-serve-ng.service"];
          after = ["nix-daemon.service"];
          requires = ["nix-daemon.service"];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            ExecStart = pkgs.writeShellScript "setup-cache1" ''
              set -euo pipefail
              mkdir -p /etc/nix
              if [ ! -f /etc/nix/cache-key.sec ]; then
                ${config.nix.package}/bin/nix-store \
                  --generate-binary-cache-key "${cacheKey1Name}" \
                  /etc/nix/cache-key.sec \
                  /etc/nix/cache-key.pub
              fi
              # World-readable so the server process can read it.
              chmod 644 /etc/nix/cache-key.pub /etc/nix/cache-key.sec
              ${config.nix.package}/bin/nix store sign \
                --key-file /etc/nix/cache-key.sec \
                "${payload1}"
              ${config.nix.package}/bin/nix store sign \
                --key-file /etc/nix/cache-key.sec \
                "${filterPayload}"
            '';
          };
        };

        # nix-serve-ng's mainProgram is "nix-serve"; signing key via env var.
        # FIXME: probably could use the NixOS option here
        systemd.services.nix-serve-ng = {
          description = "nix-serve-ng binary cache";
          wantedBy = ["multi-user.target"];
          after = [
            "setup-cache.service"
            "network.target"
          ];
          requires = ["setup-cache.service"];
          environment.NIX_SECRET_KEY_FILE = "/etc/nix/cache-key.sec";
          serviceConfig = {
            ExecStart = "${pkgs.nix-serve-ng}/bin/nix-serve --port 5000";
            Restart = "on-failure";
          };
        };
      };

      # Static binary cache whose narinfo advertises an unreachable absolute
      # NAR URL. The NAR itself is present under /nar/*, so a client only
      # succeeds when NCRO rewrites the narinfo URL back through itself.
      badurlcache = {
        config,
        pkgs,
        ...
      }: {
        imports = [commonBase];

        system.extraDependencies = [payload1];

        systemd.services.setup-badurl-cache = {
          description = "Generate static cache with deliberately bad narinfo URLs";
          wantedBy = ["multi-user.target"];
          before = ["badurl-cache.service"];
          after = ["nix-daemon.service"];
          requires = ["nix-daemon.service"];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            ExecStart = pkgs.writeShellScript "setup-badurl-cache" ''
              set -euo pipefail
              mkdir -p /etc/nix /srv/badurl-cache
              if [ ! -f /etc/nix/cache-key.sec ]; then
                ${config.nix.package}/bin/nix-store \
                  --generate-binary-cache-key "${cacheKeyBadUrlName}" \
                  /etc/nix/cache-key.sec \
                  /etc/nix/cache-key.pub
              fi
              chmod 644 /etc/nix/cache-key.pub /etc/nix/cache-key.sec

              ${config.nix.package}/bin/nix copy \
                --to 'file:///srv/badurl-cache?compression=xz&secret-key=/etc/nix/cache-key.sec' \
                '${payload1}'

              for narinfo in /srv/badurl-cache/*.narinfo; do
                ${pkgs.gnused}/bin/sed -i \
                  's#^URL: /*#URL: http://127.0.0.1:9/#' \
                  "$narinfo"
              done
            '';
          };
        };

        systemd.services.badurl-cache = {
          description = "Serve static cache with bad narinfo URLs";
          wantedBy = ["multi-user.target"];
          after = ["setup-badurl-cache.service" "network.target"];
          requires = ["setup-badurl-cache.service"];
          serviceConfig = {
            ExecStart = "${pkgs.python3}/bin/python3 -m http.server 5000 --directory /srv/badurl-cache"; # insane I know
            Restart = "on-failure";
          };
        };
      };

      # Runs harmonia. Same key-generation + signing pattern; harmonia loads
      # the key via systemd LoadCredential so chmod 644 is sufficient.
      bincache2 = {
        config,
        pkgs,
        lib,
        ...
      }: {
        imports = [commonBase];

        system.extraDependencies = [payload2 filterPayload];

        systemd.services.setup-cache = {
          description = "Generate signing key and sign e2e payload 2";
          wantedBy = ["multi-user.target"];
          before = ["harmonia.service"];
          after = ["nix-daemon.service"];
          requires = ["nix-daemon.service"];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            ExecStart = pkgs.writeShellScript "setup-cache2" ''
              set -euo pipefail

              mkdir -p /etc/nix
              if [ ! -f /etc/nix/cache-key.sec ]; then
                ${config.nix.package}/bin/nix-store \
                  --generate-binary-cache-key "${cacheKey2Name}" \
                  /etc/nix/cache-key.sec \
                  /etc/nix/cache-key.pub
              fi

              chmod 644 /etc/nix/cache-key.pub /etc/nix/cache-key.sec
              for path in "${payload2}" "${filterPayload}"; do
                ${config.nix.package}/bin/nix store sign "$path" \
                  --key-file /etc/nix/cache-key.sec
              done
            '';
          };
        };

        services.harmonia.cache = {
          enable = true;
          signKeyPaths = ["/etc/nix/cache-key.sec"];
        };

        # Start harmonia eagerly (not lazily via socket activation) and
        # only after the signing key is ready.
        systemd.services.harmonia = {
          wantedBy = ["multi-user.target"];
          after = lib.mkAfter ["setup-cache.service"];
          requires = ["setup-cache.service"];
        };
      };

      # First ncro instance. Proxies to both binary caches.
      host = {
        imports = [
          self.nixosModules.ncro
          commonBase
        ];

        nix.settings.trusted-substituters = ["http://localhost:8080"];

        services.ncro = {
          enable = true;
          settings = {
            server.listen = ":8080";
            upstreams = [
              {
                url = "http://bincache1:5000";
                priority = 1;
              }
              {
                url = "http://bincache2:5000";
                priority = 2;
              }
            ];

            cache = {
              ttl = "5m";
              negative_ttl = "30s";
            };
          };
        };
      };

      # Second ncro instance. Proxies exclusively through host's ncro,
      # exercising the two-hop path:
      # secondary --> host --> bincache.
      secondary = {
        imports = [
          self.nixosModules.ncro
          commonBase
        ];

        nix.settings.trusted-substituters = ["http://localhost:8080"];

        services.ncro = {
          enable = true;
          settings = {
            server.listen = ":8080";
            upstreams = [
              {
                url = "http://host:8080";
                priority = 1;
              }
            ];
            cache = {
              ttl = "5m";
              negative_ttl = "30s";
            };
          };
        };
      };

      # Third ncro instance. Both upstreams can serve filterPayload, but the
      # priority-1 upstream is configured to allow only payload1 by name. This
      # exercises post-narinfo filter rejection and fallback to the next backend.
      filtered = {
        imports = [
          self.nixosModules.ncro
          commonBase
        ];

        services.ncro = {
          enable = true;
          settings = {
            server.listen = ":8080";
            upstreams = [
              {
                url = "http://bincache1:5000";
                priority = 1;
                filters = [
                  {
                    action = "allow";
                    field = "name";
                    pattern = "ncro-e2e-payload1*";
                  }
                ];
              }
              {
                url = "http://bincache2:5000";
                priority = 2;
              }
            ];

            cache = {
              ttl = "5m";
              negative_ttl = "30s";
            };
          };
        };
      };

      # Fourth ncro instance. Its upstream intentionally advertises broken NAR
      # URLs, so nix copy only succeeds when nar_url_mode = "to_self" rewrites
      # the client-visible narinfo URL back through NCRO.
      rewriter = {
        imports = [
          self.nixosModules.ncro
          commonBase
        ];

        nix.settings.trusted-substituters = ["http://localhost:8080"];

        services.ncro = {
          enable = true;
          settings = {
            server.listen = ":8080";
            upstreams = [
              {
                url = "http://badurlcache:5000";
                priority = 1;
                nar_url_mode = "to_self";
              }
            ];

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

      def ncro_status_get_fallback(node, url_pattern):
          # some caches reject HEAD, 0 fails here means fallback worked and reset count.
          h = ncro_status(node)
          for u in h["upstreams"]:
              if url_pattern in u["url"]:
                  assert u["consecutive_fails"] == 0, \
                    f"{url_pattern} probe expected 0 fails (GET fallback), got {u['consecutive_fails']}"
                  return
          assert False, f"{url_pattern} not found in {node.name} upstreams"

      def store_hash(path):
          # /nix/store/<hash>-<name> → <hash>
          return path.split("/")[3].split("-")[0]

      payload1_path = "${payload1}"
      payload2_path = "${payload2}"
      filter_payload_path = "${filterPayload}"
      hash1 = store_hash(payload1_path)
      hash2 = store_hash(payload2_path)
      filter_hash = store_hash(filter_payload_path)

      start_all()

      bincache1.wait_for_unit("setup-cache.service")
      bincache1.wait_for_unit("nix-serve-ng.service")
      bincache1.wait_for_open_port(5000)

      badurlcache.wait_for_unit("setup-badurl-cache.service")
      badurlcache.wait_for_unit("badurl-cache.service")
      badurlcache.wait_for_open_port(5000)

      bincache2.wait_for_unit("setup-cache.service")
      bincache2.wait_for_unit("harmonia.service")
      bincache2.wait_for_open_port(5000)

      host.wait_for_unit("ncro.service")
      host.wait_for_open_port(8080)

      secondary.wait_for_unit("ncro.service")
      secondary.wait_for_open_port(8080)

      filtered.wait_for_unit("ncro.service")
      filtered.wait_for_open_port(8080)

      rewriter.wait_for_unit("ncro.service")
      rewriter.wait_for_open_port(8080)

      with subtest("binary caches serve nix-cache-info"):
          for node, port in ((bincache1, 5000), (bincache2, 5000)):
              out = node.succeed(f"curl -sf http://localhost:{port}/nix-cache-info")
              assert "StoreDir" in out, \
                  f"{node.name}: /nix-cache-info missing StoreDir: {out!r}"

      with subtest("each cache backend serves its own payload narinfo directly"):
          cache1_public_key = bincache1.succeed("cat /etc/nix/cache-key.pub").strip()
          cache2_public_key = bincache2.succeed("cat /etc/nix/cache-key.pub").strip()
          badurl_public_key = badurlcache.succeed("cat /etc/nix/cache-key.pub").strip()
          trusted_keys = f"{cache1_public_key} {cache2_public_key} {badurl_public_key}"

          out1 = bincache1.succeed(f"curl -sf http://localhost:5000/{hash1}.narinfo")
          assert "StorePath" in out1, \
              f"bincache1 (nix-serve-ng) did not serve narinfo for hash1: {out1!r}"
          assert "Sig: ${cacheKey1Name}:" in out1, \
              f"bincache1 narinfo missing signature: {out1!r}"

          out2 = bincache2.succeed(f"curl -sf http://localhost:5000/{hash2}.narinfo")
          assert "StorePath" in out2, \
              f"bincache2 (harmonia) did not serve narinfo for hash2: {out2!r}"
          assert "Sig: ${cacheKey2Name}:" in out2, \
              f"bincache2 narinfo missing signature: {out2!r}"

      with subtest("each cache returns 404 for the other's payload"):
          bincache1.fail(f"curl -sf http://localhost:5000/{hash2}.narinfo")
          bincache2.fail(f"curl -sf http://localhost:5000/{hash1}.narinfo")

      with subtest("host ncro proxies narinfo from nix-serve-ng backend"):
          out = host.succeed(f"curl -sf http://localhost:8080/{hash1}.narinfo")
          assert "StorePath" in out, \
              f"host ncro did not proxy hash1 narinfo: {out!r}"

      with subtest("host ncro proxies narinfo from harmonia backend"):
          out = host.succeed(f"curl -sf http://localhost:8080/{hash2}.narinfo")
          assert "StorePath" in out, \
              f"host ncro did not proxy hash2 narinfo: {out!r}"

      with subtest("path filters reject disallowed priority-1 narinfo"):
          out = filtered.succeed(f"curl -sf http://localhost:8080/{filter_hash}.narinfo")
          assert "StorePath" in out, \
              f"filtered ncro did not serve shared payload narinfo: {out!r}"
          assert "${filterPayload}" in out, \
              f"filtered ncro returned wrong store path: {out!r}"
          assert "Sig: ${cacheKey2Name}:" in out, \
              f"filtered ncro did not fall back to backend 2 after filter rejection: {out!r}"
          assert "Sig: ${cacheKey1Name}:" not in out, \
              f"filtered ncro accepted backend 1 despite path filter: {out!r}"

      with subtest("secondary ncro proxies both narinfos through host (two-hop)"):
          out1 = secondary.succeed(f"curl -sf http://localhost:8080/{hash1}.narinfo")
          assert "StorePath" in out1, \
              f"secondary did not proxy hash1 through host: {out1!r}"

          out2 = secondary.succeed(f"curl -sf http://localhost:8080/{hash2}.narinfo")
          assert "StorePath" in out2, \
              f"secondary did not proxy hash2 through host: {out2!r}"

      with subtest("nix copy payload1 (nix-serve-ng) through host ncro"):
          host.fail(f"nix store ls {payload1_path} 2>/dev/null")
          host.succeed(
              f"nix copy --from http://localhost:8080 --extra-trusted-public-keys '{trusted_keys}' {payload1_path}"
          )
          host.succeed(f"test -f {payload1_path}/data")
          host.succeed(f"grep -q 'nix-serve-ng' {payload1_path}/data")

      with subtest("nar_url_mode to_self rewrites bad upstream nar URL"):
          upstream_narinfo = badurlcache.succeed(f"curl -sf http://localhost:5000/{hash1}.narinfo")
          assert "URL: http://127.0.0.1:9/nar/" in upstream_narinfo, \
              f"badurlcache did not advertise unreachable absolute NAR URL: {upstream_narinfo!r}"

          rewritten_narinfo = rewriter.succeed(f"curl -sf http://localhost:8080/{hash1}.narinfo")
          assert "URL: nar/" in rewritten_narinfo, \
              f"rewriter ncro did not rewrite NAR URL to relative path: {rewritten_narinfo!r}"
          assert "127.0.0.1:9" not in rewritten_narinfo, \
              f"rewriter ncro leaked unreachable upstream NAR URL: {rewritten_narinfo!r}"

          rewriter.fail(f"nix store ls {payload1_path} 2>/dev/null")
          rewriter.succeed(
              f"nix copy --from http://localhost:8080 --extra-trusted-public-keys '{trusted_keys}' {payload1_path}"
          )
          rewriter.succeed(f"test -f {payload1_path}/data")
          rewriter.succeed(f"grep -q 'nix-serve-ng' {payload1_path}/data")

      with subtest("nix copy payload2 (harmonia) through host ncro"):
          host.fail(f"nix store ls {payload2_path} 2>/dev/null")
          host.succeed(
              f"nix copy --from http://localhost:8080 --extra-trusted-public-keys '{trusted_keys}' {payload2_path}"
          )
          host.succeed(f"test -f {payload2_path}/data")
          host.succeed(f"grep -q 'harmonia' {payload2_path}/data")

      with subtest("nix copy both payloads through secondary ncro (two hops)"):
          secondary.fail(f"nix store ls {payload1_path} 2>/dev/null")
          secondary.succeed(
              f"nix copy --from http://localhost:8080 --extra-trusted-public-keys '{trusted_keys}' {payload1_path}"
          )
          secondary.succeed(f"test -f {payload1_path}/data")
          secondary.succeed(f"grep -q 'nix-serve-ng' {payload1_path}/data")

          secondary.fail(f"nix store ls {payload2_path} 2>/dev/null")
          secondary.succeed(
              f"nix copy --from http://localhost:8080 --extra-trusted-public-keys '{trusted_keys}' {payload2_path}"
          )
          secondary.succeed(f"test -f {payload2_path}/data")
          secondary.succeed(f"grep -q 'harmonia' {payload2_path}/data")

      with subtest("host ncro records cache hits after repeated narinfo requests"):
          # Both hashes were already fetched above; a second request should hit
          # the in-memory or DB cache. Verify via the Prometheus metrics counter.
          host.succeed(f"curl -sf http://localhost:8080/{hash1}.narinfo > /dev/null")
          host.succeed(f"curl -sf http://localhost:8080/{hash2}.narinfo > /dev/null")
          metrics = host.succeed("curl -sf http://localhost:8080/metrics")
          assert "narinfo_cache_hits" in metrics, \
              f"host ncro: cache hit metric not found in: {metrics[:300]!r}"

      with subtest("bincache2 probe succeeds with HEAD -> GET fallback"):
          ncro_status_get_fallback(host, "bincache2")

      with subtest("secondary status endpoint lists host as upstream"):
          h = ncro_status(secondary)
          upstream_urls = [u["url"] for u in h.get("upstreams", [])]
          assert any("host" in u for u in upstream_urls), \
              f"host not in secondary upstreams: {upstream_urls}"

      with subtest("metrics endpoint returns Prometheus format on both ncro nodes"):
          for node in (host, secondary):
              metrics = node.succeed("curl -sf http://localhost:8080/metrics")
              assert "# TYPE" in metrics, \
                  f"{node.name}: /metrics not in Prometheus format: {metrics[:200]!r}"

      with subtest("resilience: payload2 still routed after bincache1 stops"):
          # Stop bincache1 and wait until its port is gone so ncro hits a
          # real connection error on the next request.
          bincache1.execute("systemctl stop nix-serve-ng")
          bincache1.wait_until_fails("curl -sf http://localhost:5000/nix-cache-info")

          # payload2 lives only on bincache2. The router gets NetworkError
          # from the priority-1 group (bincache1) and falls through to the
          # priority-2 group (bincache2). Request must succeed nevertheless.
          out = host.succeed(f"curl -sf http://localhost:8080/{hash2}.narinfo")
          assert "StorePath" in out, \
              f"host ncro lost payload2 routing after bincache1 went down: {out!r}"

          # Verify the two-hop path (secondary -> host -> bincache2) holds too.
          out = secondary.succeed(f"curl -sf http://localhost:8080/{hash2}.narinfo")
          assert "StorePath" in out, \
              f"secondary ncro lost payload2 routing after bincache1 went down: {out!r}"
    '';
  }
