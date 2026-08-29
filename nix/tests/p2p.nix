{
  pkgs,
  self,
}: let
  # A tiny derivation used as the test payload. The store path is computed
  # at evaluation time and embedded into the test script as a literal path.
  testStorePath = pkgs.runCommand "ncro-test-payload" {} ''
    mkdir -p "$out"
    echo "ncro p2p test payload" > "$out/data"
  '';

  # Key name used in Nix's "name:base64pubkey" format.
  cacheKeyName = "ncro-test-cache-1";

  # ncro's config.Validate() requires at least one static upstream. We point
  # all nodes at cache.nixos.org as a last-resort fallback so that:
  #   a) ncro does not refuse to start with an empty upstreams list
  #   b) The test can verify that *dynamic* upstreams (via discovery) are
  #      preferred, because the test payload will not be in cache.nixos.org
  commonNcroSettings = {
    server.listen = ":8080";
    upstreams = [
      {
        url = "https://cache.nixos.org";
        priority = 100; # lowest priority; dynamic peers are added at 10
        public_key = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=";
      }
    ];

    cache = {
      ttl = "5m";
      negative_ttl = "30s";
    };

    discovery = {
      enabled = true;
      service_name = "_nix-serve._tcp";
      domain = "local";

      # Short window so the test does not have to wait too long.  Stale
      # entries are evicted after discovery_time * 3 = 15 s.
      discovery_time = "5s";
      priority = 10;

      # nix-serve binds to 0.0.0.0 (IPv4 only); restrict discovery to IPv4
      # addresses so ncro does not register unreachable IPv6 upstreams.
      address_family = "ipv4";
    };
  };

  # Shared avahi configuration. Firewall is disabled so avahi multicast
  # traffic crosses the virtual network without impediment.
  commonAvahi = {
    enable = true;
    nssmdns4 = true;
    publish = {
      enable = true;
      addresses = true;
      userServices = true;
    };
  };

  # Both nodes that run nix-serve share the same key name so node2 can verify
  # signatures from either host with a single entry in trusted-public-keys.
  keygenScript = pkgs.writeShellScript "gen-cache-key" ''
    set -euo pipefail
    mkdir -p /etc/nix
    if [ ! -f /etc/nix/cache-key.sec ]; then
      ${pkgs.nix}/bin/nix-store \
        --generate-binary-cache-key "${cacheKeyName}" \
        /etc/nix/cache-key.sec \
        /etc/nix/cache-key.pub
    fi

    # Make the public key world-readable so tests can read it.
    chmod 644 /etc/nix/cache-key.pub
  '';

  # Each node imports this and merges in its node-specific overrides on top.
  commonNodeBase = {
    virtualisation.memorySize = 2048;
    virtualisation.diskSize = 8192;

    networking.useNetworkd = true;
    networking.firewall.enable = false;

    environment.systemPackages = [pkgs.curl];

    services.avahi = commonAvahi;
    services.ncro = {
      enable = true;
      settings = commonNcroSettings;
    };

    # nix store sign is part of the nix-command experimental feature.
    nix.settings.experimental-features = ["nix-command"];
  };
in
  pkgs.testers.runNixOSTest {
    name = "ncro-p2p-discovery";

    nodes = {
      # node1 serves the test payload via nix-serve, runs ncro for routing
      node1 = {pkgs, ...}: {
        imports = [self.nixosModules.ncro commonNodeBase];

        # Generate the signing key at first boot before other services start.
        systemd.services.gen-cache-key = {
          description = "Generate Nix binary cache signing key";
          wantedBy = ["multi-user.target"];
          before = ["nix-serve.service" "ncro.service"];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            ExecStart = keygenScript;
          };
        };

        services = {
          nix-serve = {
            enable = true;
            secretKeyFile = "/etc/nix/cache-key.sec";
            port = 5000;
          };

          ncro = {
            enable = true;
            settings =
              commonNcroSettings
              // {
                # Include the local nix-serve as a guaranteed reachable upstream so
                # ncro on this node can serve paths that are in the local store even
                # when the internet (cache.nixos.org) is unavailable inside the VM.
                upstreams =
                  commonNcroSettings.upstreams
                  ++ [
                    {
                      url = "http://127.0.0.1:5000";
                      priority = 1;
                    }
                  ];
              };
          };
        };

        # Advertise nix-serve via avahi so ncro can discover it via mDNS.
        # nix-serve does not register itself with avahi; the service file must
        # be provided explicitly.
        environment.etc."avahi/services/nix-serve.service".text = ''
          <?xml version="1.0" standalone='no'?>
          <!DOCTYPE service-group SYSTEM "avahi-service.dtd">
          <service-group>
            <name replace-wildcards="yes">nix-serve on %h</name>
            <service>
              <type>_nix-serve._tcp</type>
              <port>5000</port>
            </service>
          </service-group>
        '';

        # Embed the test payload into the system closure so the Nix store on
        # node1 definitely contains it when the VM boots.
        system.extraDependencies = [testStorePath];

        # Authoritative signing: runs after gen-cache-key ensures the key exists.
        # Must run as root so nix store sign can write trust info into the store.
        systemd.services.sign-test-payload = {
          description = "Sign test store path for binary cache";
          wantedBy = ["multi-user.target"];
          after = ["gen-cache-key.service" "nix-daemon.service"];
          requires = ["gen-cache-key.service" "nix-daemon.service"];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            User = "root";
            ExecStart = pkgs.writeShellScript "sign-payload" ''
              ${pkgs.nix}/bin/nix store sign \
                --key-file /etc/nix/cache-key.sec \
                "${testStorePath}"
            '';
          };
        };
      };

      # node2 runs ncro only; fetches through discovered peers
      node2 = {lib, ...}: {
        imports = [self.nixosModules.ncro commonNodeBase];

        # Point nix at ncro as primary substituter.
        # trusted-public-keys must include the cache key from node1/node3.
        # Because the VMs generate their key at runtime we cannot embed the
        # actual base64 value here; instead we configure node2 to trust any
        # key whose name matches ${cacheKeyName} by setting
        # accept-flake-config = false and relying on the narinfo signature
        # verification inside ncro (public_key in upstream config).
        #
        # For the test we must still configure a trusted-public-keys entry.
        # We work around the dynamic key problem by reading the public key
        # from node1 in the test script and using `nix.extraOptions` to
        # accept it at runtime via environment.
        nix.settings = {
          substituters = lib.mkForce ["http://localhost:8080"];
          # Start with cache.nixos.org key so nix doesn't reject everything;
          # the test script will add the runtime-generated key separately.
          trusted-public-keys = [
            "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="
          ];
          # Allow the test to add extra substituter without rebuilding.
          trusted-substituters = ["http://localhost:8080"];
          experimental-features = ["nix-command"];
        };
      };

      # node3 runs nix-serve + ncro; second source for the test payload
      node3 = {lib, ...}: {
        imports = [self.nixosModules.ncro commonNodeBase];

        systemd.services.gen-cache-key = {
          description = "Generate Nix binary cache signing key";
          wantedBy = ["multi-user.target"];
          before = ["nix-serve.service" "ncro.service"];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            ExecStart = keygenScript;
          };
        };

        services.nix-serve = {
          enable = true;
          secretKeyFile = "/etc/nix/cache-key.sec";
          port = 5000;
        };

        # Advertise nix-serve via avahi so ncro can discover it via mDNS.
        environment.etc."avahi/services/nix-serve.service".text = ''
          <?xml version="1.0" standalone='no'?>
          <!DOCTYPE service-group SYSTEM "avahi-service.dtd">
          <service-group>
            <name replace-wildcards="yes">nix-serve on %h</name>
            <service>
              <type>_nix-serve._tcp</type>
              <port>5000</port>
            </service>
          </service-group>
        '';

        services.ncro = {
          enable = true;
          settings =
            commonNcroSettings
            // {
              # Include the local nix-serve as a guaranteed reachable upstream.
              upstreams =
                commonNcroSettings.upstreams
                ++ [
                  {
                    url = "http://127.0.0.1:5000";
                    priority = 1;
                  }
                ];
            };
        };

        # node3 does NOT have the test payload pre-loaded; it will fetch the
        # payload through its own ncro proxy (discovering node1).
        nix.settings = {
          substituters = lib.mkForce ["http://localhost:8080"];
          trusted-public-keys = ["cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="];
          trusted-substituters = ["http://localhost:8080"];
        };
      };
    };

    testScript = ''
      import time
      import json

      def ncro_status(node):
          """Return the parsed /status JSON from ncro on the given node."""
          out = node.succeed("curl -sf http://localhost:8080/status")
          return json.loads(out)

      def ncro_upstream_urls(node):
          """Return the list of upstream URLs reported by ncro /status."""
          h = ncro_status(node)
          return [u["url"] for u in h.get("upstreams", [])]

      def wait_for_upstreams(node, min_count, timeout=60):
          """
          Poll /status until at least min_count upstreams are listed or
          timeout expires.  Raises on timeout.
          """
          deadline = time.time() + timeout
          while time.time() < deadline:
              try:
                  urls = ncro_upstream_urls(node)
                  if len(urls) >= min_count:
                      return urls
              except Exception:
                  pass
              time.sleep(2)
          raise AssertionError(
              f"timed out waiting for {min_count} upstreams on {node.name}; "
              f"got: {ncro_upstream_urls(node)}"
          )

      with subtest("boot all nodes"):
          start_all()

          node1.wait_for_unit("gen-cache-key.service")
          node1.wait_for_unit("sign-test-payload.service")
          node1.wait_for_unit("avahi-daemon.service")
          node1.wait_for_unit("nix-serve.service")
          node1.wait_for_unit("ncro.service")
          node1.wait_for_open_port(5000)   # nix-serve default port
          node1.wait_for_open_port(8080)   # ncro

          node2.wait_for_unit("avahi-daemon.service")
          node2.wait_for_unit("ncro.service")
          node2.wait_for_open_port(8080)

          node3.wait_for_unit("gen-cache-key.service")
          node3.wait_for_unit("avahi-daemon.service")
          node3.wait_for_unit("nix-serve.service")
          node3.wait_for_unit("ncro.service")
          node3.wait_for_open_port(5000)
          node3.wait_for_open_port(8080)

      with subtest("verify HTTP endpoints are functional"):
          # /nix-cache-info must return a valid response with StoreDir.
          for node in (node1, node2, node3):
              out = node.succeed("curl -sf http://localhost:8080/nix-cache-info")
              assert "StoreDir" in out, \
                  f"{node.name}: /nix-cache-info missing StoreDir: {out!r}"
              assert "/nix/store" in out, \
                  f"{node.name}: /nix-cache-info has wrong StoreDir: {out!r}"

          # /status must return JSON with a 'status' field and a non-empty
          # upstreams list where each entry carries url and status.
          for node in (node1, node2, node3):
              h = ncro_status(node)
              assert "status" in h, \
                  f"{node.name}: /status missing 'status': {h!r}"
              assert "upstreams" in h, \
                  f"{node.name}: /status missing 'upstreams': {h!r}"
              assert len(h["upstreams"]) > 0, \
                  f"{node.name}: /status upstreams list is empty"
              for up in h["upstreams"]:
                  assert "url" in up and "status" in up, \
                      f"{node.name}: upstream entry missing fields: {up!r}"

          # /metrics must return Prometheus-format text (at least one TYPE line).
          # XXX: This test is rather useless, but I don't want to verify the entire
          # thing. Maybe in the future?
          for node in (node1, node2, node3):
              metrics_out = node.succeed("curl -sf http://localhost:8080/metrics")
              assert "# TYPE" in metrics_out, \
                  f"{node.name}: /metrics not in Prometheus format: {metrics_out[:200]!r}"

      with subtest("read the runtime-generated public key from node1"):
          # The key was generated at boot; verify it has the expected format.
          pub_key = node1.succeed("cat /etc/nix/cache-key.pub").strip()
          expected_prefix = "${cacheKeyName}:"
          assert pub_key.startswith(expected_prefix), \
              f"unexpected public key format: {pub_key!r}"

      with subtest("wait for mDNS discovery to converge"):
          # discovery_time=5s; avahi needs a few seconds to propagate mDNS records
          # across the virtual network before ncro can discover them.
          # We poll /status rather than sleeping a fixed amount.

          # node2 should discover node1 and node3 (both run nix-serve).
          # The static cache.nixos.org upstream plus 2 discovered = >=3 total.
          node2_upstreams = wait_for_upstreams(node2, min_count=3, timeout=90)
          print(f"node2 upstreams after discovery: {node2_upstreams}")

          # Verify the discovery log messages contain the expected text.
          node2.succeed(
              "journalctl -u ncro --no-pager | grep -q 'discovered nix-serve instance'"
          )

          # node1 should have discovered node3 (its own nix-serve is not a remote peer).
          # node1 static: 127.0.0.1:5000 + cache.nixos.org; discovered: node3 -> >=3.
          node1_upstreams = wait_for_upstreams(node1, min_count=3, timeout=90)
          print(f"node1 upstreams after discovery: {node1_upstreams}")

          # node3 should have discovered node1.
          # node3 static: 127.0.0.1:5000 + cache.nixos.org; discovered: node1 -> >=3.
          node3_upstreams = wait_for_upstreams(node3, min_count=3, timeout=90)
          print(f"node3 upstreams after discovery: {node3_upstreams}")

      with subtest("mDNS: discovered upstream URLs use routable addresses"):
          # Avahi publishes all host addresses including loopback (127.0.0.1,
          # ::1). ncro must filter these: using them would route requests to
          # the requesting node's own loopback instead of the remote nix-serve.
          for node, upstreams in ((node2, node2_upstreams), (node1, node1_upstreams), (node3, node3_upstreams)):
              for url in upstreams:
                  # Static 127.0.0.1:5000 on node1/node3 is intentional; skip it.
                  if url == "http://127.0.0.1:5000":
                      continue
                  assert "127.0.0.1" not in url, \
                      f"{node.name}: discovered upstream contains loopback IPv4: {url!r}"
                  assert "[::1]" not in url, \
                      f"{node.name}: discovered upstream contains loopback IPv6: {url!r}"

      with subtest("mDNS: discovered upstream URLs use the advertised port"):
          # The avahi service file advertises port 5000.  ncro must use the
          # port from the mDNS record, not a hardcoded or default value.
          for node, upstreams in ((node2, node2_upstreams), (node1, node1_upstreams), (node3, node3_upstreams)):
              for url in upstreams:
                  if "cache.nixos.org" in url:
                      continue
                  assert ":5000" in url or url.endswith(":443"), \
                      f"{node.name}: discovered upstream does not use port 5000: {url!r}"

      with subtest("mDNS: cross-node discovery is symmetric"):
          # node1 must have discovered node3, and node3 must have discovered
          # node1.  Both advertise nix-serve; neither should be missing.
          node1_discovered = [u for u in node1_upstreams if "cache.nixos.org" not in u and u != "http://127.0.0.1:5000"]
          node3_discovered = [u for u in node3_upstreams if "cache.nixos.org" not in u and u != "http://127.0.0.1:5000"]
          assert len(node1_discovered) >= 1, \
              f"node1 did not discover any peers: {node1_upstreams}"
          assert len(node3_discovered) >= 1, \
              f"node3 did not discover any peers: {node3_upstreams}"

      with subtest("verify narinfo is served by ncro"):
          test_store_path = "${testStorePath}"
          store_hash = test_store_path.split("/")[3].split("-")[0]
          cache_public_key = node1.succeed("cat /etc/nix/cache-key.pub").strip()

          # ncro on node2 must proxy the narinfo request to node1 (which has the
          # path in its local nix-serve). node1 is discovered via mDNS.
          narinfo = node2.succeed(f"curl -sf http://localhost:8080/{store_hash}.narinfo")
          assert "Sig: ${cacheKeyName}:" in narinfo, \
              f"proxied narinfo lost upstream signature: {narinfo!r}"

      with subtest("fetch test payload through ncro on node2"):
          # Ensure the test path is not already present on node2.
          node2.fail(f"nix store ls {test_store_path} 2>/dev/null")

          node2.succeed(
              "nix copy "
              "--from http://localhost:8080 "
              f"--extra-trusted-public-keys '{cache_public_key}' "
              f"{test_store_path} "
              "2>&1"
          )

          # The file must now exist on node2.
          node2.succeed(f"test -f {test_store_path}/data")
          node2.succeed(f"grep -q 'ncro p2p test payload' {test_store_path}/data")

      with subtest("fetch test payload through ncro on node3"):
          node3.fail(f"nix store ls {test_store_path} 2>/dev/null")

          node3.succeed(
              "nix copy "
              "--from http://localhost:8080 "
              f"--extra-trusted-public-keys '{cache_public_key}' "
              f"{test_store_path} "
              "2>&1"
          )
          node3.succeed(f"test -f {test_store_path}/data")

      with subtest("stale peer removal after avahi stops advertising"):
          # Stop avahi on node1 so it sends mDNS goodbye packets and ncro on
          # node2/node3 stops receiving keep-alive announcements.
          # Stopping only nix-serve is insufficient because avahi continues to
          # advertise the service record even after the daemon is gone.
          node1.succeed("systemctl stop avahi-daemon.service")

          # Stale TTL = discovery_time * 3 = 5s * 3 = 15s.  Add margin.
          time.sleep(25)

          # ncro must have logged the removal.
          node2.succeed(
              "journalctl -u ncro --no-pager | grep -q 'removing stale peer'"
          )

          # /status should now report fewer upstreams (node1's instance removed).
          node2_upstreams_after = ncro_upstream_urls(node2)
          print(f"node2 upstreams after node1 avahi stopped: {node2_upstreams_after}")

          # All of node1's upstream URLs (one per advertised address) must be gone.
          node1_ips = node1.succeed("hostname -I").strip().split()
          for ip in node1_ips:
              assert not any(ip in u for u in node2_upstreams_after), \
                  f"node1 IP {ip!r} still in node2 upstreams after stale eviction: {node2_upstreams_after}"

      with subtest("node2 can still fetch through node3 after node1 leaves"):
          # Remove the path from node2 so we force a fresh fetch.
          node2.succeed(f"nix store delete {test_store_path} 2>&1 || true")

          node2.succeed(
              "nix copy "
              "--from http://localhost:8080 "
              f"--extra-trusted-public-keys '{cache_public_key}' "
              f"{test_store_path} "
              "2>&1"
          )
          node2.succeed(f"test -f {test_store_path}/data")

          print("All ncro P2P discovery tests passed.")
    '';
  }
