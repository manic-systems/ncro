{
  pkgs,
  self,
}: let
  # Two deterministic payloads. Each is built identically wherever it appears,
  # so a given payload has the same store path, NAR hash, NAR size, and
  # references on every node. Two nodes signing the *same* payload with
  # different keys therefore produce claims over the same content tuple, which
  # is exactly what a quorum counts.
  #
  # payload1 is the legitimate artifact served by node1 and node2.
  # payload2 is only ever served by the attacker node.
  payload1 = pkgs.runCommand "ncro-mesh-trust-payload1" {} ''
    mkdir -p "$out"
    echo "ncro mesh trust legitimate payload" > "$out/data"
  '';
  payload2 = pkgs.runCommand "ncro-mesh-trust-payload2" {} ''
    mkdir -p "$out"
    echo "ncro mesh trust attacker payload" > "$out/data"
  '';

  # Fixed Nix binary-cache key pairs, generated offline with
  # `nix-store --generate-binary-cache-key`, embedded so the test is fully
  # deterministic. A and B are the two legitimate signers; C is an attacker key
  # that no node trusts.
  secretKeyA = "ncro-mesh-a-1:m1ZXX1CzD1Z6bKrkFMhKK2oJp0G8cJJ/lVFtwt41b9dEbFtwAit6sKmpGJHf6NV3M44Mte7dFXvcT8+rGlzPNg==";
  publicKeyA = "ncro-mesh-a-1:RGxbcAIrerCpqRiR3+jVdzOODLXu3RV73E/PqxpczzY=";
  secretKeyB = "ncro-mesh-b-1:fFG4duKzMAMfn+TZ3WLlOIKIBuClekB5+DNYhXFhMXFuVKfcgHlKGWsNU/0Ot3yAdMfI8pMLKVpVWkRxabU2QQ==";
  publicKeyB = "ncro-mesh-b-1:blSn3IB5ShlrDVP9Drd8gHTHyPKTCylaVVpEcWm1NkE=";
  secretKeyC = "ncro-mesh-evil-1:1cB1EK64GyfEeozRh8b/uEDSrmh8qzxwa5pFdsdx7HdjIUhWt7AeIemVrTJXMIoocvZvkpPgpO3zdlUq5uP76A==";

  meshPort = 7946;

  # Peers are addressed by IP rather than hostname: the ncro unit is hardened
  # (no AF_UNIX), so glibc cannot reach nscd to resolve names, and the
  # documented mesh config uses `IP:port` anyway.
  #
  # The test framework assigns 192.168.1.<index> on eth1 by the *alphabetically
  # sorted* node name, so the order is: evil=.1, node1=.2, node2=.3. Only node1
  # and node2 are referenced as peers (evil only sends, and node1 accepts any
  # signed packet), so evil's own address never needs naming here.
  node1Ip = "192.168.1.2";
  node2Ip = "192.168.1.3";

  # Build a node that runs nix-serve (signing `payload` with `secretKey`) and
  # ncro in quorum mode, trusting `trustedKeys` and meshing with `peerIp`.
  mkNode = {
    secretKey,
    selfPublicKey,
    trustedKeys,
    payload,
    peerIp,
  }: {pkgs, ...}: {
    imports = [self.nixosModules.ncro];

    virtualisation.memorySize = 2048;
    virtualisation.diskSize = 8192;

    networking.useNetworkd = true;
    networking.firewall.enable = false;

    environment.systemPackages = [pkgs.curl pkgs.jq];

    nix.settings.experimental-features = ["nix-command"];

    # The payload must be in the local store so this node's nix-serve can serve
    # and sign it.
    system.extraDependencies = [payload];

    # nix-serve adds a Sig line to every narinfo it
    # serves using this key, so no separate `nix store sign` step is needed.
    environment.etc."ncro-mesh/cache.sec" = {
      text = secretKey;
      mode = "0400";
    };

    services.nix-serve = {
      enable = true;
      secretKeyFile = "/etc/ncro-mesh/cache.sec";
      port = 5000;
    };

    services.ncro = {
      enable = true;
      settings = {
        server.listen = ":8080";

        # This node's own nix-serve. With one upstream there is exactly one local
        # signer, so a quorum of two can ONLY be reached by importing a second
        # *trusted* signer's claim over the mesh.
        upstreams = [
          {
            url = "http://127.0.0.1:5000";
            priority = 1;
            public_key = selfPublicKey;
          }
        ];

        # Keep negative caching short: a quorum-rejected lookup is cached as a
        # miss, and we want that miss to expire before the post-quorum fetch and
        # between poke retries.
        cache = {
          ttl = "5m";
          negative_ttl = "1s";
          db_path = "/var/lib/ncro/routes.db";
          mass_query.in_memory_negative_ttl = "500ms";
        };

        trust = {
          mode = "quorum";
          threshold = 2;
          require_distinct_signers = true;
          fail_closed = true;
          # The signer keys whose claims may count toward a quorum. Without this
          # an attacker could self-sign forged content under throwaway keys and
          # manufacture agreement, so claims from any other key are dropped.
          trusted_keys = trustedKeys;
        };

        mesh = {
          enabled = true;
          bind_addr = "0.0.0.0:${toString meshPort}";
          gossip_interval = "3s";
          gossip_trust_claims = true;
          private_key = "/var/lib/ncro/node.key";

          # No peer public_key; the allowlist is empty, so any *signed* packet is
          # accepted at the transport layer. Relayed claims are still gated on
          # (a) a trusted signer key and (b) a valid narinfo signature, which is
          # the guarantee under test.
          peers = [{addr = "${peerIp}:${toString meshPort}";}];
        };
      };
    };
  };
in
  pkgs.testers.runNixOSTest {
    name = "ncro-mesh-trust";

    nodes = {
      # node1/node2: the legitimate quorum. Each trusts both A and B, so a claim
      # relayed from the other counts.
      # Indexes 1 and 2 -> node1Ip / node2Ip.
      node1 = mkNode {
        secretKey = secretKeyA;
        selfPublicKey = publicKeyA;
        trustedKeys = [publicKeyA publicKeyB];
        payload = payload1;
        peerIp = node2Ip;
      };
      node2 = mkNode {
        secretKey = secretKeyB;
        selfPublicKey = publicKeyB;
        trustedKeys = [publicKeyA publicKeyB];
        payload = payload1;
        peerIp = node1Ip;
      };

      # An attacker that serves a *different* payload signed with an untrusted key (C)
      # and relays the resulting claim to node1. node1 must drop it.
      # Neither node1 nor node2 lists C in trusted_keys.
      # XXX: I wanted to name this node '>:C'
      evil = mkNode {
        secretKey = secretKeyC;
        selfPublicKey = "ncro-mesh-evil-1:YyFIVrewHiHpla0yVzCKKHL2b5KT4KTt83ZVKubj++g=";
        trustedKeys = ["ncro-mesh-evil-1:YyFIVrewHiHpla0yVzCKKHL2b5KT4KTt83ZVKubj++g="];
        payload = payload2;
        peerIp = node1Ip;
      };
    };

    testScript = ''
      import json

      hash1 = "${payload1}".split("/")[3].split("-")[0]
      hash2 = "${payload2}".split("/")[3].split("-")[0]

      def trust(node, store_hash):
          """Parsed /trust JSON for a store hash on the given node."""
          out = node.succeed(f"curl -sf http://localhost:8080/trust/{store_hash}.narinfo")
          return json.loads(out)

      def poke_until(node, store_hash, jq_cond, timeout=60):
          """
          Repeatedly request the narinfo (so ncro fetches it from the local
          nix-serve, verifies the signature, and records a claim) until the
          /trust JSON satisfies jq_cond. Retrying absorbs nix-serve's cold
          PreFork startup, where the first request can fail to connect.
          """
          node.wait_until_succeeds(
              f"curl -s -o /dev/null http://localhost:8080/{store_hash}.narinfo; "
              f"curl -sf http://localhost:8080/trust/{store_hash}.narinfo | jq -e '{jq_cond}'",
              timeout=timeout,
          )

      def wait_until_trusted(node, store_hash, timeout=90):
          node.wait_until_succeeds(
              f"curl -sf http://localhost:8080/trust/{store_hash}.narinfo "
              "| jq -e '.trusted == true and .matching_claims >= 2'",
              timeout=timeout,
          )

      with subtest("boot all nodes"):
          start_all()
          for node in (node1, node2, evil):
              node.wait_for_unit("nix-serve.service")
              node.wait_for_unit("ncro.service")
              node.wait_for_open_port(5000)  # nix-serve
              node.wait_for_open_port(8080)  # ncro HTTP
          for node in (node1, node2, evil):
              node.wait_until_succeeds(
                  "journalctl -u ncro --no-pager | grep -q 'mesh node identity'"
              )

      with subtest("a single trusted signer cannot satisfy a quorum of two"):
          # node1 records its own claim (signer A). node2 has not observed the
          # path yet, so it has no claim B to relay, and node1 stays at one
          # matching claim and is not trusted.
          poke_until(node1, hash1, ".matching_claims >= 1")
          status1 = trust(node1, hash1)
          assert status1["mode"] == "quorum", f"unexpected mode: {status1!r}"
          assert status1["matching_claims"] == 1, \
              f"node1 should have exactly its own claim: {status1!r}"
          assert status1["trusted"] is False, \
              f"node1 must not be trusted with a single signer: {status1!r}"

      with subtest("quorum forms across the mesh once a second signer is seen"):
          # node2 observes the same payload from its own nix-serve, recording a
          # claim from a distinct trusted signer (B). Both nodes gossip their
          # verified claims; each re-verifies the relayed claim against its
          # original Nix signer key before storing it. After convergence both
          # hold two distinct trusted-signer claims for the same content tuple.
          poke_until(node2, hash1, ".matching_claims >= 1")
          wait_until_trusted(node1, hash1)
          wait_until_trusted(node2, hash1)

          for node, name in ((node1, "node1"), (node2, "node2")):
              status = trust(node, hash1)
              signers = {c["signer_key"] for c in status["claims"]}
              assert "${publicKeyA}" in signers, f"{name}: missing signer A: {signers!r}"
              assert "${publicKeyB}" in signers, f"{name}: missing signer B: {signers!r}"

      with subtest("a quorum from untrusted keys is rejected (not security theatre)"):
          # The attacker serves payload2 signed with key C and gossips the claim
          # to node1. C is in nobody's trusted_keys, so node1 must drop it: the
          # claim never reaches node1's store and payload2 is never trusted.
          # XXX: This is the property that makes distinct-signer quorum meaningful. Without
          # the trusted-key gate, an attacker could mint unlimited distinct keys and forge
          # agreement.
          poke_until(evil, hash2, ".claims | length >= 1")

          # Prove the attacker's gossip actually reached node1 and was rejected
          # for the right reason, rather than the test passing because nothing
          # arrived.
          node1.wait_until_succeeds(
              "journalctl -u ncro --no-pager "
              "| grep -q 'rejecting relayed trust claim from untrusted signer key'",
              timeout=60,
          )

          # Give any (wrongly accepted) claim ample time to land, then assert it
          # => node1 holds no claim for payload2 and does not trust it.
          node1.sleep(8)
          evil_status = trust(node1, hash2)
          assert evil_status["matching_claims"] == 0, \
              f"untrusted-key claims must not count: {evil_status!r}"
          assert evil_status["trusted"] is False, \
              f"payload2 must never be trusted on node1: {evil_status!r}"
          assert evil_status["claims"] == [], \
              f"untrusted-key claims must not be stored: {evil_status!r}"

      with subtest("a trusted, quorum-backed path is served end to end"):
          # The quorum-rejected miss has expired (negative_ttl=1s), so a fresh
          # request re-races, passes the quorum gate, and serves a signed
          # narinfo. Fetch the closure through ncro to prove it end to end.
          node1.succeed("nix store delete ${payload1} 2>/dev/null || true")
          narinfo = node1.succeed(f"curl -sf http://localhost:8080/{hash1}.narinfo")
          assert "Sig:" in narinfo, f"served narinfo is unsigned: {narinfo!r}"

          node1.succeed(
              "nix copy --from http://localhost:8080 "
              "--extra-trusted-public-keys '${publicKeyA} ${publicKeyB}' "
              "${payload1} 2>&1"
          )
          node1.succeed("test -f ${payload1}/data")
          node1.succeed("grep -q 'legitimate payload' ${payload1}/data")

          print("ncro mesh trust + malicious-rejection test passed.")
    '';
  }
