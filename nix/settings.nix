{lib}: let
  inherit (lib.options) mkOption;
  inherit (lib.types) addCheck bool either enum ints listOf nonEmptyStr nullOr number path str submodule;

  optional = type: description:
    mkOption {
      type = nullOr type;
      default = null;
      inherit description;
    };

  filePath = either str path;
  positiveU32 = ints.between 1 4294967295;

  upstreamOptions = {
    url = mkOption {
      type = nonEmptyStr;
      description = "Base HTTP(S) cache URL or Nix-style S3 URL.";
    };
    priority = optional ints.s32 "Latency tie preference, with lower values preferred.";
    public_key = optional str "Accepted Nix narinfo signing key.";
    public_keys = optional (listOf str) "Additional accepted narinfo signing keys.";
    username = optional str "HTTP Basic Auth username.";
    password = optional str "HTTP Basic Auth password. Mutually exclusive with password_file.";
    password_file = optional filePath "File containing the HTTP Basic Auth password.";
    narinfo_timeout = optional str "Timeout for narinfo probes and fetches.";
    nar_timeout = optional str "Read timeout while streaming NAR data.";
    allow_hedging = optional bool "Allow this upstream to serve additional NAR hedge requests.";
    nar_url_mode = optional (enum ["keep" "to_self" "to_upstream"]) "How to rewrite narinfo URLs.";
    filters = optional (listOf (submodule {
      options = {
        action = optional (enum ["allow" "deny"]) "Whether matching narinfos are allowed or denied.";
        field = optional (enum ["name" "store_path" "reference" "deriver"]) "Narinfo field to match.";
        pattern = mkOption {
          type = nonEmptyStr;
          description = "Pattern to match, with * as a wildcard.";
        };
      };
    })) "Narinfo path filters.";
  };
in {
  options = {
    server = optional (submodule {
      options = {
        listen = optional str "TCP listen address, including the :port shorthand.";
        read_timeout = optional str "Maximum time spent reading a client request body.";
        write_timeout = optional str "Maximum time allowed to write a client response body.";
        cache_priority = optional (ints.between 1 2147483647) "Cache priority advertised to Nix.";
        want_mass_query = optional bool "Advertise support for bulk narinfo queries.";
      };
    }) "Listener and client-facing settings.";

    upstreams = optional (listOf (submodule {options = upstreamOptions;})) "Upstream routing candidates.";

    fallback_cache = optional (submodule {
      options =
        upstreamOptions
        // {
          enabled = optional bool "Enable the fallback cache after normal upstreams are exhausted.";
          url = optional nonEmptyStr "Fallback cache URL. Defaults to https://cache.nixos.org.";
        };
    }) "Last-resort cache settings.";

    cache = optional (submodule {
      options = {
        db_path = optional filePath "SQLite route database path.";
        max_entries = optional ints.positive "Maximum route entries before eviction.";
        ttl = optional str "Lifetime of a successful route.";
        negative_ttl = optional str "Lifetime of a cached not-found result.";
        latency_alpha = optional (addCheck number (value: value > 0 && value < 1)) "Latency smoothing factor, strictly between 0 and 1.";
        slow_statement_threshold = optional str "SQLite statement duration that triggers a warning.";

        mass_query = optional (submodule {
          options = {
            max_concurrent_races = optional positiveU32 "Maximum simultaneous narinfo upstream races.";
            per_upstream_max_inflight = optional positiveU32 "Maximum in-flight narinfo requests per upstream.";
            in_memory_negative_ttl = optional str "In-memory suppression window for repeated misses.";
            upstream_cooldown = optional str "Time to exclude an upstream after a transient network error.";
          };
        }) "Narinfo request concurrency settings.";

        nar_hedging = optional (submodule {
          options = {
            enabled = optional bool "Enable delayed NAR hedging.";
            delay = optional str "Delay before starting each additional NAR candidate.";
            max_inflight = optional positiveU32 "Maximum overlapping NAR attempts.";
          };
        }) "NAR hedging settings.";
      };
    }) "Route cache and request concurrency settings.";

    mesh = optional (submodule {
      options = {
        enabled = optional bool "Enable signed route gossip between peers.";
        bind_addr = optional str "UDP address on which to receive gossip.";
        private_key = optional filePath "Ed25519 private key path. Empty uses an ephemeral identity.";
        gossip_interval = optional str "Interval between route announcements.";
        peers = optional (listOf (submodule {
          options = {
            addr = mkOption {
              type = nonEmptyStr;
              description = "Peer socket address.";
            };
            public_key = optional str "Hex-encoded Ed25519 peer public key.";
          };
        })) "Trusted mesh peers.";
      };
    }) "Mesh gossip settings.";

    discovery = optional (submodule {
      options = {
        enabled = optional bool "Enable mDNS cache discovery.";
        service_name = optional str "mDNS service type to browse.";
        domain = optional str "mDNS domain to browse.";
        discovery_time = optional str "Duration of each discovery cycle.";
        priority = optional ints.s32 "Priority assigned to discovered upstreams.";
        address_family = optional (enum ["any" "ipv4" "ipv6"]) "Address families to register for discovered caches.";
      };
    }) "Local cache discovery settings.";

    logging = optional (submodule {
      options = {
        level = optional nonEmptyStr "Tracing filter directive.";
        format = optional (enum ["json" "text"]) "Log output format.";
        timestamps = optional bool "Include timestamps in log output.";
      };
    }) "Logging settings.";
  };
}
