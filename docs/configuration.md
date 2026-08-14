# Configuration Reference

ncro reads TOML from the file passed with `--config`. In a configuration file,
`[[upstreams]]` must contain at least one entry; every other section and setting
is optional and uses the defaults below. With no configuration file, ncro
listens on `:8080` and uses `https://cache.nixos.org` as its only upstream.

Start with one upstream, point Nix at ncro, then add routing, authentication, or
network features only when they solve a concrete deployment need. See
[`config.example.toml`](../config.example.toml) for a complete annotated file.

## Minimal configuration

```toml
[server]
listen = ":8080"

[[upstreams]]
url = "https://cache.nixos.org"
priority = 10

[cache]
db_path = "/var/lib/ncro/routes.db"
```

At least one `[[upstreams]]` entry is required. Configure the corresponding
public keys in Nix's `trusted-public-keys`; the NixOS module can add configured
upstream keys automatically. The [installation guide](install.md) shows both
NixOS and non-NixOS setups.

## Configuration loading and environment overrides

Use `ncro --config /etc/ncro/config.toml` to select a file. ncro also applies
these non-empty environment variables after reading that file:

| Variable         | Overrides       |
| ---------------- | --------------- |
| `NCRO_LISTEN`    | `server.listen` |
| `NCRO_DB_PATH`   | `cache.db_path` |
| `NCRO_LOG_LEVEL` | `logging.level` |

## Server

`[server]` controls ncro's listener, client-facing timeouts, and the cache
capabilities advertised at `/nix-cache-info`.

| Key               | Default   | Meaning                                                                                           |
| ----------------- | --------- | ------------------------------------------------------------------------------------------------- |
| `listen`          | `":8080"` | TCP address to bind.                                                                              |
| `read_timeout`    | `"30s"`   | Maximum time spent reading a client request body.                                                 |
| `write_timeout`   | `"30s"`   | Maximum time allowed to write a response body to a client.                                        |
| `cache_priority`  | `30`      | Positive `Priority` advertised to Nix; lower values are preferred by Nix when it compares caches. |
| `want_mass_query` | `true`    | Advertise `WantMassQuery: 1`. Set false to discourage Nix from making bulk narinfo queries.       |

Durations use the human-readable syntax used throughout this document, such as
`"5s"`, `"10m"`, or `"1h"`.

## Upstreams

Each `[[upstreams]]` entry is a normal routing candidate. ncro races eligible
upstreams for narinfo requests, learns latency, and caches the winner's route.
For NAR requests it uses the recorded route and retries another upstream when
necessary. Lower `priority` wins only when latency is within the router's 10%
tie window; it is not a strict ordering mechanism.

```toml
[[upstreams]]
url = "https://cache.internal.example"
priority = 10
public_key = "cache.internal.example-1:..."
narinfo_timeout = "10s"
nar_timeout = "60s"
```

| Key               | Default               | Meaning                                                                                                                 |
| ----------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `url`             | required              | Base HTTP(S) cache URL or a Nix-style `s3://` URL.                                                                      |
| `priority`        | `0`                   | Latency-tie preference; lower is preferred.                                                                             |
| `public_key`      | empty                 | One accepted Nix narinfo signing key. Empty disables signature verification unless `public_keys` is set.                |
| `public_keys`     | `[]`                  | Additional accepted signing keys. Use this for pull-through caches that can return narinfos signed by multiple origins. |
| `username`        | empty                 | HTTP Basic Auth username. An empty username disables configured Basic Auth.                                             |
| `password`        | unset                 | HTTP Basic Auth password. Mutually exclusive with `password_file`.                                                      |
| `password_file`   | unset                 | File containing the HTTP Basic Auth password. One trailing newline is removed.                                          |
| `narinfo_timeout` | `"5s"`                | Timeout for this upstream's narinfo HEAD race and GET fetches.                                                          |
| `nar_timeout`     | `server.read_timeout` | Read timeout while streaming NAR data from this upstream.                                                               |
| `nar_url_mode`    | `"to_self"`           | How the narinfo `URL:` field is returned; described below.                                                              |
| `filters`         | `[]`                  | Narinfo path filters, described below.                                                                                  |

`url` must be valid. `password` and `password_file` cannot both be set. When a
key is configured, it must be valid Nix `name:base64` syntax.

### Nar URL mode

`nar_url_mode` changes the `URL:` line returned in a narinfo:

| Value           | Behavior                                                                                                     |
| --------------- | ------------------------------------------------------------------------------------------------------------ |
| `"to_self"`     | Default. Rewrite the URL to ncro's canonical `nar/<store-hash>` path, so NAR traffic continues through ncro. |
| `"to_upstream"` | Rewrite it to an absolute URL at the selected upstream; NAR traffic bypasses ncro.                           |
| `"keep"`        | Return the upstream's URL unchanged.                                                                         |

### Filters

Filters are evaluated after ncro fetches a candidate narinfo, since the request
only identifies the store hash and the fields to filter are in the narinfo. A
matching `deny` always rejects a candidate. If one or more `allow` rules are
configured, at least one allow rule must match for the candidate to be used.
Patterns use `*` as a wildcard.

```toml
[[upstreams]]
url = "https://cache.example"
priority = 100

[[upstreams.filters]]
action = "allow"
field = "name"
pattern = "my-project-*"

[[upstreams.filters]]
action = "deny"
field = "name"
pattern = "*-source"
```

| `field`      | Narinfo field matched                 |
| ------------ | ------------------------------------- |
| `name`       | Store path name after the hash.       |
| `store_path` | Full `/nix/store/<hash>-<name>` path. |
| `reference`  | Each entry in `References`.           |
| `deriver`    | `Deriver`.                            |

### HTTP authentication

For HTTP(S) upstreams, configure `username` with `password` or `password_file`
when ncro should send Basic Auth on probes, narinfo requests, and NAR requests.
Prefer `password_file` for secrets.

If those fields are empty, ncro also consults a netrc file: `NETRC` if set, or
`~/.netrc` otherwise. The `machine` name must match the upstream hostname; a
`default` entry is used when no machine entry matches. Explicit configuration
credentials take precedence over netrc credentials. S3 upstreams do not use
Basic Auth or netrc.

### S3 upstreams

Use a Nix-style URL for an S3 bucket or compatible service:

```toml
[[upstreams]]
url = "s3://my-bucket?endpoint=minio.example.com&scheme=https"
priority = 15
```

Credentials come from the standard AWS provider chain, including environment
variables, shared AWS config/credential files, an explicit profile, and instance
or task identity. `username` and `password` do not apply to S3.

| Query parameter    | Default     | Meaning                                                                                          |
| ------------------ | ----------- | ------------------------------------------------------------------------------------------------ |
| `endpoint`         | unset       | Custom S3-compatible host.                                                                       |
| `scheme`           | `https`     | `http` or `https` for a custom endpoint.                                                         |
| `region`           | `us-east-1` | AWS region.                                                                                      |
| `profile`          | unset       | AWS shared-config profile.                                                                       |
| `addressing-style` | `auto`      | `auto`, `path`, or `virtual`. Auto uses path style for custom endpoints and dotted bucket names. |

## Route cache and request concurrency

`[cache]` stores routing metadata in SQLite; ncro never stores NAR bodies on
disk.

| Key                        | Default                     | Meaning                                                                                             |
| -------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------- |
| `db_path`                  | `"/var/lib/ncro/routes.db"` | SQLite route database path.                                                                         |
| `max_entries`              | `100000`                    | Maximum route entries before LRU eviction. Must be positive.                                        |
| `ttl`                      | `"1h"`                      | Lifetime of a successful route. Must be positive.                                                   |
| `negative_ttl`             | `"10m"`                     | How long a not-found result is remembered. Must be positive.                                        |
| `latency_alpha`            | `0.3`                       | EMA smoothing factor for probe latency, strictly between 0 and 1. Higher values react more quickly. |
| `slow_statement_threshold` | `"1s"`                      | SQLite statement duration at which ncro logs a slow-statement warning.                              |

`[cache.mass_query]` limits the work created by concurrent narinfo lookups:

| Key                         | Default | Meaning                                                                                        |
| --------------------------- | ------- | ---------------------------------------------------------------------------------------------- |
| `max_concurrent_races`      | `64`    | Total simultaneous narinfo upstream races. Must be at least 1.                                 |
| `per_upstream_max_inflight` | `8`     | Maximum in-flight narinfo requests per upstream. Must be at least 1.                           |
| `in_memory_negative_ttl`    | `"5s"`  | Short in-memory suppression window for repeated misses. Must be positive.                      |
| `upstream_cooldown`         | `"15s"` | How long a transient upstream network error excludes it from a narinfo race. Must be positive. |

## Fallback cache

`[fallback_cache]` is a last-resort backend, disabled by default. It defaults to
`https://cache.nixos.org` when enabled.

```toml
[fallback_cache]
enabled = true
url = "https://cache.nixos.org"
public_key = "cache.nixos.org-1:..."
```

It accepts the same connection, signing-key, authentication, and S3 fields as an
upstream. It is intentionally _not_ a normal routing candidate: ncro does not
health probe it, apply priority, discovery, cooldowns, or filters to it, or
persist a successful fallback narinfo as a route winner. Use a regular
`[[upstreams]]` entry instead when the cache should participate in normal
routing.

## Discovery

`[discovery]` discovers local cache servers over mDNS and registers them as
dynamic upstreams. It is disabled by default.

| Key              | Default             | Meaning                                                                                       |
| ---------------- | ------------------- | --------------------------------------------------------------------------------------------- |
| `enabled`        | `false`             | Enable mDNS discovery.                                                                        |
| `service_name`   | `"_nix-serve._tcp"` | Service type to browse. Required when enabled.                                                |
| `domain`         | `"local"`           | mDNS domain. Required when enabled.                                                           |
| `discovery_time` | `"5s"`              | How long each discovery cycle listens. Must be positive when enabled.                         |
| `priority`       | `20`                | Priority assigned to discovered upstreams.                                                    |
| `address_family` | `"any"`             | `any`, `ipv4`, or `ipv6`. `any` registers all routable addresses so the router can race them. |

## Mesh

`[mesh]` gossips recent routing decisions between trusted ncro peers over signed
UDP. It is disabled by default and is intended for private networks.

```toml
[mesh]
enabled = true
bind_addr = "0.0.0.0:7946"
private_key = "/var/lib/ncro/node.key"
gossip_interval = "30s"

[[mesh.peers]]
addr = "100.64.1.2:7946"
public_key = "a1b2c3..." # 32-byte ed25519 key, hex encoded
```

| Key               | Default          | Meaning                                                                                |
| ----------------- | ---------------- | -------------------------------------------------------------------------------------- |
| `enabled`         | `false`          | Enable mesh gossip. At least one peer is required when enabled.                        |
| `bind_addr`       | `"0.0.0.0:7946"` | UDP address on which to receive gossip.                                                |
| `private_key`     | empty            | Path to an ed25519 private key. Empty uses an ephemeral identity.                      |
| `gossip_interval` | `"30s"`          | Interval between route announcements.                                                  |
| `peers`           | `[]`             | Peer entries, each with required `addr` and optional hex-encoded 32-byte `public_key`. |

Generate a persistent key with
`ncro --generate-mesh-key /var/lib/ncro/node.key`.

## Logging and response provenance

`[logging]` controls tracing output:

| Key          | Default  | Meaning                                                                                                         |
| ------------ | -------- | --------------------------------------------------------------------------------------------------------------- |
| `level`      | `"info"` | A tracing `EnvFilter` directive, for example `"debug"` or `"ncro=debug,tower_http=warn"`. It must not be empty. |
| `format`     | `"json"` | `json` or `text`.                                                                                               |
| `timestamps` | `true`   | Include timestamps in ncro's log output. Disable under journald if its timestamps are sufficient.               |

Successful narinfo and NAR responses also include diagnostic headers:

| Header            | Meaning                                                                      |
| ----------------- | ---------------------------------------------------------------------------- |
| `X-Ncro-Upstream` | Hostname of the selected upstream.                                           |
| `X-Ncro-Route`    | How ncro selected it: `cache-hit`, `race`, `direct`, `retry`, or `fallback`. |

These headers do not change the substituter URL Nix displays; it continues to
show ncro's configured URL. They disclose the upstream hostname to clients, so
do not expose ncro to untrusted clients if that is sensitive.
