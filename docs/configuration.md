# Configuration Reference

[`config.example.toml`]: ../config.example.toml

ncro reads TOML from the file passed with `--config`. In a configuration file,
`[[upstreams]]` must contain at least one entry; every other section and setting
is optional and uses the defaults below. With no configuration file, ncro
listens on `localhost:8080` and uses `https://cache.nixos.org` as its only
upstream.

Start with one upstream, point Nix at ncro, then add routing, authentication, or
network features only when they solve a concrete deployment need. See
[`config.example.toml`] for a complete, annotated file that you may use as
reference.

## Minimal configuration

```toml
[server]
listen = "localhost:8080"

[[upstreams]]
url = "https://cache.nixos.org"
priority = 10

[cache]
db_path = "/var/lib/ncro/routes.db"
```

[installation guide]: ./install.md

At least one `[[upstreams]]` entry is required. Configure the corresponding
public keys in Nix's `trusted-public-keys`; the NixOS module can add configured
upstream keys automatically. The [installation guide] shows both NixOS and
non-NixOS setups.

## Configuration loading and environment overrides

Use `ncro --config /etc/ncro/config.toml` to select a file. ncro also applies
these non-empty environment variables after reading that file:

| Variable         | Overrides       |
| ---------------- | --------------- |
| `NCRO_LISTEN`    | `server.listen` |
| `NCRO_DB_PATH`   | `cache.db_path` |
| `NCRO_LOG_LEVEL` | `logging.level` |

Unknown keys are an error. `ncro --config <file> --check` parses and validates a
file and exits, without reading passwords or applying the overrides above, so it
can run inside a build sandbox.

## Server

`[server]` controls ncro's listener, client-facing timeouts, and the cache
capabilities advertised at `/nix-cache-info`.

<!--markdownlint-disable MD013-->

| Key               | Default            | Meaning                                                                                                                                                |
| ----------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `listen`          | `"localhost:8080"` | TCP address to bind. `localhost:PORT` binds `127.0.0.1` and `::1`, `:PORT` binds every interface over IPv4 and IPv6, and `IP:PORT` binds that address. |
| `read_timeout`    | `"30s"`            | Maximum time spent reading a client request body.                                                                                                      |
| `write_timeout`   | `"30s"`            | Maximum time allowed to write a response body to a client.                                                                                             |
| `cache_priority`  | `30`               | Positive `Priority` advertised to Nix; lower values are preferred by Nix when it compares caches.                                                      |
| `want_mass_query` | `true`             | Advertise `WantMassQuery: 1`. Set false to discourage Nix from making bulk narinfo queries.                                                            |

<!--markdownlint-enable MD013-->

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

<!--markdownlint-disable MD013-->

| Key               | Default               | Meaning                                                                                                                                                                   |
| ----------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`             | required              | Base HTTP(S) cache URL or a Nix-style `s3://` URL.                                                                                                                        |
| `priority`        | `0`                   | Latency-tie preference; lower is preferred.                                                                                                                               |
| `public_key`      | empty                 | One accepted Nix narinfo signing key. Empty disables signature verification unless `public_keys` is set.                                                                  |
| `public_keys`     | `[]`                  | Additional accepted signing keys. Use this for pull-through caches that can return narinfos signed by multiple origins.                                                   |
| `username`        | empty                 | HTTP Basic Auth username. An empty username disables configured Basic Auth.                                                                                               |
| `password`        | unset                 | HTTP Basic Auth password. Mutually exclusive with `password_file`.                                                                                                        |
| `password_file`   | unset                 | File containing the HTTP Basic Auth password. One trailing newline is removed.                                                                                            |
| `narinfo_timeout` | `"5s"`                | Timeout for this upstream's narinfo HEAD race and GET fetches.                                                                                                            |
| `nar_timeout`     | `server.read_timeout` | Time allowed to receive successful NAR response headers and the first body chunk. Streaming continues after this deadline.                                                |
| `allow_hedging`   | `true`                | Permit additional NAR hedge, throughput-selection, probe, and recovery requests. Set false for metered or residential sources; it may still be the routed initial source. |
| `nar_url_mode`    | `"to_self"`           | How the narinfo `URL:` field is returned; described below.                                                                                                                |
| `filters`         | `[]`                  | Narinfo path filters, described below.                                                                                                                                    |

<!--markdownlint-enable MD013-->

`url` must be valid. `password` and `password_file` cannot both be set. When a
key is configured, it must be valid Nix `name:base64` syntax.

### Nar URL mode

`nar_url_mode` changes the `URL:` line returned in a narinfo:

<!--markdownlint-disable MD013-->

| Value           | Behavior                                                                                                     |
| --------------- | ------------------------------------------------------------------------------------------------------------ |
| `"to_self"`     | Default. Rewrite the URL to ncro's canonical `nar/<store-hash>` path, so NAR traffic continues through ncro. |
| `"to_upstream"` | Rewrite it to an absolute URL at the selected upstream; NAR traffic bypasses ncro.                           |
| `"keep"`        | Return the upstream's URL unchanged.                                                                         |

<!--markdownlint-enable MD013-->

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
Basic Auth or netrc. ncro refuses to start when the file `NETRC` names cannot be
read or parsed, so a wrong path or permission shows up at startup instead of as
failed authentication later. A broken `~/.netrc` is skipped with a warning.

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

<!--markdownlint-disable MD013-->

| Query parameter    | Default     | Meaning                                                                                          |
| ------------------ | ----------- | ------------------------------------------------------------------------------------------------ |
| `endpoint`         | unset       | Custom S3-compatible host.                                                                       |
| `scheme`           | `https`     | `http` or `https` for a custom endpoint.                                                         |
| `region`           | `us-east-1` | AWS region.                                                                                      |
| `profile`          | unset       | AWS shared-config profile.                                                                       |
| `addressing-style` | `auto`      | `auto`, `path`, or `virtual`. Auto uses path style for custom endpoints and dotted bucket names. |

<!--markdownlint-enable MD013-->

## Route cache and request concurrency

`[cache]` stores routing metadata in SQLite; ncro never stores NAR bodies on
disk.

<!--markdownlint-disable MD013-->

| Key                        | Default                     | Meaning                                                                                             |
| -------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------- |
| `db_path`                  | `"/var/lib/ncro/routes.db"` | SQLite route database path.                                                                         |
| `max_entries`              | `100000`                    | Maximum route entries before LRU eviction. Must be positive.                                        |
| `ttl`                      | `"1h"`                      | Lifetime of a successful route. Must be positive.                                                   |
| `negative_ttl`             | `"10m"`                     | How long a not-found result is remembered. Must be positive.                                        |
| `latency_alpha`            | `0.3`                       | EMA smoothing factor for probe latency, strictly between 0 and 1. Higher values react more quickly. |
| `slow_statement_threshold` | `"1s"`                      | SQLite statement duration at which ncro logs a slow-statement warning.                              |

<!--markdownlint-enable MD013-->

`[cache.mass_query]` limits the work created by concurrent narinfo lookups:

<!--markdownlint-disable MD013-->

| Key                         | Default | Meaning                                                                                        |
| --------------------------- | ------- | ---------------------------------------------------------------------------------------------- |
| `max_concurrent_races`      | `64`    | Total simultaneous narinfo upstream races. Must be at least 1.                                 |
| `per_upstream_max_inflight` | `8`     | Maximum in-flight narinfo requests per upstream. Must be at least 1.                           |
| `in_memory_negative_ttl`    | `"5s"`  | Short in-memory suppression window for repeated misses. Must be positive.                      |
| `upstream_cooldown`         | `"15s"` | How long a transient upstream network error excludes it from a narinfo race. Must be positive. |

<!--markdownlint-enable MD013-->

### NAR hedging

`[cache.nar_hedging]` overlaps NAR requests when the initial source has not
delivered successful headers and a first body chunk. It is enabled by default. A
winner is streamed immediately after that first chunk and all loser requests are
cancelled. `HEAD` NAR requests race all eligible normal upstreams immediately.
The fallback cache is never a hedge candidate.

<!--markdownlint-disable MD013-->

| Key            | Default | Meaning                                                                                          |
| -------------- | ------- | ------------------------------------------------------------------------------------------------ |
| `enabled`      | `true`  | Enable delayed NAR hedging.                                                                      |
| `delay`        | `"1s"`  | Delay before each additional candidate is started. Must be positive.                             |
| `max_inflight` | `2`     | Maximum overlapping normal-source NAR attempts. `1` disables overlap; values must be at least 1. |

<!--markdownlint-enable MD013-->

Additional candidates use global priority groups, then fresh NAR transfer
estimates when available, with latency as the fallback. `allow_hedging = false`
excludes an upstream from additional requests, including adaptive selection and
mid-stream recovery. NAR winners do not replace the narinfo route in SQLite.

### Adaptive NAR transfers

`[cache.nar_transfer]` learns from actual HTTP and S3 NAR transfers and
supervises normal-source response bodies. It is enabled by default and
independent of first-byte hedging. Disabling it restores first-byte-only
streaming; `max_switches = 0` retains measurement and source selection but
disables mid-stream probes and recovery.

Within each priority group, transfers at least `sample_min_bytes` in size prefer
sources with fresh measurements and rank them by measured first-byte time plus
remaining **compressed** bytes divided by observed throughput. Unknown sources
retain cached-route/latency ordering and can be explored by hedging or a bounded
slow-stream probe. Client ranges use their requested length, not the whole file
size. Narinfo lookup races remain latency-based.

Measurement windows require both minimum bytes and minimum actively awaited
upstream time. Time between downstream body polls is not included; bounded
streaming does not prefetch entire files. Active requests conservatively damp
estimates under increased concurrency. Statistics are in memory, expire, and are
capped at 1,024 upstreams; they are not persisted or mesh-gossiped.

<!--markdownlint-disable MD013-->

| Key                   | Default   | Meaning                                                                                                                  |
| --------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------ |
| `enabled`             | `true`    | Enable passive measurement, adaptive selection, and supervised streams.                                                  |
| `throughput_alpha`    | `0.3`     | EWMA weighting; finite and strictly between 0 and 1.                                                                     |
| `sample_min_bytes`    | `262144`  | Minimum bytes per sustained measurement window; positive.                                                                |
| `sample_min_duration` | `"250ms"` | Minimum active upstream time per measurement window; positive.                                                           |
| `stats_ttl`           | `"5m"`    | Maximum age of usable throughput and first-byte observations; positive.                                                  |
| `stall_timeout`       | `"5s"`    | No-progress deadline while actively awaiting a body chunk; positive.                                                     |
| `slow_window`         | `"3s"`    | Sustained active-read window before considering a slow-source probe; positive.                                           |
| `switch_margin`       | `"1s"`    | Minimum predicted completion-time saving, including discovery/setup/probe time; positive.                                |
| `min_improvement`     | `0.25`    | Also require this fractional reduction in predicted remaining completion time; finite and strictly between 0 and 1.      |
| `max_switches`        | `2`       | Per-download upper bound on discovery rounds and alternate NAR request attempts, including failed/rejected probes; 0–16. |
| `probe_bytes`         | `262144`  | Maximum prefix sampled from a slow-stream replacement; 1–16777216 bytes.                                                 |
| `probe_timeout`       | `"2s"`    | Deadline for a discovery round and each replacement/probe attempt; positive.                                             |

<!--markdownlint-enable MD013-->

On a stall, body error, or premature EOF, ncro discovers eligible sources again
and requests the exact remaining absolute byte range. A replacement requires
matching store path, compression, nonempty compressed `FileHash`, positive
`FileSize`, `NarHash`, and `NarSize`. It must return `206` with the exact
`Content-Range` and compatible length. Matching uncompressed NAR hashes alone is
insufficient; different compressed representations cannot be spliced.
Unsupported, multipart, or unverifiable ranges cannot be resumed. HTTP content
encoding must be absent or identity; ncro requests identity encoding.

Slow-stream probing pauses the current byte cursor and reads a bounded prefix
from an alternate's remaining-range response. A worthwhile candidate retains
that prefix and the same connection; otherwise ncro cancels the probe and
continues the original body. The application buffer holds at most `probe_bytes`
plus one transport chunk, with a 256-chunk prefix cap. Each source is attempted
at most once after the initial source, and both absolute and fractional saving
thresholds discourage churn. There is no parallel chunk striping or disk cache.

The original downstream status, length, and range remain unchanged. An exhausted
or unsafe recovery ends the body with an error rather than emitting unvalidated
alternate bytes; Nix still performs its normal integrity verification. Client
disconnects drop the original stream and any active probe. The fallback cache is
neither measured nor used for adaptive switching. Provenance headers describe
the initial source; subsequent switches are recorded in ncro's logs.

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

<!--markdownlint-disable MD013-->

| Key              | Default             | Meaning                                                                                       |
| ---------------- | ------------------- | --------------------------------------------------------------------------------------------- |
| `enabled`        | `false`             | Enable mDNS discovery.                                                                        |
| `service_name`   | `"_nix-serve._tcp"` | Service type to browse. Required when enabled.                                                |
| `domain`         | `"local"`           | mDNS domain. Required when enabled.                                                           |
| `discovery_time` | `"5s"`              | How long each discovery cycle listens. Must be positive when enabled.                         |
| `priority`       | `20`                | Priority assigned to discovered upstreams.                                                    |
| `address_family` | `"any"`             | `any`, `ipv4`, or `ipv6`. `any` registers all routable addresses so the router can race them. |

<!--markdownlint-enable MD013-->

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

<!--markdownlint-disable MD013-->

| Key               | Default          | Meaning                                                                       |
| ----------------- | ---------------- | ----------------------------------------------------------------------------- |
| `enabled`         | `false`          | Enable mesh gossip. At least one peer is required when enabled.               |
| `bind_addr`       | `"0.0.0.0:7946"` | UDP address on which to receive gossip.                                       |
| `private_key`     | empty            | Path to an ed25519 private key. Empty uses an ephemeral identity.             |
| `gossip_interval` | `"30s"`          | Interval between route announcements.                                         |
| `peers`           | `[]`             | Peer entries, each with required `addr` and hex-encoded 32-byte `public_key`. |

<!--markdownlint-enable MD013-->

Generate a persistent key with
`ncro --generate-mesh-key /var/lib/ncro/node.key`.

## Logging and response provenance

`[logging]` controls tracing output:

<!--markdownlint-disable MD013-->

| Key          | Default  | Meaning                                                                                                         |
| ------------ | -------- | --------------------------------------------------------------------------------------------------------------- |
| `level`      | `"info"` | A tracing `EnvFilter` directive, for example `"debug"` or `"ncro=debug,tower_http=warn"`. It must not be empty. |
| `format`     | `"json"` | `json` or `text`.                                                                                               |
| `timestamps` | `true`   | Include timestamps in ncro's log output. Disable under journald if its timestamps are sufficient.               |

<!--markdownlint-enable MD013-->

Successful narinfo and NAR responses also include diagnostic headers:

<!--markdownlint-disable MD013-->

| Header            | Meaning                                                                                       |
| ----------------- | --------------------------------------------------------------------------------------------- |
| `X-Ncro-Upstream` | Hostname of the selected upstream.                                                            |
| `X-Ncro-Route`    | How ncro selected the upstream. One of `cache-hit`, `race`, `direct`, `hedge`, or `fallback`. |

<!--markdownlint-enable MD013-->

These headers do not change the substituter URL Nix displays; it continues to
show ncro's configured URL. They disclose the upstream hostname to clients, so
do not expose ncro to untrusted clients if that is sensitive.
