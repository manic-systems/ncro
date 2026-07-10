# Configuration Reference

The most important settings are `upstreams`, `fallback_cache.enabled`,
`server.listen`, `cache.db_path`, `cache.ttl`, `cache.negative_ttl`,
`cache.latency_alpha`, `server.cache_priority`, `discovery.enabled`,
`discovery.address_family`, and `mesh.enabled`.

`upstreams` defines the cache backends ncro can use. Each upstream can carry a
`priority` value, optional Basic Auth credentials, an optional `public_key` for
narinfo signature verification, and optional `filters`. Use `password_file`
instead of `password` when the upstream Basic Auth password should be read from
a secret file; one trailing newline is ignored, and `password` plus
`password_file` is rejected.

Upstream filters support `allow` and `deny` rules over narinfo fields. The
available fields are `name`, `store_path`, `reference`, and `deriver`; patterns
use `*` wildcards. Deny rules always win. If any allow rules are configured for
an upstream, at least one must match for that upstream to be accepted.

`cache.ttl` is how long a successful routing decision remains trusted. The
negative TTL applies to failed lookups so ncro does not immediately retry the
same miss.

`cache.latency_alpha` controls how quickly EMA latency reacts to new probes. A
smaller value smooths jitter; a larger value reacts faster to recent changes.

`server.cache_priority` is used when the server layer needs to compare cache
responses. It should stay positive.

`discovery.enabled` and `mesh.enabled` turn on the optional network-coordination
paths described above. Discovery is opportunistic; mesh is signed and intended
for trusted peers.

`discovery.address_family` controls which addresses from an mDNS-discovered peer
are registered as upstreams. The default `any` registers all routable addresses
(IPv4 and IPv6) so the race engine can try them in parallel. Set `ipv4` or
`ipv6` when the upstream server only listens on one address family.

`fallback_cache` defines an optional last-resort backend. It is disabled by
default and defaults to `https://cache.nixos.org` when enabled. The fallback
cache accepts the same connection fields as an upstream (`url`, `public_key`,
`username`, `password`, `password_file`, and `s3://` URLs), but it is not part
of normal upstream routing. It is only used after normal candidates are
unavailable and its responses are not persisted as route winners.

`logging.level` is a tracing filter directive. Use a single level such as
`debug`, `info`, `warn`, or `error` for global filtering, or a directive list
such as `ncro=debug,tower_http=warn` to tune specific modules. `logging.format`
accepts `json` or `text`. `logging.timestamps` defaults to `true`; set it to
`false` when a supervisor such as systemd/journald already records timestamps.
