# Trust

ncro can optionally verify the provenance of the narinfo metadata it serves. The
system is experimental and therefore trust is _off by default_. ncro is a
_routing optimizer_, all things considered, and the trust layer is an opt-in
guard for deployments that do not want to blindly forward whatever an upstream
returns.

This document describes the model end to end: what is verified, what is _not_,
how a quorum is counted, how `fail_closed` behaves, and how the mesh lets a
quorum form across several nodes.

## Trust-Check Coverage

Nix binary caches sign narinfos with an ed25519 key. The signature is computed
over a _fingerprint_:

```plaintext
1;<StorePath>;<NarHash>;<NarSize>;<comma-separated References>
```

So in essence a valid signature cryptographically binds the **store path, the
NAR hash, the NAR size, and the references** to a signing key you trust. It does
**not** cover `FileHash`, `FileSize`, `Deriver`, `CA`, `Compression`, or `URL`.
Those travel in the narinfo but are outside the signed fingerprint.

> [!NOTE]
> ncro verifies this signature against the `public_key` configured on the
> upstream that served the narinfo (Nix `name:base64(key)` format). The
> verification logic lives in [`crates/narinfo`](../crates/narinfo/src/lib.rs);
> the policy that consumes it lives in
> [`crates/router`](../crates/router/src/lib.rs).

### Why Not Hash the NAR Bytes?

A reasonable question is _"why not also verify the bytes streamed for
`/nar/*.nar`?"_ The answer is relatively simple: because it would not add a
cryptographic guarantee and would produce false alarms:

- The signed `NarHash` is over the **uncompressed** NAR. The bytes ncro streams
  are the **compressed** file, whose digest is `FileHash`. A"nd `FileHash` is
  _not_ part of the signed fingerprint. Comparing streamed bytes to `FileHash`
  proves nothing about trust.
- Verifying the signed `NarHash` would require decompressing every stream
  (xz/zstd/...) in flight, which defeats ncro's zero-local-storage streaming
  design (see [architecture](architecture.md)).

So content trust stops at the signed metadata. If you need end-to-end NAR
content verification, Nix itself already does it on the client: the daemon
checks the NAR hash of every path it imports against the (signed) narinfo. ncro
sitting in front of Nix does not weaken that; a tampered NAR still fails in the
Nix daemon.

## Modes

`trust.mode` selects the policy. It is evaluated at the same acceptance point as
per-upstream filters: after a candidate narinfo is fetched but before the route
is cached.

<!--markdownlint-disable MD013-->

| Mode     | Requirement to accept a candidate                                               |
| -------- | ------------------------------------------------------------------------------- |
| `off`    | No verification. Route-only behavior (default).                                 |
| `signed` | The narinfo must verify against one of the serving upstream's configured signer keys. |
| `quorum` | A verified signature **plus** at least `threshold` matching claims (below).     |

<!--markdownlint-enable MD013-->

A candidate that fails the policy is treated exactly like a filter rejection:
the route is not cached and ncro keeps trying the remaining candidates.

## Claims

Every time ncro verifies a signature it records a **trust claim** in SQLite (see
`TrustClaim` in [`crates/db`](../crates/db/src/lib.rs)). A claim captures who
vouched for what:

- `signer_key` / `signer_name` -> the Nix signing key that verified.
- `upstream_url` -> the serving upstream for a local claim, or the
  `mesh://<relay-address>` that supplied a relayed claim.
- the signed content tuple: `nar_hash`, `nar_size`, `references` (and the
  `store_path` / `narinfo_hash`).
- provenance-only fields recorded for audit but **not** covered by the
  signature: `deriver`, `ca`, `file_hash`, `file_size`, and the raw `narinfo`
  bytes.

Claims are what make `quorum` and the mesh integration possible: they are a
durable, queryable record that signer X attests that path H has this NAR hash.
For relayed claims, the relay address is recorded rather than an unverified
assertion about the original upstream.

## Quorum counting

In `quorum` mode a route is accepted only once `threshold` claims **agree on the
same content tuple**, which is identical `nar_hash`, `nar_size`, and
`references`. This defends against a single compromised or buggy cache: an
attacker would have to get the same forged content signed by enough independent
keys.

`trust.require_distinct_signers` (default `true`) decides what "enough" means:

- `true` - count **distinct `signer_key`s**. The same key seen via two upstream
  URLs counts once. This is the meaningful setting: it counts independent
  signers, not independent mirrors.
- `false` - count claims regardless of signer (rarely what you want).

### The Trusted Signer Set

Counting _distinct keys_ is only meaningful if the keys are restricted to ones
you trust. Anyone can generate an ed25519 keypair and validly sign forged
content under it, so if every validly-self-signed key counted, an attacker could
mint as many distinct keys as the threshold requires and manufacture a quorum.
The signature check alone would be theatre.

ncro therefore counts a claim toward a quorum **only if its `signer_key` is in
the trusted set**:

$$\text{trusted set} = \left\{ \text{all configured upstream and enabled fallback signing keys} \right\} \cup
  \text{trust.trusted\_keys}$$

- Locally-recorded claims always use a configured upstream key, so a single node
  racing several independently-signed upstreams needs no extra config.
- For a **mesh**, each node must additionally list the signer keys of the other
  nodes' trusted upstreams in `trust.trusted_keys`. Otherwise a relayed claim
  signed by an unknown key is dropped (and never even stored). This is what
  bounds a quorum to _trusted_ signers rather than _any_ signer.

> [!WARNING]
> Quorum needs more than one independent signer to ever see the same path. On a
> single node that only races caches sharing one signing key, a `threshold > 1`
> can never be met. Either configure multiple independently-signed upstreams, or
> use the mesh (below) so peers contribute claims.

## `fail_closed`

`trust.fail_closed` (default `true`) decides what happens when the policy is not
satisfied:

- `true` (**closed**) -> the candidate is rejected. If no candidate satisfies
  the policy, the request ends in a 404 rather than serving unverified content.
- `false` (**open**) -> the content is served anyway, but the bypass is made
  observable: ncro logs a warning and increments
  `ncro_trust_bypass_total{reason="unsigned"|"below_quorum"}`. Use this to roll
  trust out in "report-only" mode and watch the metric before enforcing.

A non-zero `ncro_trust_bypass_total` means trust is advisory, not enforced.

## Trust Over The Mesh

When `trust.mode = "quorum"`, a lone node usually cannot reach a quorum because
it only sees the upstreams configured on it. The mesh closes this gap.

With `mesh.gossip_trust_claims = true`, a node:

1. **Gossips** the trust claims it has verified locally to its peers, alongside
   the route gossip it already sends, over the same ed25519-signed UDP packets.
   Claims are batched to stay within the UDP packet-size limit; a claim too
   large to fit on its own is logged and skipped rather than dropped silently.
2. **Receives** claims relayed by peers and applies two gates before storing
   each one:
   - **Trusted signer.** The claim's `signer_key` must be in this node's trusted
     set (above). A claim from any other key is dropped immediately, this is
     what stops an attacker from minting throwaway keys to forge a quorum, and
     it also bounds how many claims a hostile peer can write to the database.
   - **Valid signature and canonical fields.** The claim carries the original
     narinfo bytes. The receiver re-verifies that narinfo against the claim's
     `signer_key` and rebuilds every signed field from it, so a peer cannot
     substitute a different path or content tuple.

This means **a peer becomes a witness, never a substitute for a real Nix
signature.** A malicious or compromised peer cannot inflate a quorum: a claim
signed by an untrusted key is dropped, and a claim claiming a trusted key but
without a valid signature fails re-verification. A quorum therefore counts
distinct _trusted Nix signers_, whether those signatures were observed locally
or relayed by a peer. Configure `mesh.peers[].public_key` to also allowlist mesh
senders; peers without configured keys are accepted at the transport layer but
still cannot alter the signed claim tuple.

## Inspecting Trust (At Runtime)

The trust endpoint exposes the recorded claims:

- `GET /trust/<hash>.narinfo` - the current trust decision for a path: the mode,
  threshold, the count of matching claims, whether the path is currently
  trusted, and the full list of claims.
It is useful for confirming that claims are propagating across a mesh: after a
path is resolved on one node, it should appear in `/trust` on a peer within a
gossip interval or two.

## Configuration Summary

```toml
[trust]
mode                     = "quorum" # off | signed | quorum
threshold                = 2        # matching claims required in quorum mode
require_distinct_signers = true     # count distinct signer keys, not mirrors
fail_closed              = true     # reject (true) vs serve+warn (false)

# Signer keys trusted to vouch in quorum mode, in addition to upstream keys.
# In a mesh, list the OTHER nodes' upstream signer keys here.
trusted_keys = ["cache-b-1:bbbb...", "cache-c-1:cccc..."]

[mesh]
enabled             = true
gossip_trust_claims = true # gossip + re-verify claims across peers
peers               = [{ addr = "10.0.0.2:7946", public_key = "<hex ed25519>" }]
```
