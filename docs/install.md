# Installation and Setup

This document covers installation, configuration, and first-run setup.

## Building/Installing

### With Nix

Nix is the recommended way of downloading (and developing!) ncro. You can
install it using Nix flakes using `nix profile add` if on non-nixos or add ncro
as a flake input if you are on NixOS.

```nix
{
  # Add ncro to your inputs like so:
  inputs.ncro.url = "github:feel-co/ncro";

  outputs = { /* ... */ };
}
```

Then you can get the package from your flake input, and add it to your packages
to make `ncro` available in your system.

```nix
{inputs, pkgs, ...}: let
  ncroPkg = inputs.ncro.packages.${pkgs.stdenv.hostPlatform.system}.ncro;
in {
  environment.systemPackages = [ncroPkg];
}
```

You can also use the NixOS module as described below in the
[NixOS section](#nixos).

If you want to give ncro a try before you switch to it, you may also run it one
time with `nix run`.

```sh
# Run directly from the git repository; will be garbage collected
$ nix run github:feel-co/ncro  # start the ncro service
```

### Without Nix

[GitHub Releases]: https://github.com/feel-co/ncro/releases

You can also install ncro on any of your systems _without_ using Nix. New
releases are made when a version gets tagged, and are available under
[GitHub Releases]. To install ncro on your system without Nix, either:

- Download a tagged release from [GitHub Releases] for your platform and place
  the binary in your `$PATH`. Instructions may differ based on your
  distribution, but generally you want to download the built binary from
  releases and put it somewhere like `/usr/bin` or `~/.local/bin` depending on
  your distribution.
- Build and install from source with Cargo:

  ```bash
  cargo install ncro --locked
  ```

Additionally, you may get ncro from source via `cargo install` using
`cargo install --git https://github.com/feel-co/ncro --locked` or you may check
out to the repository, and use Cargo to build it. You'll need Rust 1.90 or
above. Most distributions should package this version already. You may, of
course, prefer to package the built releases if you'd like.

## Running Locally

```bash
# Run with default config
$ ncro
```

By default, `ncro` listens on `:8080` and uses `https://cache.nixos.org` as the
primary upstream, so you can usually start it without writing a config file
first.

To use an explicit config file:

```bash
ncro --config /etc/ncro/config.toml
```

You can also point the binary at a config file with `NCRO_CONFIG`.

## Minimal Config

```toml
[server]
listen = ":8080"

[[upstreams]]
url = "https://cache.nixos.org"
priority = 10

[cache]
db_path = "/var/lib/ncro/routes.db"
negative_ttl = "10m"

[logging]
level = "info"
format = "json"
```

`negative_ttl` controls how long failed lookups are cached before ncro tries the
upstreams again.

## Production Setup

In production, it is worth keeping the SQLite database on persistent storage,
making sure the service can write to it, and starting with a small upstream set
before you add more caches. A short TTL is useful while upstream performance is
changing; a longer one reduces churn once things are stable.

- keep `cache.db_path` on persistent storage
- ensure the service can write to the SQLite file
- set `listen` to the interface clients can reach
- start with one or two upstreams before adding more
- keep `ttl` reasonably short if upstream performance changes often

The server also uses `cache_priority` when it needs to order cache-backed
responses, so keep that at a positive value unless you have a specific reason to
change it.

## NixOS

```nix
{
  services.ncro = {
    enable = true;
    settings = {
      upstreams = [
        {
          url = "https://cache.nixos.org";
          priority = 10;
          public_key = "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=";
        }
      ];
    };
  };

  nix.settings.substituters = [ "http://localhost:8080" ];
}
```

By default, the module appends every non-empty
`services.ncro.settings.upstreams.*.public_key` value to
`nix.settings.trusted-public-keys`. If you're managing those keys separately,
you may set `services.ncro.addUpstreamPublicKeys` to false. The option defaults
to true.

### Discovery

If you enable discovery or mesh, those settings live in the same `settings`
block. Discovery is useful when you want ncro to learn peers from the local
network; mesh is the signed gossip path for sharing route decisions between
trusted nodes.

For discovery, the relevant fields are `enabled`, `service_name`, `domain`, and
`discovery_time`. The default service name is `_nix-serve._tcp`, and the domain
defaults to `local`. For mesh, `mesh.private_key` may point at a persisted
ed25519 key file. If you leave it empty, ncro creates an ephemeral identity on
startup. Peer public keys must be hex-encoded ed25519 keys.

## Systemd

If you are not on NixOS, a small Systemd unit is usually enough to get started:

```ini
[Unit]
Description=Nix Cache Route Optimizer

[Service]
ExecStart=/path/to/ncro --config /etc/ncro/config.toml
DynamicUser=true
StateDirectory=ncro
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Alternative init systems are _technically_ not supported, but ncro does not
inherently depend on Systemd. If you're using another init system (like Finit on
something like Finix, or Runit on something like MicrOS) simply adapt the
service to your system. The important part is running with `--config` and
optional environment overrides.

## Client Configuration

Point a Nix client at ncro like this:

```bash
nix-shell -p hello \
  --substituters http://localhost:8080 \
  --extra-trusted-public-keys cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
```

> [!TIP]
> For persistent setup, add the URL to `nix.settings.substituters` and add every
> upstream cache signing key to `nix.settings.trusted-public-keys`. The NixOS
> module does this for configured `public_key` and `public_keys` values unless
> `services.ncro.addUpstreamPublicKeys` is disabled.

## Verification

After installation, a quick health check and metrics scrape are usually enough
to confirm that the service is alive and talking to upstreams:

```bash
# Check health endpoint
$ curl http://localhost:8080/health

# Check metrics endpoint
$ curl http://localhost:8080/metrics
```

If health looks wrong, the usual places to check are the upstream URLs, local
DNS, firewall rules, and the `db_path` location.

If ncro is running but clients still miss it, check that the substituter URL is
present in the client configuration and that the proxy port is reachable.
