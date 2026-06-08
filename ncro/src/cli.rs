use ncro_config::Config;
use ncro_db::Db;
use ncro_discovery::Discovery;
use ncro_health::Prober;
use ncro_router::{Router, RouterTuning};
use pound::Parse;
use tokio::net::TcpListener;
use tracing_subscriber::{EnvFilter, fmt};

/// Returns the number of socket-activation fds passed by systemd, or `None`
/// if socket activation is not in use.
fn parse_listen_fds(val: Option<&str>) -> Option<u32> {
  let n: u32 = val?.parse().ok()?;
  if n >= 1 { Some(n) } else { None }
}

/// Attempts to inherit a pre-bound TCP socket from systemd (fd 3).
/// Returns `None` when `LISTEN_FDS` is absent or zero, or when `LISTEN_PID`
/// does not match this process (guards against inheriting stale env vars).
fn inherited_listener() -> Option<std::net::TcpListener> {
  use std::os::unix::io::FromRawFd;
  parse_listen_fds(std::env::var("LISTEN_FDS").ok().as_deref())?;
  // Confirm the fds are intended for this process, as required by the
  // sd_listen_fds(3) protocol.
  let our_pid = std::process::id().to_string();
  if std::env::var("LISTEN_PID").ok().as_deref() != Some(our_pid.as_str()) {
    return None;
  }
  // SAFETY: systemd passes a pre-bound TCP socket as fd 3
  // (SD_LISTEN_FDS_START). The fd is valid for the lifetime of this process
  // and not owned by anyone else when LISTEN_FDS >= 1 and LISTEN_PID matches.
  let listener = unsafe { std::net::TcpListener::from_raw_fd(3) };
  listener.set_nonblocking(true).ok()?;
  Some(listener)
}

/// Sends `READY=1` to the systemd notification socket if `$NOTIFY_SOCKET` is
/// set. Logs a warning on error but does not abort startup.
fn sd_notify_ready() {
  use std::{
    ffi::OsString,
    os::unix::{ffi::OsStringExt, net::UnixDatagram},
  };

  let Ok(socket_path) = std::env::var("NOTIFY_SOCKET") else {
    return;
  };
  let sock = match UnixDatagram::unbound() {
    Ok(s) => s,
    Err(e) => {
      tracing::warn!("sd_notify: failed to create socket: {e}");
      return;
    },
  };
  // NOTIFY_SOCKET may use abstract socket syntax ('@' prefix), which maps
  // to a null-byte prefix in the sockaddr_un.
  let result = socket_path.strip_prefix('@').map_or_else(
    || sock.send_to(b"READY=1\n", &socket_path),
    |name| {
      let mut bytes = vec![0u8];
      bytes.extend_from_slice(name.as_bytes());
      sock.send_to(
        b"READY=1\n",
        std::path::Path::new(&OsString::from_vec(bytes)),
      )
    },
  );
  if let Err(e) = result {
    tracing::warn!("sd_notify failed: {e}");
  }
}

#[derive(Debug, Parse)]
#[pound(name = "ncro")]
/// Nix Cache Route Optimizer
pub struct Args {
  /// Path to the configuration file.
  #[pound(short, long, env = "NCRO_CONFIG")]
  pub config: Option<String>,
}

pub async fn run() -> anyhow::Result<()> {
  let args = Args::parse();
  let cfg = Config::load(args.config.as_deref())?;
  cfg.validate()?;

  init_logging(&cfg.logging.level, &cfg.logging.format);
  let _ = ncro_metrics::get();

  let db = Db::open(&cfg.cache.db_path, cfg.cache.max_entries).await?;
  let prober = Prober::new(cfg.cache.latency_alpha)?;
  prober.init_upstreams(&cfg.upstreams).await;
  for row in db.load_all_health().await.unwrap_or_default() {
    prober
      .seed(
        &row.url,
        row.ema_latency,
        row.consecutive_fails,
        row.total_queries,
      )
      .await;
  }
  let db_for_health = db.clone();
  prober
    .set_health_persistence(move |url, ema, fails, queries| {
      let db = db_for_health.clone();
      tokio::spawn(async move {
        let _ = db
          .save_health(
            &url,
            ema,
            i64::from(fails),
            i64::try_from(queries).unwrap_or(i64::MAX),
          )
          .await;
      });
    })
    .await;
  for upstream in &cfg.upstreams {
    let prober = prober.clone();
    let url = upstream.url.clone();
    tokio::spawn(async move {
      prober.probe_upstream(url).await;
    });
  }

  let router = Router::new(
    db.clone(),
    prober.clone(),
    cfg.cache.ttl.0,
    std::time::Duration::from_secs(5),
    cfg.cache.negative_ttl.0,
    RouterTuning {
      max_concurrent_races:      cfg.cache.mass_query.max_concurrent_races,
      per_upstream_max_inflight: cfg.cache.mass_query.per_upstream_max_inflight,
      in_memory_negative_ttl:    cfg.cache.mass_query.in_memory_negative_ttl.0,
      upstream_cooldown:         cfg.cache.mass_query.upstream_cooldown.0,
    },
  )?;

  for upstream in &cfg.upstreams {
    router.register_upstream(upstream).await?;
  }
  if cfg.fallback_cache.enabled {
    router
      .register_upstream(&cfg.fallback_cache.upstream)
      .await?;
  }

  let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
  let probe_prober = prober.clone();
  let probe_stop = stop_rx.clone();
  tokio::spawn(async move {
    probe_prober
      .run_probe_loop(std::time::Duration::from_secs(30), probe_stop)
      .await;
  });

  let db_for_expiry = db.clone();
  let mut expiry_stop = stop_rx.clone();
  tokio::spawn(async move {
    let mut ticker = tokio::time::interval(std::time::Duration::from_mins(5));
    loop {
      tokio::select! {
          _ = expiry_stop.changed() => return,
          _ = ticker.tick() => {
              let _ = db_for_expiry.expire_old_routes().await;
              let _ = db_for_expiry.expire_negatives().await;
              if let Ok(count) = db_for_expiry.route_count().await { ncro_metrics::get().route_entries.set(count); }
          }
      }
    }
  });

  if cfg.discovery.enabled {
    let discovery = Discovery::new(cfg.discovery.clone(), prober.clone())?;
    let discovery_stop = stop_rx.clone();
    tokio::spawn(async move {
      let _ = discovery.run(discovery_stop).await;
    });
  }

  if cfg.mesh.enabled {
    let node = ncro_mesh::Node::new(&cfg.mesh.private_key_path).await?;
    tracing::info!(
      node_id = node.id(),
      public_key = hex::encode(node.public_key()),
      "mesh node identity"
    );
    let allowed = cfg
      .mesh
      .peers
      .iter()
      .filter_map(|p| hex::decode(&p.public_key).ok()?.try_into().ok())
      .collect::<Vec<[u8; 32]>>();
    ncro_mesh::listen_and_serve(
      &cfg.mesh.bind_addr,
      db.clone(),
      allowed,
      stop_rx.clone(),
    )
    .await?;
    let peers = cfg
      .mesh
      .peers
      .iter()
      .map(|p| p.addr.clone())
      .collect::<Vec<_>>();
    tokio::spawn(ncro_mesh::run_gossip_loop(
      node,
      db.clone(),
      peers,
      cfg.mesh.gossip_interval.0,
      stop_rx.clone(),
    ));
  }

  let app = ncro_server::app(router, prober, db, ncro_server::AppConfig {
    upstreams:      cfg.upstreams.clone(),
    fallback_cache: cfg
      .fallback_cache
      .enabled
      .then_some(cfg.fallback_cache.upstream.clone()),
    cache_priority: cfg.server.cache_priority,
    read_timeout:   cfg.server.read_timeout.0,
    write_timeout:  cfg.server.write_timeout.0,
  })?;
  let listener = match inherited_listener() {
    Some(std_listener) => {
      tracing::info!("socket activation: using inherited listener (fd 3)");
      TcpListener::from_std(std_listener)?
    },
    None => TcpListener::bind(normalize_listen(&cfg.server.listen)).await?,
  };
  tracing::info!(
    addr = cfg.server.listen,
    upstreams = cfg.upstreams.len(),
    version = env!("CARGO_PKG_VERSION"),
    "ncro listening"
  );
  sd_notify_ready();
  let server = axum::serve(listener, app).with_graceful_shutdown(async move {
    let _ = tokio::signal::ctrl_c().await;
  });
  let result = server.await;
  let _ = stop_tx.send(true);
  result?;
  Ok(())
}

fn init_logging(level: &str, format_name: &str) {
  let filter =
    EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
  if format_name == "text" {
    fmt().with_env_filter(filter).init();
  } else {
    fmt().json().with_env_filter(filter).init();
  }
}

fn normalize_listen(listen: &str) -> String {
  if listen.starts_with(':') {
    format!("0.0.0.0{listen}")
  } else {
    listen.to_string()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn listen_fds_absent() {
    assert!(parse_listen_fds(None).is_none());
  }

  #[test]
  fn listen_fds_zero() {
    assert!(parse_listen_fds(Some("0")).is_none());
  }

  #[test]
  fn listen_fds_one() {
    assert_eq!(parse_listen_fds(Some("1")), Some(1));
  }

  #[test]
  fn listen_fds_invalid() {
    assert!(parse_listen_fds(Some("not-a-number")).is_none());
    assert!(parse_listen_fds(Some("")).is_none());
  }
}
