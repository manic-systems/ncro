use std::{
  collections::HashMap,
  net::SocketAddr,
  sync::Arc,
  time::{Duration, Instant},
};

use mdns_sd::{ServiceDaemon, ServiceEvent};
use ncro_config::{AddressFamily, DiscoveryConfig};
use ncro_health::Prober;
use tokio::{
  sync::{Mutex, mpsc, watch},
  task::spawn_blocking,
  time,
};

/// fullname -> (list of upstream URLs for all routable addresses, last seen)
type PeerMap = Arc<Mutex<HashMap<String, (Vec<String>, Instant)>>>;

pub struct Discovery {
  cfg:    DiscoveryConfig,
  prober: Prober,
  daemon: ServiceDaemon,
  peers:  PeerMap,
}

impl Discovery {
  /// # Errors
  ///
  /// Returns an error if the mDNS service daemon cannot be created.
  pub fn new(cfg: DiscoveryConfig, prober: Prober) -> anyhow::Result<Self> {
    Ok(Self {
      cfg,
      prober,
      daemon: ServiceDaemon::new()?,
      peers: Arc::new(Mutex::new(HashMap::new())),
    })
  }

  /// # Errors
  ///
  /// Returns an error if the mDNS browse subscription fails.
  pub async fn run(
    self,
    mut stop: watch::Receiver<bool>,
  ) -> anyhow::Result<()> {
    let service = format!(
      "{}.{}.",
      self.cfg.service_name.trim_end_matches('.'),
      self.cfg.domain.trim_end_matches('.')
    );
    let receiver = self.daemon.browse(&service)?;
    let (event_tx, mut event_rx) = mpsc::channel(16);
    spawn_blocking(move || {
      while let Ok(event) = receiver.recv() {
        if event_tx.blocking_send(event).is_err() {
          break;
        }
      }
    });
    let peers = Arc::clone(&self.peers);
    let prober = self.prober.clone();
    let priority = self.cfg.priority;
    let mut cleanup = time::interval(Duration::from_secs(10));
    let expiration = if self.cfg.discovery_time.0.is_zero() {
      Duration::from_secs(30)
    } else {
      self.cfg.discovery_time.0 * 3
    };

    loop {
      tokio::select! {
          _ = stop.changed() => { let _ = self.daemon.shutdown(); return Ok(()); }
          _ = cleanup.tick() => {
              let stale = {
                  let mut guard = peers.lock().await;
                  let now = Instant::now();
                  let stale = guard
                      .iter()
                      .filter(|(_, (_, seen))| now.duration_since(*seen) > expiration)
                      .map(|(k, (urls, _))| (k.clone(), urls.clone()))
                      .collect::<Vec<_>>();
                  for (key, _) in &stale { guard.remove(key); }
                  stale
              };
              for (_, urls) in stale {
                  for url in &urls {
                      tracing::info!(url = url.as_str(), "removing stale peer");
                      prober.remove_upstream(url).await;
                  }
              }
          }
          event = event_rx.recv() => {
              if let Some(ServiceEvent::ServiceResolved(info)) = event {
                  // Register every matching-family routable address as a separate
                  // upstream so the router's race engine can try them in parallel.
                  // Loopback and unspecified are always skipped (avahi publishes
                  // all addresses including 127.0.0.1/::1).
                  let af = &self.cfg.address_family;
                  let urls: Vec<String> = info
                      .get_addresses()
                      .iter()
                      .map(mdns_sd::ScopedIp::to_ip_addr)
                      .filter(|ip| !ip.is_loopback() && !ip.is_unspecified())
                      .filter(|ip| match af {
                          AddressFamily::Any  => true,
                          AddressFamily::Ipv4 => ip.is_ipv4(),
                          AddressFamily::Ipv6 => ip.is_ipv6(),
                      })
                      .map(|addr| {
                          format!(
                              "http://{}",
                            SocketAddr::new(addr, info.get_port())
                          )
                      })
                      .collect();
                  if urls.is_empty() {
                      continue;
                  }
                  let key = info.get_fullname().to_string();
                  let is_new = peers
                      .lock()
                      .await
                      .insert(key, (urls.clone(), Instant::now()))
                      .is_none();
                  if is_new {
                      for url in &urls {
                          tracing::info!(url = url.as_str(), "discovered nix-serve instance");
                          prober.add_upstream(url.clone(), priority).await;
                      }
                  }
              }
          }
      }
    }
  }
}
