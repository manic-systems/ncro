use std::{
  collections::{HashMap, HashSet},
  net::SocketAddr,
  time::{Duration, Instant},
};

use mdns_sd::{ServiceDaemon, ServiceEvent};
use ncro_config::{AddressFamily, DiscoveryConfig};
use ncro_health::Prober;
use tokio::{
  sync::{mpsc, watch},
  task::spawn_blocking,
  time,
};

pub struct Discovery {
  cfg:    DiscoveryConfig,
  prober: Prober,
  daemon: ServiceDaemon,
}

/// Discovered peers and the upstreams they registered with the prober.
#[derive(Default)]
struct Peers {
  /// fullname -> (upstream URLs for every routable address, last seen)
  by_name: HashMap<String, (Vec<String>, Instant)>,
  /// URLs discovery added, so a configured upstream is never removed.
  owned:   HashSet<String>,
}

impl Peers {
  async fn resolved(
    &mut self,
    prober: &Prober,
    name: String,
    urls: Vec<String>,
    priority: i32,
  ) {
    let previous = self
      .by_name
      .remove(&name)
      .map(|(previous, _)| previous)
      .unwrap_or_default();
    for url in urls.iter().filter(|url| !previous.contains(url)) {
      if prober.add_upstream(url.clone(), priority).await {
        tracing::info!(url = url.as_str(), "discovered nix-serve instance");
        self.owned.insert(url.clone());
      }
    }

    let dropped = previous
      .into_iter()
      .filter(|url| !urls.contains(url))
      .collect::<Vec<_>>();
    self.by_name.insert(name, (urls, Instant::now()));
    for url in &dropped {
      self.release(prober, url).await;
    }
  }

  async fn expire(&mut self, prober: &Prober, expiration: Duration) {
    let now = Instant::now();
    let stale = self
      .by_name
      .extract_if(|_, (_, seen)| now.duration_since(*seen) > expiration)
      .flat_map(|(_, (urls, _))| urls)
      .collect::<Vec<_>>();
    for url in &stale {
      self.release(prober, url).await;
    }
  }

  /// Stops probing `url` once no live peer advertises it.
  async fn release(&mut self, prober: &Prober, url: &str) {
    let advertised = self
      .by_name
      .values()
      .any(|(urls, _)| urls.iter().any(|live| live == url));
    if advertised || !self.owned.remove(url) {
      return;
    }
    tracing::info!(url, "removing stale peer");
    prober.remove_upstream(url).await;
  }
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
    let mut peers = Peers::default();
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
          _ = cleanup.tick() => peers.expire(&self.prober, expiration).await,
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
                  let name = info.get_fullname().to_string();
                  peers.resolved(&self.prober, name, urls, priority).await;
              }
          }
      }
    }
  }
}
