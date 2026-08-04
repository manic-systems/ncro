use std::{
  io::{self, ErrorKind},
  path::{Path, PathBuf},
  sync::Arc,
};

use chrono::Utc;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use ncro_db::{Db, RouteEntry};
use rand::RngExt;
use rmp_serde::{decode::Error as DecodeError, encode::Error as EncodeError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
  fs::{self, OpenOptions},
  io::AsyncWriteExt,
  net::UdpSocket,
  sync::watch,
  time::{self, Duration},
};

const MAX_PACKET_SIZE: usize = 1_400;
const HEADER_SIZE: usize = 96;
const MAX_GOSSIP_ROUTES: i64 = 25;

type DecodedPacket<'a> = (&'a [u8], &'a [u8], &'a [u8], Message);

#[derive(Debug, Error)]
pub enum MeshError {
  #[error("io: {0}")]
  Io(#[from] io::Error),
  #[error("msgpack: {0}")]
  Encode(#[from] EncodeError),
  #[error("decode msgpack: {0}")]
  Decode(#[from] DecodeError),
  #[error("packet too short: {0} bytes")]
  PacketTooShort(usize),
  #[error("invalid signature")]
  InvalidSignature,
  #[error("invalid key file size {got}, want 32 or 64 bytes")]
  InvalidKeyFileSize { got: usize },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MsgType {
  Announce = 1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
  pub r#type:    MsgType,
  pub node_id:   String,
  pub timestamp: i64,
  pub routes:    Vec<RouteEntry>,
}

#[derive(Clone)]
pub struct Node {
  signing_key: Arc<SigningKey>,
}

impl Node {
  /// # Errors
  ///
  /// Returns [`MeshError`] if the key file exists but cannot be read, is the
  /// wrong size, or a new key cannot be written to `key_path`.
  pub async fn new(key_path: &str) -> Result<Self, MeshError> {
    if key_path.is_empty() {
      return Ok(Self {
        signing_key: Arc::new(SigningKey::from_bytes(&random_key_bytes())),
      });
    }
    match fs::read(key_path).await {
      Ok(data) => {
        if data.len() != 32 && data.len() != 64 {
          return Err(MeshError::InvalidKeyFileSize { got: data.len() });
        }
        let bytes = <[u8; 32]>::try_from(&data[..32])
          .map_err(|_| MeshError::InvalidSignature)?;
        return Ok(Self {
          signing_key: Arc::new(SigningKey::from_bytes(&bytes)),
        });
      },
      Err(err) if err.kind() == ErrorKind::NotFound => {},
      Err(err) => return Err(MeshError::Io(err)),
    }
    if let Some(parent) = Path::new(key_path).parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent).await?;
    }
    let key = SigningKey::from_bytes(&random_key_bytes());
    write_new_key(Path::new(key_path), &key.to_bytes()).await?;
    Ok(Self {
      signing_key: Arc::new(key),
    })
  }

  #[must_use]
  pub fn id(&self) -> String {
    hex::encode(&self.public_key()[..8])
  }
  #[must_use]
  pub fn public_key(&self) -> [u8; 32] {
    self.signing_key.verifying_key().to_bytes()
  }
  /// # Errors
  ///
  /// Returns [`MeshError`] if the message cannot be serialized.
  pub fn sign(&self, msg: &Message) -> Result<(Vec<u8>, Vec<u8>), MeshError> {
    let body = rmp_serde::to_vec(msg)?;
    Ok((
      body.clone(),
      self.signing_key.sign(&body).to_bytes().to_vec(),
    ))
  }
}

async fn write_new_key(
  key_path: &Path,
  key_bytes: &[u8; 32],
) -> Result<(), io::Error> {
  let temp_path = temporary_key_path(key_path);
  let mut options = OpenOptions::new();
  options.write(true).create_new(true);
  #[cfg(unix)]
  {
    options.mode(0o600);
  }
  let mut file = options.open(&temp_path).await?;
  let write_result = async {
    file.write_all(key_bytes).await?;
    file.sync_all().await?;
    fs::hard_link(&temp_path, key_path).await
  }
  .await;
  drop(file);
  let cleanup_result = fs::remove_file(temp_path).await;
  write_result?;
  cleanup_result
}

fn temporary_key_path(key_path: &Path) -> PathBuf {
  let mut path = key_path.as_os_str().to_os_string();
  path.push(format!(".{}.tmp", hex::encode(random_key_bytes())));
  path.into()
}

fn random_key_bytes() -> [u8; 32] {
  let mut bytes = [0_u8; 32];
  rand::rng().fill(&mut bytes);
  bytes
}

/// # Errors
///
/// Returns [`MeshError::InvalidSignature`] if the key, signature, or body
/// fails verification.
pub fn verify(pubkey: &[u8], body: &[u8], sig: &[u8]) -> Result<(), MeshError> {
  let pubkey: [u8; 32] =
    pubkey.try_into().map_err(|_| MeshError::InvalidSignature)?;
  let sig: [u8; 64] =
    sig.try_into().map_err(|_| MeshError::InvalidSignature)?;
  VerifyingKey::from_bytes(&pubkey)
    .map_err(|_| MeshError::InvalidSignature)?
    .verify(body, &Signature::from_bytes(&sig))
    .map_err(|_| MeshError::InvalidSignature)
}

/// # Errors
///
/// Returns [`MeshError`] if the UDP socket cannot be bound to `addr`.
pub async fn listen_and_serve(
  addr: &str,
  db: Db,
  allowed_keys: Vec<[u8; 32]>,
  stop: watch::Receiver<bool>,
) -> Result<(), MeshError> {
  let socket = UdpSocket::bind(addr).await?;
  tokio::spawn(async move {
    let mut stop = stop;
    let mut buf = vec![0; MAX_PACKET_SIZE];
    loop {
      tokio::select! {
          _ = stop.changed() => return,
          recv = socket.recv_from(&mut buf) => {
              let Ok((n, src)) = recv else { return; };
              match decode_packet(&buf[..n]) {
                  Ok((pubkey, sig, body, msg)) => {
                      if !allowed_keys.is_empty() && !allowed_keys.iter().any(|k| k.as_slice() == pubkey) {
                          tracing::warn!(?src, "mesh: rejecting packet from unknown sender");
                          continue;
                      }
                      if let Err(err) = verify(pubkey, body, sig) {
                          tracing::warn!(?src, error = %err, "mesh: signature verification failed");
                          continue;
                      }
                      if msg.r#type == MsgType::Announce && !msg.routes.is_empty() {
                          merge_routes(&db, msg.routes).await;
                      }
                  }
                  Err(err) => tracing::warn!(?src, error = %err, "mesh: malformed packet"),
              }
          }
      }
    }
  });
  Ok(())
}

async fn merge_routes(db: &Db, incoming: Vec<RouteEntry>) {
  let now = Utc::now();
  for route in incoming.into_iter().filter(|route| route.ttl > now) {
    let should_set = match db.get_route(&route.store_path).await {
      Ok(Some(existing)) if route.latency_ema > existing.latency_ema => false,
      Ok(Some(existing))
        if route.latency_ema.total_cmp(&existing.latency_ema).is_eq()
          && route.last_verified <= existing.last_verified =>
      {
        false
      },
      Ok(_) => true,
      Err(err) => {
        tracing::warn!(error = %err, store = route.store_path, "mesh: route lookup failed");
        false
      },
    };
    if should_set && let Err(err) = db.set_route(&route).await {
      tracing::warn!(error = %err, store = route.store_path, "mesh: route merge failed");
    }
  }
}

/// # Errors
///
/// Returns [`MeshError`] if the message cannot be signed, the socket cannot
/// be bound, or the packet fails to send.
pub async fn announce(
  peer_addr: &str,
  node: &Node,
  routes: Vec<RouteEntry>,
) -> Result<(), MeshError> {
  let msg = Message {
    r#type: MsgType::Announce,
    node_id: node.id(),
    timestamp: Utc::now().timestamp_nanos_opt().unwrap_or_default(),
    routes,
  };
  let packet = encode_packet(node, &msg)?;
  let socket = UdpSocket::bind("0.0.0.0:0").await?;
  socket.send_to(&packet, peer_addr).await?;
  Ok(())
}

pub async fn run_gossip_loop(
  node: Node,
  db: Db,
  peers: Vec<String>,
  interval: Duration,
  mut stop: watch::Receiver<bool>,
) {
  let mut ticker = time::interval(interval);
  loop {
    tokio::select! {
        _ = stop.changed() => return,
        _ = ticker.tick() => {
            let Ok(routes) = db.list_recent_routes(MAX_GOSSIP_ROUTES).await else { continue; };
            if routes.is_empty() { continue; }
            for peer in &peers {
                let peer = peer.clone();
                let node = node.clone();
                let routes = routes.clone();
                tokio::spawn(async move { let _ = announce(&peer, &node, routes).await; });
            }
        }
    }
  }
}

fn encode_packet(node: &Node, msg: &Message) -> Result<Vec<u8>, MeshError> {
  let (body, sig) = node.sign(msg)?;
  let mut packet = Vec::with_capacity(HEADER_SIZE + body.len());
  packet.extend_from_slice(&node.public_key());
  packet.extend_from_slice(&sig);
  packet.extend_from_slice(&body);
  Ok(packet)
}

fn decode_packet(packet: &[u8]) -> Result<DecodedPacket<'_>, MeshError> {
  if packet.len() < HEADER_SIZE {
    return Err(MeshError::PacketTooShort(packet.len()));
  }
  let pubkey = &packet[..32];
  let sig = &packet[32..HEADER_SIZE];
  let body = &packet[HEADER_SIZE..];
  let msg = rmp_serde::from_slice(body)?;
  Ok((pubkey, sig, body, msg))
}

#[cfg(test)]
mod tests {
  use std::{
    env,
    error::Error,
    process,
    time::{Duration, SystemTime, UNIX_EPOCH},
  };

  use chrono::Duration as ChronoDuration;
  use ncro_db::{Db, RouteEntry};
  use tokio::fs;

  use super::{Node, merge_routes};

  const SLOW_STATEMENT_THRESHOLD: Duration = Duration::from_secs(1);

  #[tokio::test]
  async fn node_generates_and_reloads_private_key() -> Result<(), Box<dyn Error>>
  {
    let path = env::temp_dir().join(format!(
      "ncro-mesh-key-{}-{}",
      process::id(),
      SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    let path = path.to_string_lossy();

    let generated = Node::new(&path).await?;
    let reloaded = Node::new(&path).await?;

    assert_eq!(generated.public_key(), reloaded.public_key());
    let metadata = fs::metadata(path.as_ref()).await?;
    assert_eq!(metadata.len(), 32);
    #[cfg(unix)]
    {
      use std::os::unix::fs::PermissionsExt;
      let mode = metadata.permissions().mode();
      assert_eq!(mode & 0o777, 0o600);
    }
    fs::remove_file(path.as_ref()).await?;
    Ok(())
  }

  fn route(store_path: &str, latency_ema: f64, ttl_secs: i64) -> RouteEntry {
    let now = chrono::Utc::now();
    RouteEntry {
      store_path: store_path.into(),
      upstream_url: "http://test.example.com".into(),
      latency_ms: latency_ema,
      latency_ema,
      last_verified: now,
      query_count: 1,
      failure_count: 0,
      ttl: now + ChronoDuration::seconds(ttl_secs),
      nar_hash: "sha256:aabbcc".into(),
      nar_size: 42,
      nar_url: "nar/test.nar".into(),
      upstream_nar_url: "nar/test.nar".into(),
      narinfo_bytes: None,
    }
  }

  #[tokio::test]
  async fn merge_routes_inserts_new_route() -> Result<(), Box<dyn Error>> {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    merge_routes(&db, vec![route("abc123", 10.0, 3600)]).await;
    assert!(db.get_route("abc123").await?.is_some());
    Ok(())
  }

  #[tokio::test]
  async fn merge_routes_skips_expired_route() -> Result<(), Box<dyn Error>> {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    merge_routes(&db, vec![route("abc123", 10.0, -1)]).await;
    assert!(db.get_route("abc123").await?.is_none());
    Ok(())
  }

  #[tokio::test]
  async fn merge_routes_does_not_overwrite_lower_latency()
  -> Result<(), Box<dyn Error>> {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    db.set_route(&route("abc123", 5.0, 3600)).await?;
    merge_routes(&db, vec![route("abc123", 20.0, 3600)]).await;
    let got = db
      .get_route("abc123")
      .await?
      .ok_or("expected route in db")?;
    assert!(
      (got.latency_ema - 5.0).abs() < f64::EPSILON,
      "worse incoming must not overwrite better existing: got {}",
      got.latency_ema
    );
    Ok(())
  }

  #[tokio::test]
  async fn merge_routes_overwrites_higher_latency() -> Result<(), Box<dyn Error>>
  {
    let db = Db::open(":memory:", 100, SLOW_STATEMENT_THRESHOLD).await?;
    db.set_route(&route("abc123", 20.0, 3600)).await?;
    merge_routes(&db, vec![route("abc123", 5.0, 3600)]).await;
    let got = db
      .get_route("abc123")
      .await?
      .ok_or("expected route in db")?;
    assert!(
      (got.latency_ema - 5.0).abs() < f64::EPSILON,
      "better incoming must overwrite worse existing: got {}",
      got.latency_ema
    );
    Ok(())
  }
}
