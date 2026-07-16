use std::{path::Path, sync::Arc};

use chrono::Utc;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use ncro_db::{Db, RouteEntry, TrustClaim};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{net::UdpSocket, time::Duration};

const MAX_PACKET_SIZE: usize = 1_400;
const HEADER_SIZE: usize = 96;
const MAX_GOSSIP_ROUTES: i64 = 25;

type DecodedPacket<'a> = (&'a [u8], &'a [u8], &'a [u8], Message);

#[derive(Debug, Error)]
pub enum MeshError {
  #[error("io: {0}")]
  Io(#[from] std::io::Error),
  #[error("msgpack: {0}")]
  Encode(#[from] rmp_serde::encode::Error),
  #[error("decode msgpack: {0}")]
  Decode(#[from] rmp_serde::decode::Error),
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
  Claims   = 2,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
  pub r#type:    MsgType,
  pub node_id:   String,
  pub timestamp: i64,
  /// Route gossip carried by an `Announce` message (empty for `Claims`).
  #[serde(default)]
  pub routes:    Vec<RouteEntry>,
  /// Trust claims carried by a `Claims` message (empty for `Announce`).
  #[serde(default)]
  pub claims:    Vec<TrustClaim>,
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
    match tokio::fs::read(key_path).await {
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
      Err(err) if err.kind() == std::io::ErrorKind::NotFound => {},
      Err(err) => return Err(MeshError::Io(err)),
    }
    if let Some(parent) = Path::new(key_path).parent() {
      tokio::fs::create_dir_all(parent).await?;
    }
    let key = SigningKey::from_bytes(&random_key_bytes());
    tokio::fs::write(key_path, key.to_bytes()).await?;
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
/// `trusted_keys` is the set of Nix signer public keys (`name:base64(key)`)
/// whose relayed trust claims may be accepted; claims signed by any other key
/// are dropped (see [`merge_claims`]).
pub async fn listen_and_serve(
  addr: &str,
  db: Db,
  allowed_keys: Vec<[u8; 32]>,
  trusted_keys: Vec<String>,
  stop: tokio::sync::watch::Receiver<bool>,
) -> Result<(), MeshError> {
  let socket = UdpSocket::bind(addr).await?;
  let trusted: std::collections::HashSet<String> =
    trusted_keys.into_iter().collect();
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
                      match msg.r#type {
                          MsgType::Announce if !msg.routes.is_empty() => {
                              merge_routes(&db, msg.routes).await;
                          }
                          MsgType::Claims if !msg.claims.is_empty() => {
                              let relay = format!("mesh://{src}");
                              merge_claims(&db, msg.claims, &trusted, &relay).await;
                          }
                          _ => {}
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

/// Merge trust claims relayed by a peer into the local store.
///
/// A relayed claim is accepted only if **both** hold:
///
/// 1. Its `signer_key` is in `trusted`, the set of Nix keys this node is
///    configured to trust. Without this gate, distinct-signer quorum is
///    security theatre: an attacker could generate any number of throwaway
///    keypairs, self-sign forged content under each, and relay the claims to
///    fabricate agreement. Dropping untrusted keys here also bounds how many
///    claims a hostile peer can write to the database.
/// 2. The embedded narinfo verifies against that `signer_key`, and the stored
///    claim is rebuilt from its signed fields. The peer's packet signature only
///    proves *who relayed* the claim; it cannot substitute a content tuple.
async fn merge_claims(
  db: &Db,
  incoming: Vec<TrustClaim>,
  trusted: &std::collections::HashSet<String>,
  relay: &str,
) {
  for claim in incoming {
    if !trusted.contains(&claim.signer_key) {
      tracing::warn!(
        store = claim.store_path,
        signer = claim.signer_name,
        "mesh: rejecting relayed trust claim from untrusted signer key"
      );
      continue;
    }
    let Ok(claim) = TrustClaim::from_verified_narinfo(
      &claim.narinfo_hash,
      relay,
      &claim.signer_key,
      &claim.narinfo,
      Utc::now(),
    ) else {
      tracing::warn!(
        store = claim.store_path,
        signer = claim.signer_name,
        "mesh: rejecting relayed trust claim that disagrees with its signed \
         narinfo"
      );
      continue;
    };
    if let Err(err) = db.set_trust_claim(&claim).await {
      tracing::warn!(error = %err, store = claim.store_path, "mesh: trust claim merge failed");
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
    claims: Vec::new(),
  };
  let packet = encode_packet(node, &msg)?;
  let socket = UdpSocket::bind("0.0.0.0:0").await?;
  socket.send_to(&packet, peer_addr).await?;
  Ok(())
}

fn encode_claims(
  node: &Node,
  claims: &[TrustClaim],
) -> Result<Vec<u8>, MeshError> {
  let msg = Message {
    r#type:    MsgType::Claims,
    node_id:   node.id(),
    timestamp: Utc::now().timestamp_nanos_opt().unwrap_or_default(),
    routes:    Vec::new(),
    claims:    claims.to_vec(),
  };
  encode_packet(node, &msg)
}

/// Relay trust claims to a peer, batching them so no UDP packet exceeds
/// [`MAX_PACKET_SIZE`].  A single claim that cannot fit on its own (its raw
/// narinfo is too large) is logged and skipped rather than silently dropped.
///
/// # Errors
///
/// Returns [`MeshError`] if a claim cannot be encoded, the socket cannot be
/// bound, or a packet fails to send.
pub async fn announce_claims(
  peer_addr: &str,
  node: &Node,
  claims: Vec<TrustClaim>,
) -> Result<(), MeshError> {
  let socket = UdpSocket::bind("0.0.0.0:0").await?;
  let mut batch: Vec<TrustClaim> = Vec::new();
  for claim in claims {
    // A claim that cannot fit in an empty packet on its own can never be
    // gossiped; log and skip it rather than dropping silently.
    if encode_claims(node, std::slice::from_ref(&claim))?.len()
      > MAX_PACKET_SIZE
    {
      tracing::warn!(
        store = claim.store_path,
        "mesh: trust claim exceeds packet size, skipping"
      );
      continue;
    }
    batch.push(claim);
    if encode_claims(node, &batch)?.len() <= MAX_PACKET_SIZE {
      continue;
    }
    // Adding the last claim overflowed the packet: flush the batch without it,
    // then start a fresh batch holding the claim that did not fit.
    let overflow = batch.split_off(batch.len() - 1);
    socket
      .send_to(&encode_claims(node, &batch)?, peer_addr)
      .await?;
    batch = overflow;
  }
  if !batch.is_empty() {
    socket
      .send_to(&encode_claims(node, &batch)?, peer_addr)
      .await?;
  }
  Ok(())
}

pub async fn run_gossip_loop(
  node: Node,
  db: Db,
  peers: Vec<String>,
  interval: Duration,
  gossip_claims: bool,
  mut stop: tokio::sync::watch::Receiver<bool>,
) {
  let mut ticker = tokio::time::interval(interval);
  loop {
    tokio::select! {
        _ = stop.changed() => return,
        _ = ticker.tick() => {
            let routes = db.list_recent_routes(MAX_GOSSIP_ROUTES).await.unwrap_or_default();
            let claims = if gossip_claims {
                db.list_recent_trust_claims(MAX_GOSSIP_ROUTES).await.unwrap_or_default()
            } else {
                Vec::new()
            };
            if routes.is_empty() && claims.is_empty() { continue; }
            for peer in &peers {
                let peer = peer.clone();
                let node = node.clone();
                let routes = routes.clone();
                let claims = claims.clone();
                tokio::spawn(async move {
                    if !routes.is_empty() { let _ = announce(&peer, &node, routes).await; }
                    if !claims.is_empty() { let _ = announce_claims(&peer, &node, claims).await; }
                });
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
  use base64::{Engine, engine::general_purpose::STANDARD};
  use ed25519_dalek::{Signer, SigningKey};
  use ncro_db::{Db, RouteEntry, TrustClaim};
  use ncro_narinfo::NarInfo;
  use rand::RngExt;

  use super::{merge_claims, merge_routes};

  /// Build a trust claim carrying a narinfo signed by `signer_key` so it
  /// re-verifies, unless `tamper` flips a content field after signing.
  fn signed_claim(store_hash: &str, tamper: bool) -> TrustClaim {
    let mut key_bytes = [0_u8; 32];
    rand::rng().fill(&mut key_bytes);
    let signing = SigningKey::from_bytes(&key_bytes);
    let store_path = format!("/nix/store/{store_hash}-pkg");
    let mut ni = NarInfo {
      store_path: store_path.clone(),
      nar_hash: "sha256:abc".into(),
      nar_size: 12,
      references: vec![format!("{store_hash}-pkg")],
      ..Default::default()
    };
    let sig = signing.sign(ni.fingerprint().as_bytes());
    let signer_key = format!(
      "test:{}",
      STANDARD.encode(signing.verifying_key().to_bytes())
    );
    ni.sig = vec![format!("test:{}", STANDARD.encode(sig.to_bytes()))];
    if tamper {
      ni.nar_size = 999;
    }
    let now = chrono::Utc::now();
    let narinfo = format!(
      "StorePath: {}\nNarHash: {}\nNarSize: {}\nReferences: {}\nSig: {}\n",
      ni.store_path,
      ni.nar_hash,
      ni.nar_size,
      ni.references.join(" "),
      ni.sig[0],
    );
    TrustClaim {
      narinfo_hash: store_hash.into(),
      store_path,
      upstream_url: "https://cache.example".into(),
      signer_name: "test".into(),
      signer_key,
      nar_hash: ni.nar_hash.clone(),
      nar_size: ni.nar_size,
      references: ni.references.join(" "),
      deriver: String::new(),
      ca: String::new(),
      file_hash: String::new(),
      file_size: 0,
      first_seen: now,
      last_seen: now,
      narinfo: narinfo.into_bytes(),
    }
  }

  fn trust_set(claim: &TrustClaim) -> std::collections::HashSet<String> {
    std::iter::once(claim.signer_key.clone()).collect()
  }

  #[tokio::test]
  async fn merge_claims_accepts_valid_trusted_signature()
  -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open(":memory:", 100).await?;
    let claim = signed_claim("aaa111", false);
    let trusted = trust_set(&claim);
    merge_claims(&db, vec![claim], &trusted, "mesh://test").await;
    assert_eq!(db.trust_claims("aaa111").await?.len(), 1);
    Ok(())
  }

  #[tokio::test]
  async fn merge_claims_rejects_tampered_claim()
  -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open(":memory:", 100).await?;
    let claim = signed_claim("bbb222", true);
    // Trust the signer key so we isolate the *signature* check.
    let trusted = trust_set(&claim);
    merge_claims(&db, vec![claim], &trusted, "mesh://test").await;
    assert!(
      db.trust_claims("bbb222").await?.is_empty(),
      "a claim whose narinfo signature does not verify must be dropped"
    );
    Ok(())
  }

  #[tokio::test]
  async fn merge_claims_rejects_forged_signed_fields()
  -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open(":memory:", 100).await?;
    let mut claim = signed_claim("forged", false);
    let trusted = trust_set(&claim);
    claim.nar_hash = "sha256:attacker-controlled".into();
    claim.references = "attacker-controlled".into();
    merge_claims(&db, vec![claim], &trusted, "mesh://test").await;
    let claims = db.trust_claims("forged").await?;
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].nar_hash, "sha256:abc");
    assert_eq!(claims[0].references, "forged-pkg");

    let mut claim = signed_claim("source", false);
    let trusted = trust_set(&claim);
    claim.narinfo_hash = "target".into();
    merge_claims(&db, vec![claim], &trusted, "mesh://test").await;
    assert!(db.trust_claims("target").await?.is_empty());
    Ok(())
  }

  #[tokio::test]
  async fn merge_claims_rejects_untrusted_signer()
  -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open(":memory:", 100).await?;
    // A perfectly valid self-signed claim, but the signer key is not trusted:
    // this is the attacker-generated-key case and must be dropped.
    let claim = signed_claim("ccc333", false);
    let trusted = std::collections::HashSet::new();
    merge_claims(&db, vec![claim], &trusted, "mesh://test").await;
    assert!(
      db.trust_claims("ccc333").await?.is_empty(),
      "a validly-signed claim from an untrusted key must be dropped"
    );
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
      ttl: now + chrono::Duration::seconds(ttl_secs),
      nar_hash: "sha256:aabbcc".into(),
      nar_size: 42,
      nar_url: "nar/test.nar".into(),
      narinfo_bytes: None,
    }
  }

  #[tokio::test]
  async fn merge_routes_inserts_new_route()
  -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open(":memory:", 100).await?;
    merge_routes(&db, vec![route("abc123", 10.0, 3600)]).await;
    assert!(db.get_route("abc123").await?.is_some());
    Ok(())
  }

  #[tokio::test]
  async fn merge_routes_skips_expired_route()
  -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open(":memory:", 100).await?;
    merge_routes(&db, vec![route("abc123", 10.0, -1)]).await;
    assert!(db.get_route("abc123").await?.is_none());
    Ok(())
  }

  #[tokio::test]
  async fn merge_routes_does_not_overwrite_lower_latency()
  -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open(":memory:", 100).await?;
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
  async fn merge_routes_overwrites_higher_latency()
  -> Result<(), Box<dyn std::error::Error>> {
    let db = Db::open(":memory:", 100).await?;
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
