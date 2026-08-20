use std::{
  collections::{BTreeMap, HashMap},
  str,
  sync::Arc,
  time::{Duration, Instant},
};

use chrono::Utc;
use dashmap::{DashMap, mapref::entry::Entry};
use futures_util::{StreamExt, stream::FuturesUnordered};
use moka::future::Cache as MokaCache;
use ncro_config::{
  FilterAction,
  FilterField,
  FilterRule,
  NarUrlMode,
  S3Config,
  UpstreamConfig,
};
use ncro_db::{Db, DbError, RouteEntry};
use ncro_health::{Prober, Status};
use ncro_narinfo::{NarInfo, NarInfoError, parse_public_key};
use ncro_s3::{S3ClientPool, S3Error};
use thiserror::Error;
use tokio::{
  sync::{Mutex, RwLock, Semaphore},
  time,
};

#[derive(Debug, Error)]
pub enum RouterError {
  #[error("not found in any upstream")]
  NotFound,
  #[error("all upstreams unavailable")]
  UpstreamUnavailable,
  #[error("no candidates for {0:?}")]
  NoCandidates(String),
  #[error("narinfo signature verification failed")]
  SignatureVerificationFailed,
  #[error("fetch narinfo: {0}")]
  FetchNarinfo(#[from] reqwest::Error),
  #[error("S3 request failed: {0}")]
  S3(#[from] S3Error),
  #[error("parse narinfo: {0}")]
  ParseNarinfo(#[from] NarInfoError),
  #[error(transparent)]
  Db(#[from] DbError),
}

#[derive(Debug, Error)]
pub enum UpstreamRegistrationError {
  #[error("invalid upstream public key: {0}")]
  PublicKey(#[from] NarInfoError),
  #[error("build upstream HTTP client: {0}")]
  HttpClient(#[from] reqwest::Error),
}

pub trait RouterUpstream {
  fn url(&self) -> &str;
  fn public_key(&self) -> &str;
  fn public_keys(&self) -> &[String];
  fn username(&self) -> &str;
  fn password(&self) -> Option<&str>;
  fn filters(&self) -> &[FilterRule];
  fn nar_url_mode(&self) -> NarUrlMode;
  fn narinfo_timeout(&self) -> Option<Duration>;
  fn s3(&self) -> Option<&S3Config>;
}

impl RouterUpstream for UpstreamConfig {
  fn url(&self) -> &str {
    &self.url
  }

  fn public_key(&self) -> &str {
    &self.public_key
  }

  fn public_keys(&self) -> &[String] {
    &self.public_keys
  }

  fn username(&self) -> &str {
    &self.username
  }

  fn password(&self) -> Option<&str> {
    self.password.as_deref()
  }

  fn filters(&self) -> &[FilterRule] {
    &self.filters
  }

  fn nar_url_mode(&self) -> NarUrlMode {
    self.nar_url_mode
  }

  fn narinfo_timeout(&self) -> Option<Duration> {
    self.narinfo_timeout.as_ref().map(|timeout| timeout.0)
  }

  fn s3(&self) -> Option<&S3Config> {
    self.s3.as_ref()
  }
}

#[derive(Debug, Clone)]
pub struct ResolveResult {
  pub url:           String,
  pub latency_ms:    f64,
  pub cache_hit:     bool,
  pub narinfo_bytes: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct Router {
  inner: Arc<RouterInner>,
}

#[derive(Debug, Clone, Copy)]
pub struct RouterTuning {
  pub max_concurrent_races:      u32,
  pub per_upstream_max_inflight: u32,
  pub in_memory_negative_ttl:    Duration,
  pub upstream_cooldown:         Duration,
}

struct RouterInner {
  db:                       Db,
  prober:                   Prober,
  route_ttl:                Duration,
  race_timeout:             Duration,
  negative_ttl:             Duration,
  client:                   reqwest::Client,
  upstream_clients:         RwLock<HashMap<String, reqwest::Client>>,
  s3:                       S3ClientPool,
  upstream_keys:            RwLock<HashMap<String, Vec<String>>>,
  upstream_auth:            RwLock<HashMap<String, (String, Option<String>)>>,
  upstream_filters:         RwLock<HashMap<String, Vec<FilterRule>>>,
  upstream_nar_url_modes:   RwLock<HashMap<String, NarUrlMode>>,
  inflight:                 DashMap<String, Arc<Mutex<()>>>,
  lru:                      MokaCache<String, Arc<ResolveResult>>,
  miss_lru:                 MokaCache<String, ()>,
  race_semaphore:           Arc<Semaphore>,
  per_upstream_limit:       u32,
  upstream_semaphores:      DashMap<String, Arc<Semaphore>>,
  upstream_cooldown:        DashMap<String, Instant>,
  upstream_cooldown_window: Duration,
}

#[derive(Debug)]
struct RaceResult {
  url:        String,
  latency_ms: f64,
}

enum CommitOutcome {
  Accepted(ResolveResult),
  Rejected,
}

enum CachedFilterCheck {
  Accepted(Option<Vec<u8>>),
  Rejected,
}

/// Outcome of racing a single priority group.
#[derive(Debug)]
enum RaceGroupError {
  /// Every reachable upstream in the group returned a non-success status
  /// (i.e. the path is not present in this group).
  NotFound,
  /// Every upstream in the group hit a network-level error; the path may
  /// exist but the group is unreachable.
  NetworkError,
  /// The race deadline expired before any upstream responded.
  Timeout,
}

enum RaceAttempt {
  Winner(RaceResult),
  NotFound,
  NetworkError { upstream: String },
}

struct InflightGuard<'a> {
  map: &'a DashMap<String, Arc<Mutex<()>>>,
  key: String,
  arc: Arc<Mutex<()>>,
}

impl Drop for InflightGuard<'_> {
  fn drop(&mut self) {
    self
      .map
      .remove_if(&self.key, |_, v| Arc::ptr_eq(v, &self.arc));
  }
}

impl Router {
  /// Create a router backed by the database and health prober.
  ///
  /// # Errors
  ///
  /// Returns an error if the HTTP client cannot be constructed.
  pub fn new(
    db: Db,
    prober: Prober,
    route_ttl: Duration,
    race_timeout: Duration,
    negative_ttl: Duration,
    tuning: RouterTuning,
  ) -> Result<Self, reqwest::Error> {
    Ok(Self {
      inner: Arc::new(RouterInner {
        db,
        prober,
        route_ttl,
        race_timeout,
        negative_ttl,
        client: reqwest::Client::builder().timeout(race_timeout).build()?,
        upstream_clients: RwLock::new(HashMap::new()),
        s3: S3ClientPool::default(),
        upstream_keys: RwLock::new(HashMap::new()),
        upstream_auth: RwLock::new(HashMap::new()),
        upstream_filters: RwLock::new(HashMap::new()),
        upstream_nar_url_modes: RwLock::new(HashMap::new()),
        inflight: DashMap::new(),
        lru: MokaCache::builder()
          .max_capacity(1024)
          .time_to_live(route_ttl)
          .build(),
        miss_lru: MokaCache::builder()
          .max_capacity(32_768)
          .time_to_live(tuning.in_memory_negative_ttl)
          .build(),
        race_semaphore: Arc::new(Semaphore::new(
          usize::try_from(tuning.max_concurrent_races).unwrap_or(64),
        )),
        per_upstream_limit: tuning.per_upstream_max_inflight,
        upstream_semaphores: DashMap::new(),
        upstream_cooldown: DashMap::new(),
        upstream_cooldown_window: tuning.upstream_cooldown,
      }),
    })
  }

  /// # Errors
  ///
  /// Returns [`UpstreamRegistrationError`] if the upstream public key is
  /// invalid or a per-upstream HTTP client cannot be built.
  pub async fn register_upstream(
    &self,
    upstream: &(impl RouterUpstream + Sync + ?Sized),
  ) -> Result<(), UpstreamRegistrationError> {
    let url = upstream.url().to_string();
    if let Some(s3) = upstream.s3() {
      self.inner.s3.register(url.clone(), s3.clone());
    }
    self
      .register_upstream_keys(
        url.clone(),
        upstream.public_key().to_string(),
        upstream.public_keys().to_vec(),
      )
      .await?;
    self
      .register_upstream_auth(
        url.clone(),
        upstream.username().to_string(),
        upstream.password().map(str::to_string),
      )
      .await;
    self
      .register_upstream_filters(url.clone(), upstream.filters().to_vec())
      .await;
    self
      .register_upstream_nar_url_mode(url.clone(), upstream.nar_url_mode())
      .await;
    self
      .register_upstream_narinfo_timeout(url, upstream.narinfo_timeout())
      .await?;
    Ok(())
  }

  /// # Errors
  ///
  /// Returns [`NarInfoError`] if any public key is not in valid
  /// `name:base64` Nix format.
  async fn register_upstream_keys(
    &self,
    url: String,
    public_key: String,
    public_keys: Vec<String>,
  ) -> Result<(), NarInfoError> {
    let keys = normalized_public_keys(public_key, public_keys);
    for key in &keys {
      parse_public_key(key)?;
    }
    let mut map = self.inner.upstream_keys.write().await;
    if keys.is_empty() {
      map.remove(&url);
    } else {
      map.insert(url, keys);
    }
    drop(map);
    Ok(())
  }

  async fn register_upstream_auth(
    &self,
    url: String,
    username: String,
    password: Option<String>,
  ) {
    let mut map = self.inner.upstream_auth.write().await;
    if username.is_empty() {
      map.remove(&url);
    } else {
      map.insert(url, (username, password));
    }
  }

  pub(crate) async fn register_upstream_filters(
    &self,
    url: String,
    filters: Vec<FilterRule>,
  ) {
    let mut map = self.inner.upstream_filters.write().await;
    if filters.is_empty() {
      map.remove(&url);
    } else {
      map.insert(url, filters);
    }
  }

  async fn register_upstream_nar_url_mode(
    &self,
    url: String,
    mode: NarUrlMode,
  ) {
    // `Keep` has to be stored, not skipped, since absence means the default.
    self
      .inner
      .upstream_nar_url_modes
      .write()
      .await
      .insert(url, mode);
  }

  /// Build and register a per-upstream HTTP client with a custom narinfo
  /// timeout. When set, this client is used in place of the default
  /// race-timeout client for HEAD races and GET fetches against `url`.
  ///
  /// # Errors
  ///
  /// Returns an error if the per-upstream client cannot be built.
  async fn register_upstream_narinfo_timeout(
    &self,
    url: String,
    timeout: Option<Duration>,
  ) -> Result<(), reqwest::Error> {
    let mut map = self.inner.upstream_clients.write().await;
    if let Some(timeout) = timeout {
      map.insert(url, reqwest::Client::builder().timeout(timeout).build()?);
    } else {
      map.remove(&url);
    }
    drop(map);
    Ok(())
  }

  /// Resolve through a last-resort fallback cache without using route cache,
  /// health, priority, cooldown, filters, or route persistence.
  ///
  /// # Errors
  ///
  /// Returns [`RouterError::NotFound`] if the fallback cache does not have the
  /// narinfo, or propagates fetch/parse/signature errors from the fallback.
  pub async fn resolve_fallback(
    &self,
    store_hash: &str,
    upstream: &str,
  ) -> Result<ResolveResult, RouterError> {
    let start = Instant::now();
    let (body, _) = self.fetch_narinfo(upstream, store_hash).await?;
    let narinfo_bytes = self
      .response_narinfo_bytes(upstream, store_hash, body.as_deref())
      .await;
    Ok(ResolveResult {
      url: upstream.to_string(),
      latency_ms: start.elapsed().as_secs_f64() * 1000.0,
      cache_hit: false,
      narinfo_bytes,
    })
  }

  /// Resolve a narinfo hash to an upstream URL by checking the route cache
  /// then racing all candidates.
  ///
  /// # Errors
  ///
  /// Returns [`RouterError::NotFound`] if no upstream has the path,
  /// [`RouterError::UpstreamUnavailable`] if all upstreams failed, or a
  /// database/network error propagated from a dependency.
  pub async fn resolve(
    &self,
    store_hash: &str,
    candidates: &[String],
  ) -> Result<ResolveResult, RouterError> {
    if self.inner.miss_lru.get(store_hash).await.is_some() {
      ncro_metrics::get().narinfo_memory_negative_hits.inc();
      return Err(RouterError::NotFound);
    }
    if self.inner.db.is_negative(store_hash).await? {
      return Err(RouterError::NotFound);
    }
    if let Some(result) = self.valid_cached_route(store_hash).await? {
      return Ok(result);
    }
    ncro_metrics::get().narinfo_cache_misses.inc();

    let lock = match self.inner.inflight.entry(store_hash.to_string()) {
      Entry::Occupied(entry) => {
        ncro_metrics::get().narinfo_singleflight_waiters.inc();
        Arc::clone(entry.get())
      },
      Entry::Vacant(entry) => {
        let inserted = entry.insert(Arc::new(Mutex::new(())));
        Arc::clone(&inserted)
      },
    };
    let _guard = lock.lock().await;
    let _cleanup = InflightGuard {
      map: &self.inner.inflight,
      key: store_hash.to_string(),
      arc: Arc::clone(&lock),
    };
    if let Some(result) = self.valid_cached_route(store_hash).await? {
      return Ok(result);
    }

    let result = self.race(store_hash, candidates).await;
    if matches!(result, Err(RouterError::NotFound)) {
      self.inner.miss_lru.insert(store_hash.to_string(), ()).await;
      let _ = self
        .inner
        .db
        .set_negative(store_hash, self.inner.negative_ttl)
        .await;
    }
    result
  }

  async fn valid_cached_route(
    &self,
    store_hash: &str,
  ) -> Result<Option<ResolveResult>, RouterError> {
    if let Some(cached) = self.inner.lru.get(store_hash).await {
      let CachedFilterCheck::Accepted(narinfo_bytes) = self
        .cached_route_filter_check(
          &cached.url,
          store_hash,
          cached.narinfo_bytes.as_deref(),
        )
        .await
      else {
        self.inner.lru.invalidate(store_hash).await;
        return Ok(None);
      };
      ncro_metrics::get().narinfo_cache_hits.inc();
      let mut result = (*cached).clone();
      if result.narinfo_bytes.is_none() {
        result.narinfo_bytes = self
          .response_narinfo_bytes(
            &cached.url,
            store_hash,
            narinfo_bytes.as_deref(),
          )
          .await;
        self
          .inner
          .lru
          .insert(store_hash.to_string(), Arc::new(result.clone()))
          .await;
      }
      return Ok(Some(result));
    }
    let Some(mut entry) = self.inner.db.get_route(store_hash).await? else {
      return Ok(None);
    };
    if !entry.is_valid() {
      return Ok(None);
    }
    let health = self.inner.prober.get_health(&entry.upstream_url).await;
    if health.as_ref().is_some_and(|h| h.status == Status::Down) {
      return Ok(None);
    }
    let CachedFilterCheck::Accepted(narinfo_bytes) = self
      .cached_route_filter_check(
        &entry.upstream_url,
        store_hash,
        entry.narinfo_bytes.as_deref(),
      )
      .await
    else {
      return Ok(None);
    };
    if entry.narinfo_bytes.is_none() && narinfo_bytes.is_some() {
      entry.narinfo_bytes = narinfo_bytes;
      self.inner.db.set_route(&entry).await?;
    }
    ncro_metrics::get().narinfo_cache_hits.inc();
    let narinfo_bytes = self
      .response_narinfo_bytes(
        &entry.upstream_url,
        store_hash,
        entry.narinfo_bytes.as_deref(),
      )
      .await;
    let result = ResolveResult {
      url: entry.upstream_url.clone(),
      latency_ms: entry.latency_ema,
      cache_hit: true,
      narinfo_bytes,
    };
    let arc = Arc::new(result.clone());
    self.inner.lru.insert(store_hash.to_string(), arc).await;
    Ok(Some(result))
  }

  async fn race(
    &self,
    store_hash: &str,
    candidates: &[String],
  ) -> Result<ResolveResult, RouterError> {
    if candidates.is_empty() {
      return Err(RouterError::NoCandidates(store_hash.to_string()));
    }
    let wait_start = Instant::now();
    let _race_permit = Arc::clone(&self.inner.race_semaphore)
      .acquire_owned()
      .await
      .map_err(|_| RouterError::UpstreamUnavailable)?;
    ncro_metrics::get()
      .narinfo_race_wait_seconds
      .with_label_values(&["global"])
      .observe(wait_start.elapsed().as_secs_f64());

    let filtered = self.cooldown_filtered_candidates(candidates);
    let effective_candidates = if filtered.is_empty() {
      candidates.to_vec()
    } else {
      filtered
    };

    // Group candidates by priority. Lower number means higher priority, tried
    // first. Upstreams whose health entry is missing get i32::MAX so that they
    // fall into the lowest-priority group rather than being silently dropped.
    let mut groups: BTreeMap<i32, Vec<String>> = BTreeMap::new();
    for url in &effective_candidates {
      let priority = self
        .inner
        .prober
        .get_health(url)
        .await
        .map_or(i32::MAX, |h| h.priority);
      groups.entry(priority).or_default().push(url.clone());
    }

    let mut any_not_found = false;
    let mut attempts_total = 0_u32;
    for (_priority, group) in groups {
      let mut group_candidates = group;
      while !group_candidates.is_empty() {
        let (group_result, attempts) =
          self.race_group(store_hash, &group_candidates).await;
        attempts_total += attempts;
        match group_result {
          Ok(winner) => {
            let winner_url = winner.url.clone();
            match self.commit_winner(winner, store_hash).await {
              Ok(CommitOutcome::Accepted(result)) => {
                ncro_metrics::get()
                  .narinfo_upstream_attempts_per_resolve
                  .with_label_values(&["success"])
                  .observe(f64::from(attempts_total));
                return Ok(result);
              },
              Ok(CommitOutcome::Rejected) => {
                any_not_found = true;
                group_candidates.retain(|url| url != &winner_url);
              },
              Err(err) if commit_error_is_retryable(&err) => {
                if commit_error_is_network_like(&err) {
                  self.mark_cooldown(&winner_url);
                } else {
                  any_not_found = true;
                }
                tracing::warn!(
                  upstream = &winner_url,
                  store = store_hash,
                  error = %err,
                  "narinfo winner could not be committed; trying next candidate"
                );
                group_candidates.retain(|url| url != &winner_url);
              },
              Err(err) => return Err(err),
            }
          },
          Err(RaceGroupError::NotFound) => {
            any_not_found = true;
            break;
          },
          // Try the next priority group on network error; those upstreams were
          // unreachable so we cannot conclude the path is absent.
          Err(RaceGroupError::NetworkError | RaceGroupError::Timeout) => break,
        }
      }
    }
    ncro_metrics::get()
      .narinfo_upstream_attempts_per_resolve
      .with_label_values(&[if any_not_found {
        "not_found"
      } else {
        "unavailable"
      }])
      .observe(f64::from(attempts_total));

    if any_not_found {
      Err(RouterError::NotFound)
    } else {
      Err(RouterError::UpstreamUnavailable)
    }
  }

  /// Race all upstreams in `group` in parallel. Returns the first winner or
  /// a classification of the failure.
  async fn race_group(
    &self,
    store_hash: &str,
    group: &[String],
  ) -> (Result<RaceResult, RaceGroupError>, u32) {
    let auth_snapshot = self.inner.upstream_auth.read().await.clone();
    let clients_snapshot = self.inner.upstream_clients.read().await.clone();
    let mut handles = FuturesUnordered::new();
    for upstream in group {
      let upstream = upstream.clone();
      let store_hash = store_hash.to_string();
      let client = clients_snapshot
        .get(&upstream)
        .cloned()
        .unwrap_or_else(|| self.inner.client.clone());
      let s3 = self.inner.s3.clone();
      let gate = self.upstream_gate(&upstream);
      let auth = auth_snapshot.get(&upstream).cloned();
      handles.push(tokio::spawn(async move {
        let Ok(_permit) = gate.acquire_owned().await else {
          return RaceAttempt::NetworkError { upstream };
        };
        let start = Instant::now();
        if s3.contains(&upstream) {
          match s3
            .head_object(&upstream, &format!("{store_hash}.narinfo"))
            .await
          {
            Ok(true) => {
              RaceAttempt::Winner(RaceResult {
                url:        upstream,
                latency_ms: start.elapsed().as_secs_f64() * 1000.0,
              })
            },
            Ok(false) => RaceAttempt::NotFound,
            Err(_) => RaceAttempt::NetworkError { upstream },
          }
        } else {
          let mut req = client.head(format!("{upstream}/{store_hash}.narinfo"));
          if let Some((user, pass)) = auth {
            req = req.basic_auth(user, pass);
          }
          let res = req.send().await;
          match res {
            Ok(resp) if resp.status().is_success() => {
              RaceAttempt::Winner(RaceResult {
                url:        upstream,
                latency_ms: start.elapsed().as_secs_f64() * 1000.0,
              })
            },
            Ok(_) => RaceAttempt::NotFound, // 404 / non-success = not found
            Err(_) => RaceAttempt::NetworkError { upstream }, // network error
          }
        }
      }));
    }

    let mut net_errs = 0usize;
    let mut not_founds = 0usize;
    let mut attempts = 0_u32;
    let deadline = time::sleep(self.inner.race_timeout);
    tokio::pin!(deadline);

    let winner = loop {
      if handles.is_empty() {
        break None;
      }
      tokio::select! {
          () = &mut deadline => break None,
          joined = handles.next() => {
              match joined {
                  Some(Ok(RaceAttempt::Winner(res))) => {
                      attempts += 1;
                      ncro_metrics::get().narinfo_upstream_attempts.inc();
                      break Some(res)
                  },
                  Some(Ok(RaceAttempt::NetworkError { upstream })) => {
                      attempts += 1;
                      ncro_metrics::get().narinfo_upstream_attempts.inc();
                      net_errs += 1;
                      self.mark_cooldown(&upstream);
                  },
                  Some(Ok(RaceAttempt::NotFound)) => {
                      attempts += 1;
                      ncro_metrics::get().narinfo_upstream_attempts.inc();
                      not_founds += 1;
                  },
                  Some(Err(_)) => {
                      attempts += 1;
                      ncro_metrics::get().narinfo_upstream_attempts.inc();
                      net_errs += 1;
                  },
                  None => break None,
              }
          }
      }
    };

    if let Some(winner) = winner {
      return (Ok(winner), attempts);
    }

    // If there is no winner classify the failure so the caller can decide
    // whether to try the next priority group.
    if net_errs > 0 && not_founds == 0 {
      (Err(RaceGroupError::NetworkError), attempts)
    } else if not_founds > 0 {
      (Err(RaceGroupError::NotFound), attempts)
    } else {
      (Err(RaceGroupError::Timeout), attempts)
    }
  }

  fn cooldown_filtered_candidates(&self, candidates: &[String]) -> Vec<String> {
    candidates
      .iter()
      .filter(|url| !self.in_cooldown(url))
      .cloned()
      .collect()
  }

  fn in_cooldown(&self, url: &str) -> bool {
    if let Some(until) = self.inner.upstream_cooldown.get(url)
      && *until > Instant::now()
    {
      return true;
    }
    self.inner.upstream_cooldown.remove(url);
    false
  }

  fn mark_cooldown(&self, url: &str) {
    self.inner.upstream_cooldown.insert(
      url.to_string(),
      Instant::now() + self.inner.upstream_cooldown_window,
    );
  }

  fn upstream_gate(&self, upstream: &str) -> Arc<Semaphore> {
    match self.inner.upstream_semaphores.entry(upstream.to_string()) {
      Entry::Occupied(entry) => Arc::clone(entry.get()),
      Entry::Vacant(entry) => {
        Arc::clone(&entry.insert(Arc::new(Semaphore::new(
          usize::try_from(self.inner.per_upstream_limit).unwrap_or(8),
        ))))
      },
    }
  }

  /// Fetch the full narinfo, then record metrics, update the prober and DB
  /// for a race winner.
  ///
  /// Metrics and side-effects are only committed once the fetch succeeds, so
  /// a failure does not inflate the win/latency counters.
  async fn commit_winner(
    &self,
    winner: RaceResult,
    store_hash: &str,
  ) -> Result<CommitOutcome, RouterError> {
    let (body, parsed) = self.fetch_narinfo(&winner.url, store_hash).await?;
    if !self.upstream_allows_narinfo(&winner.url, &parsed).await {
      tracing::debug!(
        upstream = &winner.url,
        store_path = &parsed.store_path,
        "narinfo rejected by upstream filter"
      );
      return Ok(CommitOutcome::Rejected);
    }
    // harmonia appends `?hash=STORE_HASH` and needs it to locate the store
    // path, so the fetch keeps the query and only the lookup key drops it.
    let upstream_path = parsed.url.trim_start_matches('/');
    let key = upstream_path
      .split_once('?')
      .map_or(upstream_path, |(path, _)| path);
    let nar_url = match self.nar_url_mode(&winner.url).await {
      NarUrlMode::ToSelf => canonical_nar_url(&parsed.url, store_hash),
      NarUrlMode::Keep | NarUrlMode::ToUpstream => key.to_string(),
    };
    // Stored `/`-prefixed and host-stripped because the NAR handler appends it
    // to the upstream base. An upstream may publish an absolute URL here.
    let upstream_nar_url = format!("/{}", relative_nar_url(&parsed.url));

    ncro_metrics::get()
      .upstream_race_wins
      .with_label_values(&[&winner.url])
      .inc();
    ncro_metrics::get()
      .upstream_latency
      .with_label_values(&[&winner.url])
      .observe(winner.latency_ms / 1000.0);

    let ema = self.inner.prober.get_health(&winner.url).await.map_or(
      winner.latency_ms,
      |h| {
        self.inner.prober.alpha().mul_add(
          winner.latency_ms,
          (1.0 - self.inner.prober.alpha()) * h.ema_latency,
        )
      },
    );
    self
      .inner
      .prober
      .record_latency(&winner.url, winner.latency_ms)
      .await;
    let now = Utc::now();
    self
      .inner
      .db
      .set_route(&RouteEntry {
        store_path: store_hash.to_string(),
        upstream_url: winner.url.clone(),
        latency_ms: winner.latency_ms,
        latency_ema: ema,
        last_verified: now,
        query_count: 1,
        failure_count: 0,
        ttl: now
          + chrono::Duration::from_std(self.inner.route_ttl)
            .unwrap_or_default(),
        nar_hash: parsed.nar_hash,
        nar_size: parsed.nar_size,
        nar_url,
        upstream_nar_url,
        narinfo_bytes: body.clone(),
      })
      .await?;
    let result = ResolveResult {
      url:           winner.url.clone(),
      latency_ms:    winner.latency_ms,
      cache_hit:     false,
      narinfo_bytes: self
        .response_narinfo_bytes(&winner.url, store_hash, body.as_deref())
        .await,
    };
    self
      .inner
      .lru
      .insert(store_hash.to_string(), Arc::new(result.clone()))
      .await;
    Ok(CommitOutcome::Accepted(result))
  }

  async fn upstream_allows_narinfo(
    &self,
    upstream: &str,
    narinfo: &NarInfo,
  ) -> bool {
    let rules = {
      let filters = self.inner.upstream_filters.read().await;
      filters.get(upstream).cloned()
    };
    let Some(rules) = rules else {
      return true;
    };
    if rules.is_empty() {
      return true;
    }

    let has_allow = rules
      .iter()
      .any(|rule| matches!(rule.action, FilterAction::Allow));
    let mut allow_matched = false;
    for rule in &rules {
      if !filter_rule_matches(rule, narinfo) {
        continue;
      }
      match rule.action {
        FilterAction::Deny => return false,
        FilterAction::Allow => allow_matched = true,
      }
    }

    !has_allow || allow_matched
  }

  async fn cached_route_filter_check(
    &self,
    upstream: &str,
    store_hash: &str,
    narinfo_bytes: Option<&[u8]>,
  ) -> CachedFilterCheck {
    if !self
      .inner
      .upstream_filters
      .read()
      .await
      .contains_key(upstream)
    {
      return CachedFilterCheck::Accepted(narinfo_bytes.map(<[u8]>::to_vec));
    }
    if let Some(bytes) = narinfo_bytes
      && let Ok(narinfo) = NarInfo::parse(bytes)
      && self.upstream_allows_narinfo(upstream, &narinfo).await
    {
      return CachedFilterCheck::Accepted(Some(bytes.to_vec()));
    }

    if let Ok((body, parsed)) = self.fetch_narinfo(upstream, store_hash).await
      && self.upstream_allows_narinfo(upstream, &parsed).await
    {
      return CachedFilterCheck::Accepted(body);
    }
    CachedFilterCheck::Rejected
  }

  /// Ask one upstream where it keeps the NAR for `store_hash`.
  ///
  /// A canonical path doesn't carry the upstream's layout, so retrying a
  /// different upstream has to re-read its narinfo.
  ///
  /// # Errors
  ///
  /// Returns [`RouterError::NotFound`] if the upstream does not have the path,
  /// or propagates fetch, parse, and signature errors.
  pub async fn upstream_nar_path(
    &self,
    upstream: &str,
    store_hash: &str,
  ) -> Result<String, RouterError> {
    let (_, parsed) = self.fetch_narinfo(upstream, store_hash).await?;
    Ok(format!("/{}", relative_nar_url(&parsed.url)))
  }

  async fn fetch_narinfo(
    &self,
    upstream: &str,
    store_hash: &str,
  ) -> Result<(Option<Vec<u8>>, NarInfo), RouterError> {
    let body = if self.inner.s3.contains(upstream) {
      self
        .inner
        .s3
        .get_object_bytes(upstream, &format!("{store_hash}.narinfo"))
        .await?
        .ok_or(RouterError::NotFound)?
    } else {
      let auth = self.inner.upstream_auth.read().await.get(upstream).cloned();
      let client = self
        .inner
        .upstream_clients
        .read()
        .await
        .get(upstream)
        .cloned()
        .unwrap_or_else(|| self.inner.client.clone());
      let mut req = client.get(format!("{upstream}/{store_hash}.narinfo"));
      if let Some((user, pass)) = auth {
        req = req.basic_auth(user, pass);
      }
      let resp = req.send().await?;
      if !resp.status().is_success() {
        return Err(RouterError::NotFound);
      }
      resp.bytes().await?.to_vec()
    };
    let parsed = NarInfo::parse(body.as_slice())?;
    if let Some(public_keys) =
      self.inner.upstream_keys.read().await.get(upstream)
      && !narinfo_verifies_any_key(&parsed, public_keys)
    {
      tracing::warn!(
        upstream,
        store = store_hash,
        "narinfo signature verification failed"
      );
      return Err(RouterError::SignatureVerificationFailed);
    }
    Ok((Some(body), parsed))
  }

  async fn response_narinfo_bytes(
    &self,
    upstream: &str,
    store_hash: &str,
    body: Option<&[u8]>,
  ) -> Option<Vec<u8>> {
    let body = body?;
    let mode = self.nar_url_mode(upstream).await;
    if mode == NarUrlMode::Keep {
      return Some(body.to_vec());
    }
    Some(rewrite_narinfo_url(body, upstream, store_hash, mode))
  }

  async fn nar_url_mode(&self, upstream: &str) -> NarUrlMode {
    let map = self.inner.upstream_nar_url_modes.read().await;
    map.get(upstream).copied().unwrap_or_default()
  }
}

fn normalized_public_keys(
  public_key: String,
  public_keys: Vec<String>,
) -> Vec<String> {
  let mut keys =
    Vec::with_capacity(usize::from(!public_key.is_empty()) + public_keys.len());
  if !public_key.is_empty() {
    keys.push(public_key);
  }
  keys.extend(public_keys);
  keys.sort();
  keys.dedup();
  keys
}

fn narinfo_verifies_any_key(narinfo: &NarInfo, public_keys: &[String]) -> bool {
  public_keys
    .iter()
    .any(|public_key| narinfo.verify(public_key).unwrap_or(false))
}

fn rewrite_narinfo_url(
  body: &[u8],
  upstream: &str,
  store_hash: &str,
  mode: NarUrlMode,
) -> Vec<u8> {
  if mode == NarUrlMode::Keep {
    return body.to_vec();
  }
  let Ok(text) = str::from_utf8(body) else {
    return body.to_vec();
  };
  let mut out = String::with_capacity(text.len() + upstream.len());
  for line in text.split_inclusive('\n') {
    let (line, newline) = line.strip_suffix('\n').map_or((line, ""), |line| {
      line
        .strip_suffix('\r')
        .map_or((line, "\n"), |line| (line, "\r\n"))
    });
    if let Some(url) = line.strip_prefix("URL: ") {
      out.push_str("URL: ");
      out.push_str(&rewrite_nar_url_value(url, upstream, store_hash, mode));
    } else {
      out.push_str(line);
    }
    out.push_str(newline);
  }
  out.into_bytes()
}

fn rewrite_nar_url_value(
  url: &str,
  upstream: &str,
  store_hash: &str,
  mode: NarUrlMode,
) -> String {
  match mode {
    NarUrlMode::Keep => url.to_string(),
    NarUrlMode::ToSelf => canonical_nar_url(url, store_hash),
    NarUrlMode::ToUpstream => {
      format!(
        "{}/{}",
        upstream.trim_end_matches('/'),
        relative_nar_url(url).trim_start_matches('/')
      )
    },
  }
}

/// Build the `nar/<store-hash>` path NCRO serves for a NAR body.
///
/// Upstreams key NAR paths differently and a root-level one misses
/// `/nar/{*path}` entirely, so only the store hash works against all of them.
#[must_use]
pub fn canonical_nar_url(url: &str, store_hash: &str) -> String {
  format!("nar/{store_hash}{}", nar_extension(url))
}

fn nar_extension(url: &str) -> &str {
  let path = relative_nar_url(url);
  let path = path.split_once('?').map_or(path, |(path, _)| path);
  let file = path.rsplit('/').next().unwrap_or(path);
  file.rfind(".nar").map_or("", |idx| &file[idx..])
}

/// Recover the store hash from a [`canonical_nar_url`] path.
#[must_use]
pub fn store_hash_from_canonical_nar_url(path: &str) -> Option<&str> {
  let rest = path.trim_start_matches('/').strip_prefix("nar/")?;
  let hash = rest.split_once(".nar").map_or(rest, |(hash, _)| hash);
  (hash.len() == 32 && hash.bytes().all(|b| b.is_ascii_alphanumeric()))
    .then_some(hash)
}

fn relative_nar_url(url: &str) -> &str {
  if let Some((_, rest)) = url.split_once("://")
    && let Some((_, path)) = rest.split_once('/')
  {
    return path;
  }
  url.trim_start_matches('/')
}

fn filter_rule_matches(rule: &FilterRule, narinfo: &NarInfo) -> bool {
  match rule.field {
    FilterField::StorePath => {
      wildcard_match(&rule.pattern, &narinfo.store_path)
    },
    FilterField::Name => {
      wildcard_match(&rule.pattern, store_path_name(&narinfo.store_path))
    },
    FilterField::Reference => {
      narinfo
        .references
        .iter()
        .any(|reference| wildcard_match(&rule.pattern, reference))
    },
    FilterField::Deriver => wildcard_match(&rule.pattern, &narinfo.deriver),
  }
}

const fn commit_error_is_retryable(err: &RouterError) -> bool {
  matches!(
    err,
    RouterError::NotFound | RouterError::FetchNarinfo(_) | RouterError::S3(_)
  )
}

const fn commit_error_is_network_like(err: &RouterError) -> bool {
  matches!(err, RouterError::FetchNarinfo(_) | RouterError::S3(_))
}

fn store_path_name(store_path: &str) -> &str {
  let Some(base) = store_path.rsplit('/').next() else {
    return store_path;
  };
  base.split_once('-').map_or(base, |(_, name)| name)
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
  if pattern == "*" {
    return true;
  }
  let mut remainder = value;
  let parts = pattern.split('*');
  let anchored_start = !pattern.starts_with('*');
  let anchored_end = !pattern.ends_with('*');
  let mut first = true;

  for part in parts {
    if part.is_empty() {
      continue;
    }
    if first && anchored_start {
      let Some(next) = remainder.strip_prefix(part) else {
        return false;
      };
      remainder = next;
    } else if let Some(index) = remainder.find(part) {
      remainder = &remainder[index + part.len()..];
    } else {
      return false;
    }
    first = false;
  }

  !anchored_end || remainder.is_empty()
}

#[cfg(test)]
mod tests {
  #![expect(clippy::unwrap_used, reason = "Fine in tests")]
  use std::{slice, sync::Arc, time::Duration};

  use base64::{Engine as _, engine::general_purpose::STANDARD};
  use chrono::Utc;
  use ed25519_dalek::{Signer, SigningKey};
  use ncro_config::{NarUrlMode, UpstreamConfig};
  use ncro_db::{Db, RouteEntry};
  use ncro_health::Prober;
  use rand::RngExt;
  use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
  };

  use super::{
    FilterAction,
    FilterField,
    FilterRule,
    InflightGuard,
    NarInfo,
    Router,
    RouterTuning,
    canonical_nar_url,
    filter_rule_matches,
    narinfo_verifies_any_key,
    rewrite_narinfo_url,
    store_hash_from_canonical_nar_url,
    store_path_name,
    wildcard_match,
  };

  async fn make_router(cooldown: Duration) -> Router {
    make_router_with_upstreams(cooldown, &[]).await
  }

  async fn make_router_with_upstreams(
    cooldown: Duration,
    upstreams: &[UpstreamConfig],
  ) -> Router {
    let db = Db::open(":memory:", 100, Duration::from_secs(1))
      .await
      .unwrap();
    let prober = Prober::new(0.3).unwrap();
    prober.init_upstreams(upstreams).await;
    Router::new(
      db,
      prober,
      Duration::from_hours(1),
      Duration::from_secs(5),
      Duration::from_mins(10),
      RouterTuning {
        max_concurrent_races:      4,
        per_upstream_max_inflight: 2,
        in_memory_negative_ttl:    Duration::from_mins(5),
        upstream_cooldown:         cooldown,
      },
    )
    .unwrap()
  }

  async fn spawn_narinfo_server(get_status: u16, store_name: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store_name = store_name.to_string();
    tokio::spawn(async move {
      loop {
        let Ok((mut stream, _)) = listener.accept().await else {
          return;
        };
        let store_name = store_name.clone();
        tokio::spawn(async move {
          let mut buf = [0_u8; 1024];
          let Ok(n) = stream.read(&mut buf).await else {
            return;
          };
          let request = String::from_utf8_lossy(&buf[..n]);
          if request.starts_with("HEAD ") {
            let _ = stream
              .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
              .await;
            return;
          }
          if request.starts_with("GET ") && get_status == 200 {
            let body = format!(
              "StorePath: /nix/store/abc123-{store_name}\nURL: \
               nar/test.nar.xz\nCompression: xz\nNarHash: \
               sha256:abc\nNarSize: 1\nReferences: \n"
            );
            let response = format!(
              "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
              body.len(),
              body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            return;
          }
          let _ = stream
            .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
            .await;
        });
      }
    });
    format!("http://{addr}")
  }

  async fn spawn_head_ok_get_drop_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
      loop {
        let Ok((mut stream, _)) = listener.accept().await else {
          return;
        };
        tokio::spawn(async move {
          let mut buf = [0_u8; 1024];
          let Ok(n) = stream.read(&mut buf).await else {
            return;
          };
          let request = String::from_utf8_lossy(&buf[..n]);
          if request.starts_with("HEAD ") {
            let _ = stream
              .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
              .await;
          }
        });
      }
    });
    format!("http://{addr}")
  }

  fn signed_narinfo(key_name: &str) -> (NarInfo, String) {
    let mut key_bytes = [0_u8; 32];
    rand::rng().fill(&mut key_bytes);
    let signing = SigningKey::from_bytes(&key_bytes);
    let mut narinfo = NarInfo {
      store_path: "/nix/store/abc123-test".to_string(),
      nar_hash: "sha256:abc".to_string(),
      nar_size: 12,
      references: vec![],
      ..Default::default()
    };
    let sig = signing.sign(narinfo.fingerprint().as_bytes());
    narinfo.sig =
      vec![format!("{key_name}:{}", STANDARD.encode(sig.to_bytes()))];
    let public_key = format!(
      "{key_name}:{}",
      STANDARD.encode(signing.verifying_key().to_bytes())
    );
    (narinfo, public_key)
  }

  #[test]
  fn extracts_store_path_name() {
    assert_eq!(
      store_path_name("/nix/store/abc123-zedless-0.1.0"),
      "zedless-0.1.0"
    );
  }

  #[test]
  fn wildcard_patterns_match_expected_values() {
    assert!(wildcard_match("zedless*", "zedless-0.1.0"));
    assert!(wildcard_match("*-source", "foo-source"));
    assert!(wildcard_match("*zed*", "my-zedless-package"));
    assert!(!wildcard_match("zedless", "zedless-0.1.0"));
  }

  #[test]
  fn narinfo_verification_accepts_any_configured_public_key() {
    let (narinfo, matching_key) = signed_narinfo("origin");
    let (_, other_key) = signed_narinfo("other");

    assert!(narinfo_verifies_any_key(&narinfo, &[
      other_key.clone(),
      matching_key
    ]));
    assert!(!narinfo_verifies_any_key(&narinfo, &[other_key]));
  }

  #[test]
  fn filter_rules_match_selected_fields() {
    let narinfo = NarInfo {
      store_path: "/nix/store/abc123-zedless-0.1.0".to_string(),
      references: vec!["dep-one".to_string()],
      deriver: "abc123-zedless.drv".to_string(),
      ..Default::default()
    };

    assert!(filter_rule_matches(
      &FilterRule {
        action:  FilterAction::Allow,
        field:   FilterField::Name,
        pattern: "zedless*".to_string(),
      },
      &narinfo,
    ));
    assert!(filter_rule_matches(
      &FilterRule {
        action:  FilterAction::Deny,
        field:   FilterField::Reference,
        pattern: "dep-*".to_string(),
      },
      &narinfo,
    ));
  }

  const STORE_HASH: &str = "ad4slq98kiq9ypdd35yfg0bykdwj86ba";

  #[test]
  fn narinfo_url_to_self_rewrites_to_canonical_path() {
    // An absolute URL with no `nar/` prefix
    let body = b"StorePath: /nix/store/abc-hello\nURL: https://blobs.example/ad4slq98kiq9ypdd35yfg0bykdwj86ba-hello-1.0.nar.xz\nNarHash: sha256:abc\nNarSize: 1\n";

    let rewritten = rewrite_narinfo_url(
      body,
      "https://cache.example",
      STORE_HASH,
      NarUrlMode::ToSelf,
    );

    let rewritten = String::from_utf8(rewritten).unwrap();
    assert!(rewritten.contains(&format!("URL: nar/{STORE_HASH}.nar.xz\n")));
  }

  #[test]
  fn canonical_nar_url_round_trips() {
    let url =
      canonical_nar_url("https://blobs.example/x-foo.nar.zst", STORE_HASH);
    assert_eq!(url, format!("nar/{STORE_HASH}.nar.zst"));
    assert_eq!(store_hash_from_canonical_nar_url(&url), Some(STORE_HASH));

    // A `nar/` path keyed on the 52-character NAR hash belongs to an upstream,
    // so a path NCRO didn't mint must not be taken for one it did.
    assert_eq!(
      store_hash_from_canonical_nar_url(
        "nar/1bpq616dpxk1pn7f9w8pw1zjs9x2q3vv3f8kmc1a9k6ha2b4mmzz.nar.xz"
      ),
      None
    );
  }

  #[test]
  fn narinfo_url_to_upstream_uses_selected_upstream() {
    let body = b"StorePath: /nix/store/abc-hello\nURL: /nar/abc.nar.xz\nNarHash: sha256:abc\nNarSize: 1\n";

    let rewritten = rewrite_narinfo_url(
      body,
      "https://cache.example/root/",
      STORE_HASH,
      NarUrlMode::ToUpstream,
    );

    let rewritten = String::from_utf8(rewritten).unwrap();
    assert!(
      rewritten.contains("URL: https://cache.example/root/nar/abc.nar.xz\n")
    );
  }

  #[tokio::test]
  async fn resolve_retries_after_winner_get_returns_not_found() {
    let failing = spawn_narinfo_server(500, "wrong").await;
    let working = spawn_narinfo_server(200, "zedless-0.1.0").await;
    let router = make_router_with_upstreams(Duration::from_mins(1), &[
      UpstreamConfig {
        url: failing.clone(),
        priority: 1,
        ..Default::default()
      },
      UpstreamConfig {
        url: working.clone(),
        priority: 2,
        ..Default::default()
      },
    ])
    .await;

    let result = router
      .resolve("abc123", &[failing.clone(), working.clone()])
      .await
      .unwrap();

    assert_eq!(result.url, working);
    assert!(!router.in_cooldown(&failing));
  }

  #[tokio::test]
  async fn resolve_retries_after_winner_get_network_error() {
    let failing = spawn_head_ok_get_drop_server().await;
    let working = spawn_narinfo_server(200, "zedless-0.1.0").await;
    let router = make_router_with_upstreams(Duration::from_mins(1), &[
      UpstreamConfig {
        url: failing.clone(),
        priority: 1,
        ..Default::default()
      },
      UpstreamConfig {
        url: working.clone(),
        priority: 2,
        ..Default::default()
      },
    ])
    .await;

    let result = router
      .resolve("abc123", &[failing.clone(), working.clone()])
      .await
      .unwrap();

    assert_eq!(result.url, working);
    assert!(router.in_cooldown(&failing));
  }

  #[tokio::test]
  async fn resolve_retries_after_filter_rejects_winner() {
    let rejected = spawn_narinfo_server(200, "unrelated-1.0").await;
    let accepted = spawn_narinfo_server(200, "zedless-0.1.0").await;
    let router = make_router_with_upstreams(Duration::from_mins(1), &[
      UpstreamConfig {
        url: rejected.clone(),
        priority: 1,
        ..Default::default()
      },
      UpstreamConfig {
        url: accepted.clone(),
        priority: 2,
        ..Default::default()
      },
    ])
    .await;
    router
      .register_upstream_filters(rejected.clone(), vec![FilterRule {
        action:  FilterAction::Allow,
        field:   FilterField::Name,
        pattern: "zedless*".to_string(),
      }])
      .await;

    let result = router
      .resolve("abc123", &[rejected, accepted.clone()])
      .await
      .unwrap();

    assert_eq!(result.url, accepted);
  }

  #[tokio::test]
  async fn fallback_resolve_ignores_filters_and_does_not_persist_route() {
    let fallback = spawn_narinfo_server(200, "unrelated-1.0").await;
    let router = make_router(Duration::from_mins(1)).await;
    router
      .register_upstream_filters(fallback.clone(), vec![FilterRule {
        action:  FilterAction::Allow,
        field:   FilterField::Name,
        pattern: "zedless*".to_string(),
      }])
      .await;

    let result = router.resolve_fallback("abc123", &fallback).await.unwrap();

    assert_eq!(result.url, fallback);
    assert!(!result.cache_hit);
    assert!(result.narinfo_bytes.is_some());
    assert!(router.inner.db.get_route("abc123").await.unwrap().is_none());
  }

  #[tokio::test]
  async fn cached_route_is_revalidated_against_new_filters() {
    let previously_accepted = spawn_narinfo_server(200, "unrelated-1.0").await;
    let accepted = spawn_narinfo_server(200, "zedless-0.1.0").await;
    let router = make_router_with_upstreams(Duration::from_mins(1), &[
      UpstreamConfig {
        url: previously_accepted.clone(),
        priority: 1,
        ..Default::default()
      },
      UpstreamConfig {
        url: accepted.clone(),
        priority: 2,
        ..Default::default()
      },
    ])
    .await;

    let cached = router
      .resolve("abc123", slice::from_ref(&previously_accepted))
      .await
      .unwrap();
    assert_eq!(cached.url, previously_accepted);

    router
      .register_upstream_filters(previously_accepted.clone(), vec![
        FilterRule {
          action:  FilterAction::Allow,
          field:   FilterField::Name,
          pattern: "zedless*".to_string(),
        },
      ])
      .await;

    let result = router
      .resolve("abc123", &[previously_accepted, accepted.clone()])
      .await
      .unwrap();

    assert_eq!(result.url, accepted);
    assert!(!result.cache_hit);
  }

  #[tokio::test]
  async fn cached_route_without_narinfo_bytes_uses_cache_after_filter_check() {
    let upstream = spawn_narinfo_server(200, "zedless-0.1.0").await;
    let router =
      make_router_with_upstreams(Duration::from_mins(1), &[UpstreamConfig {
        url: upstream.clone(),
        priority: 1,
        ..Default::default()
      }])
      .await;
    let now = Utc::now();
    router
      .inner
      .db
      .set_route(&RouteEntry {
        store_path:       "abc123".to_string(),
        upstream_url:     upstream.clone(),
        latency_ms:       5.0,
        latency_ema:      5.0,
        last_verified:    now,
        query_count:      1,
        failure_count:    0,
        ttl:              now + chrono::Duration::hours(1),
        nar_hash:         "sha256:abc".to_string(),
        nar_size:         1,
        nar_url:          "nar/test.nar.xz".to_string(),
        upstream_nar_url: "nar/test.nar.xz".to_string(),
        narinfo_bytes:    None,
      })
      .await
      .unwrap();
    router
      .register_upstream_filters(upstream.clone(), vec![FilterRule {
        action:  FilterAction::Allow,
        field:   FilterField::Name,
        pattern: "zedless*".to_string(),
      }])
      .await;

    let result = router.resolve("abc123", &[]).await.unwrap();

    assert_eq!(result.url, upstream);
    assert!(result.cache_hit);
    assert!(result.narinfo_bytes.is_some());
    assert!(
      router
        .inner
        .db
        .get_route("abc123")
        .await
        .unwrap()
        .unwrap()
        .narinfo_bytes
        .is_some()
    );
  }

  #[tokio::test]
  async fn cached_route_filter_check_failure_falls_back_to_race() {
    let stale = spawn_narinfo_server(500, "stale-1.0").await;
    let working = spawn_narinfo_server(200, "zedless-0.1.0").await;
    let router = make_router_with_upstreams(Duration::from_mins(1), &[
      UpstreamConfig {
        url: stale.clone(),
        priority: 1,
        ..Default::default()
      },
      UpstreamConfig {
        url: working.clone(),
        priority: 2,
        ..Default::default()
      },
    ])
    .await;
    let now = Utc::now();
    router
      .inner
      .db
      .set_route(&RouteEntry {
        store_path:       "abc123".to_string(),
        upstream_url:     stale.clone(),
        latency_ms:       5.0,
        latency_ema:      5.0,
        last_verified:    now,
        query_count:      1,
        failure_count:    0,
        ttl:              now + chrono::Duration::hours(1),
        nar_hash:         "sha256:abc".to_string(),
        nar_size:         1,
        nar_url:          "nar/test.nar.xz".to_string(),
        upstream_nar_url: "nar/test.nar.xz".to_string(),
        narinfo_bytes:    None,
      })
      .await
      .unwrap();
    router
      .register_upstream_filters(stale, vec![FilterRule {
        action:  FilterAction::Allow,
        field:   FilterField::Name,
        pattern: "zedless*".to_string(),
      }])
      .await;

    let result = router
      .resolve("abc123", slice::from_ref(&working))
      .await
      .unwrap();

    assert_eq!(result.url, working);
    assert!(!result.cache_hit);
  }

  #[test]
  fn inflight_guard_removes_entry_on_drop() {
    use dashmap::DashMap;
    let map: DashMap<String, Arc<Mutex<()>>> = DashMap::new();
    let key = "test_hash".to_string();
    let arc = Arc::new(Mutex::new(()));
    map.insert(key.clone(), Arc::clone(&arc));
    assert!(map.contains_key(&key));
    {
      let _guard = InflightGuard {
        map: &map,
        key: key.clone(),
        arc: Arc::clone(&arc),
      };
    }
    assert!(
      !map.contains_key(&key),
      "entry not removed after guard drop"
    );
  }

  #[tokio::test]
  async fn mark_cooldown_makes_upstream_unavailable() {
    let router = make_router(Duration::from_mins(1)).await;
    let url = "https://cache.example.com";
    assert!(!router.in_cooldown(url));
    router.mark_cooldown(url);
    assert!(router.in_cooldown(url));
  }

  #[tokio::test]
  async fn cooldown_expires_with_zero_window() {
    let router = make_router(Duration::ZERO).await;
    let url = "https://cache.example.com";
    // Deadline is Instant::now() + 0, already not in the future.
    router.mark_cooldown(url);
    assert!(!router.in_cooldown(url));
  }

  #[tokio::test]
  async fn cooldown_filter_excludes_cooled_down_upstream() {
    let router = make_router(Duration::from_mins(1)).await;
    let hot = "https://hot.example.com".to_string();
    let cold = "https://cold.example.com".to_string();
    router.mark_cooldown(&cold);
    let result = router.cooldown_filtered_candidates(&[hot.clone(), cold]);
    assert_eq!(result, vec![hot]);
  }

  #[tokio::test]
  async fn cooldown_filter_passes_all_when_none_cooled() {
    let router = make_router(Duration::from_mins(1)).await;
    let candidates = vec![
      "https://a.example.com".to_string(),
      "https://b.example.com".to_string(),
    ];
    assert_eq!(router.cooldown_filtered_candidates(&candidates), candidates);
  }

  #[tokio::test]
  async fn upstream_gate_is_stable_per_key() {
    let router = make_router(Duration::from_mins(1)).await;
    let url = "https://cache.example.com";
    let gate1 = router.upstream_gate(url);
    let gate2 = router.upstream_gate(url);
    assert!(Arc::ptr_eq(&gate1, &gate2));
  }

  #[tokio::test]
  async fn upstream_gate_is_distinct_per_upstream() {
    let router = make_router(Duration::from_mins(1)).await;
    let gate_a = router.upstream_gate("https://a.example.com");
    let gate_b = router.upstream_gate("https://b.example.com");
    assert!(!Arc::ptr_eq(&gate_a, &gate_b));
  }

  #[tokio::test]
  async fn upstream_gate_semaphore_capacity_matches_tuning() {
    let router = make_router(Duration::from_mins(1)).await;
    let gate = router.upstream_gate("https://cache.example.com");
    assert_eq!(gate.available_permits(), 2);
  }
}
