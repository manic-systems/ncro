use std::{
  collections::{BTreeMap, HashMap},
  io,
  sync::Arc,
  time::{Duration, Instant},
};

use axum::{
  Router as AxumRouter,
  body::Body,
  extract::{Path, State},
  http::{
    HeaderMap,
    HeaderName,
    HeaderValue,
    Method,
    Request,
    StatusCode,
    uri::PathAndQuery,
  },
  response::{IntoResponse, Response},
  routing::get,
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use ncro_config::{NarHedgingConfig, UpstreamConfig};
use ncro_db::Db;
use ncro_health::{Prober, Status, UpstreamHealth};
use ncro_router::{Router, RouterError, store_hash_from_canonical_nar_url};
use ncro_s3::S3ClientPool;
use serde::Serialize;
use tokio::{
  task::JoinSet,
  time::{Instant as TokioInstant, sleep_until},
};
use tower_http::timeout::{RequestBodyTimeoutLayer, ResponseBodyTimeoutLayer};
use url::Url;

#[derive(Clone, Copy, PartialEq, Eq)]
enum RouteKind {
  CacheHit,
  Direct,
  Hedge,
  Fallback,
  Race,
}

impl RouteKind {
  const fn header_value(self) -> &'static str {
    match self {
      Self::CacheHit => "cache-hit",
      Self::Direct => "direct",
      Self::Hedge => "hedge",
      Self::Fallback => "fallback",
      Self::Race => "race",
    }
  }
}

/// Add diagnostic source metadata without forwarding arbitrary upstream
/// headers.
fn with_provenance(
  mut response: Response,
  upstream: &str,
  route: RouteKind,
) -> Response {
  let source = Url::parse(upstream)
    .ok()
    .and_then(|url| url.host_str().map(str::to_owned))
    .unwrap_or_else(|| "unknown".to_string());
  let headers = response.headers_mut();
  headers.insert(
    HeaderName::from_static("x-ncro-upstream"),
    HeaderValue::from_str(&source)
      .unwrap_or_else(|_| HeaderValue::from_static("unknown")),
  );
  headers.insert(
    HeaderName::from_static("x-ncro-route"),
    HeaderValue::from_static(route.header_value()),
  );
  response
}

#[derive(Clone)]
pub struct AppState {
  router:          Router,
  prober:          Prober,
  db:              Db,
  upstreams:       Vec<UpstreamConfig>,
  fallback_cache:  Option<UpstreamConfig>,
  s3:              S3ClientPool,
  nar_clients:     HashMap<String, reqwest::Client>,
  default_client:  reqwest::Client,
  nar_hedging:     NarHedgingConfig,
  cache_priority:  i32,
  want_mass_query: bool,
  started:         Instant,
}

impl AppState {
  fn nar_client(&self, url: &str) -> &reqwest::Client {
    self.nar_clients.get(url).unwrap_or(&self.default_client)
  }
}

pub struct AppConfig {
  pub upstreams:       Vec<UpstreamConfig>,
  pub fallback_cache:  Option<UpstreamConfig>,
  pub nar_hedging:     NarHedgingConfig,
  pub cache_priority:  i32,
  pub want_mass_query: bool,
  pub read_timeout:    Duration,
  pub write_timeout:   Duration,
}

/// Build the HTTP application router.
///
/// # Errors
///
/// Returns an error if the proxy HTTP client cannot be constructed.
pub fn app(
  router: Router,
  prober: Prober,
  db: Db,
  config: AppConfig,
) -> Result<AxumRouter, reqwest::Error> {
  let AppConfig {
    upstreams,
    fallback_cache,
    nar_hedging,
    cache_priority,
    want_mass_query,
    read_timeout,
    write_timeout,
  } = config;
  let s3 = S3ClientPool::default();
  for upstream in &upstreams {
    if let Some(config) = &upstream.s3 {
      s3.register(upstream.url.clone(), config.clone());
    }
  }
  if let Some(upstream) = &fallback_cache
    && let Some(config) = &upstream.s3
  {
    s3.register(upstream.url.clone(), config.clone());
  }

  let mut nar_clients = HashMap::new();
  for upstream in &upstreams {
    let timeout = upstream.nar_timeout.as_ref().map_or(read_timeout, |t| t.0);
    nar_clients.insert(
      upstream.url.clone(),
      reqwest::Client::builder().read_timeout(timeout).build()?,
    );
  }
  if let Some(upstream) = &fallback_cache
    && !nar_clients.contains_key(&upstream.url)
  {
    let timeout = upstream.nar_timeout.as_ref().map_or(read_timeout, |t| t.0);
    nar_clients.insert(
      upstream.url.clone(),
      reqwest::Client::builder().read_timeout(timeout).build()?,
    );
  }

  let default_client = reqwest::Client::builder()
    .read_timeout(read_timeout)
    .build()?;

  let state = AppState {
    router,
    prober,
    db,
    upstreams,
    fallback_cache,
    s3,
    nar_clients,
    default_client,
    nar_hedging,
    cache_priority,
    want_mass_query,
    started: Instant::now(),
  };
  Ok(
    AxumRouter::new()
      .merge(
        AxumRouter::new()
          .route("/nix-cache-info", get(cache_info).head(cache_info))
          .route("/health", get(health))
          .route("/status", get(status_endpoint))
          .route("/metrics", get(metrics_endpoint))
          .route("/{hash_narinfo}", get(narinfo).head(narinfo))
          .route_layer(ResponseBodyTimeoutLayer::new(write_timeout)),
      )
      .merge(AxumRouter::new().route("/nar/{*path}", get(nar).head(nar)))
      .layer(RequestBodyTimeoutLayer::new(read_timeout))
      .with_state(Arc::new(state)),
  )
}

/// Render the `/nix-cache-info` body. `WantMassQuery` is `1` when
/// `want_mass_query` is set and `0` otherwise; `Priority` is the advertised
/// cache priority.
fn cache_info_body(want_mass_query: bool, cache_priority: i32) -> String {
  format!(
    "StoreDir: /nix/store\nWantMassQuery: {}\nPriority: {cache_priority}\n",
    u8::from(want_mass_query),
  )
}

async fn cache_info(State(state): State<Arc<AppState>>) -> Response {
  (
    [("content-type", "text/plain")],
    cache_info_body(state.want_mass_query, state.cache_priority),
  )
    .into_response()
}

#[derive(Serialize)]
struct HealthResponse {
  status: String,
}

/// Derive an overall health verdict from per-upstream statuses.
///
/// Returns `"down"` only when every upstream is [`Status::Down`],
/// `"degraded"` when any upstream is down or degraded, and `"ok"` otherwise.
fn overall_status(health: &[UpstreamHealth]) -> &'static str {
  let down_count = health.iter().filter(|h| h.status == Status::Down).count();
  let any_degraded = health.iter().any(|h| h.status == Status::Degraded);
  if !health.is_empty() && down_count == health.len() {
    "down"
  } else if down_count > 0 || any_degraded {
    "degraded"
  } else {
    "ok"
  }
}

/// Liveness/readiness probe. Returns `503` when every upstream is down (ncro
/// cannot serve any cache), `200` otherwise. Per-upstream detail lives at
/// `/status`.
async fn health(State(state): State<Arc<AppState>>) -> Response {
  let sorted = state.prober.sorted_by_latency().await;
  let status = overall_status(&sorted);
  let code = if status == "down" {
    StatusCode::SERVICE_UNAVAILABLE
  } else {
    StatusCode::OK
  };
  (
    code,
    axum::Json(HealthResponse {
      status: status.to_string(),
    }),
  )
    .into_response()
}

#[derive(Serialize)]
struct StatusResponse {
  version:     &'static str,
  uptime_secs: u64,
  status:      String,
  cache:       CacheStatus,
  config:      ConfigStatus,
  upstreams:   Vec<UpstreamStatusFull>,
  fallback:    Option<UpstreamStatusFull>,
}

#[derive(Serialize)]
struct CacheStatus {
  route_entries:         i64,
  narinfo_hits:          u64,
  narinfo_misses:        u64,
  narinfo_negative_hits: u64,
  nar_requests:          u64,
}

#[derive(Serialize)]
struct ConfigStatus {
  cache_priority:      i32,
  upstream_count:      usize,
  fallback_configured: bool,
}

#[derive(Serialize)]
struct UpstreamStatusFull {
  url:                 String,
  status:              String,
  priority:            i32,
  ema_latency_ms:      f64,
  consecutive_fails:   u32,
  total_queries:       u64,
  last_probe_secs_ago: Option<u64>,
  kind:                &'static str,
  authenticated:       bool,
}

/// Build a detailed status view for a single upstream. `now` anchors the
/// last-probe age; `kind` is `"s3"` or `"http"`; `authenticated` reflects
/// whether credentials are configured for the upstream.
fn upstream_status_full(
  health: UpstreamHealth,
  now: Instant,
  kind: &'static str,
  authenticated: bool,
) -> UpstreamStatusFull {
  UpstreamStatusFull {
    last_probe_secs_ago: health
      .last_probe
      .map(|t| now.saturating_duration_since(t).as_secs()),
    status: health.status.as_str().to_string(),
    url: health.url,
    priority: health.priority,
    ema_latency_ms: health.ema_latency,
    consecutive_fails: health.consecutive_fails,
    total_queries: health.total_queries,
    kind,
    authenticated,
  }
}

async fn status_endpoint(State(state): State<Arc<AppState>>) -> Response {
  let now = Instant::now();
  let sorted = state.prober.sorted_by_latency().await;
  let status = overall_status(&sorted).to_string();

  let metrics = ncro_metrics::get();
  let cache = CacheStatus {
    route_entries:         metrics.route_entries.get(),
    narinfo_hits:          metrics.narinfo_cache_hits.get(),
    narinfo_misses:        metrics.narinfo_cache_misses.get(),
    narinfo_negative_hits: metrics.narinfo_memory_negative_hits.get(),
    nar_requests:          metrics.nar_requests.get(),
  };

  let upstreams = sorted
    .into_iter()
    .map(|h| {
      let kind = if state.s3.contains(&h.url) {
        "s3"
      } else {
        "http"
      };
      let authenticated = upstream_auth(&state, &h.url).is_some();
      upstream_status_full(h, now, kind, authenticated)
    })
    .collect();

  let fallback = if let Some(fallback) = &state.fallback_cache {
    let health =
      state
        .prober
        .get_health(&fallback.url)
        .await
        .unwrap_or_else(|| {
          UpstreamHealth::new(fallback.url.clone(), fallback.priority)
        });
    let kind = if state.s3.contains(&fallback.url) {
      "s3"
    } else {
      "http"
    };
    let authenticated = upstream_auth(&state, &fallback.url).is_some();
    Some(upstream_status_full(health, now, kind, authenticated))
  } else {
    None
  };

  axum::Json(StatusResponse {
    version: env!("CARGO_PKG_VERSION"),
    uptime_secs: now.saturating_duration_since(state.started).as_secs(),
    status,
    cache,
    config: ConfigStatus {
      cache_priority:      state.cache_priority,
      upstream_count:      state.upstreams.len(),
      fallback_configured: state.fallback_cache.is_some(),
    },
    upstreams,
    fallback,
  })
  .into_response()
}

async fn metrics_endpoint() -> Response {
  (
    [("content-type", "text/plain; version=0.0.4")],
    ncro_metrics::gather(),
  )
    .into_response()
}

async fn narinfo(
  State(state): State<Arc<AppState>>,
  Path(hash_narinfo): Path<String>,
  req: Request<Body>,
) -> Response {
  let Some(hash) = hash_narinfo.strip_suffix(".narinfo") else {
    return StatusCode::NOT_FOUND.into_response();
  };
  let candidates = upstream_urls(&state).await;
  match state.router.resolve(hash, &candidates).await {
    Ok(result) => {
      tracing::info!(
        hash = hash,
        upstream = result.url,
        cache_hit = result.cache_hit,
        latency_ms = format_args!("{:.5}", result.latency_ms),
        "narinfo routed"
      );
      ncro_metrics::get()
        .narinfo_requests
        .with_label_values(&["200"])
        .inc();
      let route = if result.cache_hit {
        RouteKind::CacheHit
      } else {
        RouteKind::Race
      };
      if let Some(bytes) = result.narinfo_bytes {
        return with_provenance(
          (
            StatusCode::OK,
            [("content-type", "text/x-nix-narinfo")],
            Bytes::from(bytes),
          )
            .into_response(),
          &result.url,
          route,
        );
      }
      with_provenance(
        proxy(
          state.nar_client(&result.url),
          req.method().clone(),
          req.headers(),
          format!("{}{}", result.url, req.uri().path()),
          upstream_auth(&state, &result.url),
        )
        .await,
        &result.url,
        route,
      )
    },
    Err(RouterError::NotFound) => {
      ncro_metrics::get()
        .narinfo_requests
        .with_label_values(&["error"])
        .inc();
      StatusCode::NOT_FOUND.into_response()
    },
    Err(RouterError::UpstreamUnavailable | RouterError::NoCandidates(_)) => {
      if let Some(resp) = try_fallback_narinfo(&state, hash, req).await {
        return resp;
      }
      ncro_metrics::get()
        .narinfo_requests
        .with_label_values(&["error"])
        .inc();
      (StatusCode::BAD_GATEWAY, "upstream unavailable").into_response()
    },
    Err(err) => {
      tracing::warn!(hash, error = %err, "narinfo resolve failed");
      ncro_metrics::get()
        .narinfo_requests
        .with_label_values(&["error"])
        .inc();
      (StatusCode::BAD_GATEWAY, "upstream unavailable").into_response()
    },
  }
}

#[derive(Clone)]
struct NarCandidate {
  upstream: String,
  path:     String,
  route:    RouteKind,
}

fn spawn_nar_attempt(
  attempts: &mut JoinSet<(NarCandidate, Option<Response>)>,
  state: Arc<AppState>,
  method: Method,
  headers: HeaderMap,
  candidate: NarCandidate,
) {
  attempts.spawn(async move {
    let response = try_nar_upstream(NarUpstreamRequest {
      client: state.nar_client(&candidate.upstream),
      s3: &state.s3,
      method,
      headers: &headers,
      upstream: &candidate.upstream,
      path: &candidate.path,
      route: candidate.route,
      auth: upstream_auth(&state, &candidate.upstream),
    })
    .await;
    (candidate, response)
  });
}

async fn hedged_nar(
  state: Arc<AppState>,
  method: Method,
  headers: HeaderMap,
  initial: NarCandidate,
  mut candidates: Vec<NarCandidate>,
) -> Option<Response> {
  let max_inflight = if state.nar_hedging.enabled {
    usize::try_from(state.nar_hedging.max_inflight).unwrap_or(1)
  } else {
    1
  };
  let delay = state.nar_hedging.delay.0;
  let mut attempts = JoinSet::new();
  spawn_nar_attempt(
    &mut attempts,
    Arc::clone(&state),
    method.clone(),
    headers.clone(),
    initial,
  );
  if method == Method::HEAD && state.nar_hedging.enabled {
    while let Some(candidate) = candidates.pop() {
      ncro_metrics::get()
        .nar_hedges
        .with_label_values(&["started", &candidate.upstream])
        .inc();
      spawn_nar_attempt(
        &mut attempts,
        Arc::clone(&state),
        method.clone(),
        headers.clone(),
        candidate,
      );
    }
  }
  let mut next_hedge = TokioInstant::now() + delay;

  loop {
    if attempts.is_empty() && candidates.is_empty() {
      return None;
    }
    tokio::select! {
      result = attempts.join_next(), if !attempts.is_empty() => {
        let Some(Ok((candidate, response))) = result else { continue; };
        if let Some(response) = response {
          if candidate.route == RouteKind::Hedge {
            ncro_metrics::get().nar_hedges.with_label_values(&["won", &candidate.upstream]).inc();
          }
          tracing::info!(upstream = candidate.upstream, route = candidate.route.header_value(), "nar attempt won");
          let cancelled = attempts.len();
          if cancelled > 0 {
            ncro_metrics::get().nar_hedges.with_label_values(&["cancelled", &candidate.upstream]).inc_by(u64::try_from(cancelled).unwrap_or(u64::MAX));
            tracing::info!(upstream = candidate.upstream, cancelled, "nar hedge losers cancelled");
          }
          attempts.abort_all();
          return Some(response);
        }
        ncro_metrics::get().nar_hedge_failures.with_label_values(&["unavailable", &candidate.upstream]).inc();
        tracing::debug!(upstream = candidate.upstream, "nar attempt failed");
        if let Some(candidate) = candidates.pop() {
          ncro_metrics::get().nar_hedges.with_label_values(&["started", &candidate.upstream]).inc();
          tracing::info!(upstream = candidate.upstream, "nar hedge started after failure");
          spawn_nar_attempt(&mut attempts, Arc::clone(&state), method.clone(), headers.clone(), candidate);
        }
      }
      () = sleep_until(next_hedge), if method != Method::HEAD && state.nar_hedging.enabled && attempts.len() < max_inflight && !candidates.is_empty() => {
        if let Some(candidate) = candidates.pop() {
          ncro_metrics::get().nar_hedges.with_label_values(&["started", &candidate.upstream]).inc();
          tracing::info!(upstream = candidate.upstream, "nar hedge started");
          spawn_nar_attempt(&mut attempts, Arc::clone(&state), method.clone(), headers.clone(), candidate);
          next_hedge = TokioInstant::now() + delay;
        }
      }
    }
  }
}

fn hedge_allowed(state: &AppState, url: &str) -> bool {
  state
    .upstreams
    .iter()
    .find(|upstream| upstream.url == url)
    .is_none_or(|upstream| upstream.allow_hedging)
}

async fn hedge_candidates(
  state: &AppState,
  store_hash: Option<&str>,
  path: &str,
  skip: &str,
  include_hedging_opt_outs: bool,
) -> Vec<NarCandidate> {
  let mut by_priority = BTreeMap::<i32, Vec<UpstreamHealth>>::new();
  for health in state.prober.sorted_by_latency().await {
    if health.status == Status::Down || health.url == skip {
      continue;
    }
    if !include_hedging_opt_outs && !hedge_allowed(state, &health.url) {
      continue;
    }
    by_priority.entry(health.priority).or_default().push(health);
  }
  let mut candidates = Vec::new();
  for group in by_priority.into_values() {
    for health in group {
      let candidate_path = if let Some(store_hash) = store_hash {
        let Ok(candidate_path) = state
          .router
          .upstream_nar_path(&health.url, store_hash)
          .await
        else {
          continue;
        };
        candidate_path
      } else {
        path.to_string()
      };
      candidates.push(NarCandidate {
        upstream: health.url,
        path:     candidate_path,
        route:    RouteKind::Hedge,
      });
    }
  }
  candidates.reverse();
  candidates
}

async fn nar(
  State(state): State<Arc<AppState>>,
  req: Request<Body>,
) -> Response {
  ncro_metrics::get().nar_requests.inc();
  // Path without leading slash for DB lookup (query stripped; harmonia appends
  // ?hash=STORE_HASH which is not part of the stored key).
  let nar_url = req.uri().path().trim_start_matches('/').to_string();
  // Full path+query forwarded to upstream so harmonia can locate the store
  // path.
  let path_and_query = req
    .uri()
    .path_and_query()
    .map_or_else(|| req.uri().path(), PathAndQuery::as_str)
    .to_string();

  let mut normal_attempted = false;
  let routed_upstream = 'routed: {
    let Ok(Some(entry)) = state.db.get_route_by_nar_url(&nar_url).await else {
      break 'routed None;
    };
    if !entry.is_valid() {
      break 'routed None;
    }
    let upstream_path = if entry.upstream_nar_url.is_empty() {
      &path_and_query
    } else {
      &entry.upstream_nar_url
    };
    let candidates = hedge_candidates(
      &state,
      store_hash_from_canonical_nar_url(&nar_url),
      &path_and_query,
      &entry.upstream_url,
      false,
    )
    .await;
    normal_attempted = true;
    if let Some(resp) = hedged_nar(
      Arc::clone(&state),
      req.method().clone(),
      req.headers().clone(),
      NarCandidate {
        upstream: entry.upstream_url.clone(),
        path:     upstream_path.clone(),
        route:    RouteKind::CacheHit,
      },
      candidates,
    )
    .await
    {
      return resp;
    }
    Some(entry.upstream_url)
  };

  if !normal_attempted {
    let mut candidates = hedge_candidates(
      &state,
      store_hash_from_canonical_nar_url(&nar_url),
      &path_and_query,
      "",
      true,
    )
    .await;
    if let Some(mut initial) = candidates.pop() {
      initial.route = RouteKind::Direct;
      candidates.retain(|candidate| hedge_allowed(&state, &candidate.upstream));
      if let Some(resp) = hedged_nar(
        Arc::clone(&state),
        req.method().clone(),
        req.headers().clone(),
        initial,
        candidates,
      )
      .await
      {
        return resp;
      }
    }
  }

  let _ = routed_upstream;
  if let Some(fallback) = &state.fallback_cache
    && let Some(resp) = try_nar_upstream(NarUpstreamRequest {
      client:   state.nar_client(&fallback.url),
      s3:       &state.s3,
      method:   req.method().clone(),
      headers:  req.headers(),
      upstream: &fallback.url,
      path:     &path_and_query,
      route:    RouteKind::Fallback,
      auth:     upstream_auth(&state, &fallback.url),
    })
    .await
  {
    return resp;
  }
  StatusCode::NOT_FOUND.into_response()
}

async fn try_fallback_narinfo(
  state: &AppState,
  hash: &str,
  req: Request<Body>,
) -> Option<Response> {
  let fallback = state.fallback_cache.as_ref()?;
  match state.router.resolve_fallback(hash, &fallback.url).await {
    Ok(result) => {
      tracing::warn!(
        hash,
        upstream = result.url,
        latency_ms = format_args!("{:.5}", result.latency_ms),
        "narinfo routed to fallback cache"
      );
      ncro_metrics::get()
        .narinfo_requests
        .with_label_values(&["200"])
        .inc();
      if let Some(bytes) = result.narinfo_bytes {
        return Some(with_provenance(
          (
            StatusCode::OK,
            [("content-type", "text/x-nix-narinfo")],
            Bytes::from(bytes),
          )
            .into_response(),
          &result.url,
          RouteKind::Fallback,
        ));
      }
      Some(with_provenance(
        proxy(
          state.nar_client(&result.url),
          req.method().clone(),
          req.headers(),
          format!("{}{}", result.url, req.uri().path()),
          upstream_auth(state, &result.url),
        )
        .await,
        &result.url,
        RouteKind::Fallback,
      ))
    },
    Err(RouterError::NotFound) => {
      ncro_metrics::get()
        .narinfo_requests
        .with_label_values(&["error"])
        .inc();
      Some(StatusCode::NOT_FOUND.into_response())
    },
    Err(err) => {
      tracing::warn!(hash, upstream = fallback.url, error = %err, "fallback cache failed");
      None
    },
  }
}

fn upstream_auth(
  state: &AppState,
  url: &str,
) -> Option<(String, Option<String>)> {
  state
    .upstreams
    .iter()
    .chain(state.fallback_cache.iter())
    .find(|u| u.url == url && !u.username.is_empty())
    .map(|u| (u.username.clone(), u.password.clone()))
}

async fn upstream_urls(state: &AppState) -> Vec<String> {
  let urls = state
    .prober
    .sorted_by_latency()
    .await
    .into_iter()
    .filter(|h| h.status != Status::Down)
    .map(|h| h.url)
    .collect::<Vec<_>>();
  if urls.is_empty() {
    state.upstreams.iter().map(|u| u.url.clone()).collect()
  } else {
    urls
  }
}

struct NarUpstreamRequest<'a> {
  client:   &'a reqwest::Client,
  s3:       &'a S3ClientPool,
  method:   Method,
  headers:  &'a HeaderMap,
  upstream: &'a str,
  path:     &'a str,
  route:    RouteKind,
  auth:     Option<(String, Option<String>)>,
}

async fn try_nar_upstream(req: NarUpstreamRequest<'_>) -> Option<Response> {
  let NarUpstreamRequest {
    client,
    s3,
    method,
    headers,
    upstream,
    path,
    route,
    auth,
  } = req;
  if s3.contains(upstream) {
    let key = path.trim_start_matches('/');
    if method == Method::HEAD {
      let metadata = s3.head_object_metadata(upstream, key).await.ok()??;
      return Some(with_provenance(
        response_from_s3_head(metadata),
        upstream,
        route,
      ));
    }
    if method != Method::GET {
      return None;
    }
    let range = headers.get("range").and_then(|value| value.to_str().ok());
    let object = s3.get_object(upstream, key, range).await.ok()??;
    let response = response_from_s3_first_byte(object).await?;
    return Some(with_provenance(response, upstream, route));
  }
  let resp = upstream_request(
    client,
    method,
    headers,
    format!("{upstream}{path}"),
    auth,
  )
  .await
  .ok()?;
  if !resp.status().is_success() {
    return None;
  }
  let response = response_from_reqwest_first_byte(resp).await?;
  Some(with_provenance(response, upstream, route))
}

async fn proxy(
  client: &reqwest::Client,
  method: Method,
  headers: &HeaderMap,
  url: String,
  auth: Option<(String, Option<String>)>,
) -> Response {
  match upstream_request(client, method, headers, url, auth).await {
    Ok(resp) => response_from_reqwest(resp),
    Err(err) => {
      tracing::warn!(error = %err, "upstream request failed");
      (StatusCode::BAD_GATEWAY, "upstream error").into_response()
    },
  }
}

async fn upstream_request(
  client: &reqwest::Client,
  method: Method,
  headers: &HeaderMap,
  url: String,
  auth: Option<(String, Option<String>)>,
) -> reqwest::Result<reqwest::Response> {
  let mut req = client.request(method, url);
  if let Some((user, pass)) = auth {
    req = req.basic_auth(user, pass);
  }
  for name in ["accept", "accept-encoding", "range"] {
    if let Some(value) = headers.get(name) {
      req = req.header(name, value);
    }
  }
  req.send().await
}

fn response_from_headers(
  status: StatusCode,
  headers: &HeaderMap,
  body: Body,
) -> Response {
  let mut out = Response::builder().status(status);
  for name in [
    "accept-ranges",
    "content-type",
    "content-length",
    "content-range",
    "content-encoding",
    "etag",
    "x-nix-signature",
    "cache-control",
    "last-modified",
  ] {
    if let Some(value) = headers.get(name)
      && let (Ok(header_name), Ok(header_value)) = (
        HeaderName::from_bytes(name.as_bytes()),
        HeaderValue::from_bytes(value.as_bytes()),
      )
    {
      out = out.header(header_name, header_value);
    }
  }
  out
    .body(body)
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn response_from_reqwest(resp: reqwest::Response) -> Response {
  let status = StatusCode::from_u16(resp.status().as_u16())
    .unwrap_or(StatusCode::BAD_GATEWAY);
  let headers = resp.headers().clone();
  let stream = resp.bytes_stream().map_err(io::Error::other);
  response_from_headers(status, &headers, Body::from_stream(stream))
}

async fn response_from_reqwest_first_byte(
  resp: reqwest::Response,
) -> Option<Response> {
  let status = StatusCode::from_u16(resp.status().as_u16()).ok()?;
  let headers = resp.headers().clone();
  let mut stream = Box::pin(resp.bytes_stream().map_err(io::Error::other));
  let first = stream.try_next().await.ok()??;
  let body =
    Body::from_stream(stream::once(async move { Ok(first) }).chain(stream));
  Some(response_from_headers(status, &headers, body))
}

async fn response_from_s3_first_byte(
  object: ncro_s3::S3Object,
) -> Option<Response> {
  let mut stream = Box::pin(S3ClientPool::body_stream(object.body));
  let first = stream.try_next().await.ok()??;
  let mut out =
    Response::builder().status(StatusCode::from_u16(object.status).ok()?);
  for (name, value) in [
    ("accept-ranges", object.accept_ranges),
    ("content-type", object.content_type),
    (
      "content-length",
      object.content_length.map(|value| value.to_string()),
    ),
    ("content-range", object.content_range),
    ("etag", object.etag),
    ("last-modified", object.last_modified),
  ] {
    if let Some(value) = value {
      out = out.header(name, value);
    }
  }
  out
    .body(Body::from_stream(
      stream::once(async move { Ok(first) }).chain(stream),
    ))
    .ok()
}

fn response_from_s3_head(metadata: ncro_s3::S3ObjectHead) -> Response {
  let mut out = Response::builder().status(StatusCode::OK);
  for (name, value) in [
    ("accept-ranges", metadata.accept_ranges),
    ("content-type", metadata.content_type),
    (
      "content-length",
      metadata.content_length.map(|value| value.to_string()),
    ),
    ("etag", metadata.etag),
    ("last-modified", metadata.last_modified),
  ] {
    if let Some(value) = value {
      out = out.header(name, value);
    }
  }
  out
    .body(Body::empty())
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

#[cfg(test)]
mod tests {
  use std::time::{Duration, Instant};

  use axum::{
    body::Body,
    http::{HeaderValue, Response},
  };
  use ncro_health::{Status, UpstreamHealth};

  use super::{
    RouteKind,
    cache_info_body,
    overall_status,
    upstream_status_full,
    with_provenance,
  };

  #[test]
  fn cache_info_body_reflects_want_mass_query() {
    assert_eq!(
      cache_info_body(true, 30),
      "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 30\n"
    );
    assert_eq!(
      cache_info_body(false, 42),
      "StoreDir: /nix/store\nWantMassQuery: 0\nPriority: 42\n"
    );
  }

  #[test]
  fn provenance_uses_hostname_and_preserves_response_headers() {
    let mut response = Response::new(Body::empty());
    response
      .headers_mut()
      .insert("content-type", HeaderValue::from_static("text/plain"));
    let response = with_provenance(
      response,
      "https://cache.example.test:8443/path?secret=ignored",
      RouteKind::Race,
    );

    assert_eq!(response.headers()["content-type"], "text/plain");
    assert_eq!(response.headers()["x-ncro-upstream"], "cache.example.test");
    assert_eq!(response.headers()["x-ncro-route"], "race");
  }

  #[test]
  fn provenance_marks_hedge_winners() {
    let response = with_provenance(
      Response::new(Body::empty()),
      "https://cache.example.test",
      RouteKind::Hedge,
    );

    assert_eq!(response.headers()["x-ncro-route"], "hedge");
  }

  #[test]
  fn provenance_hides_invalid_upstream_values() {
    let response = with_provenance(
      Response::new(Body::empty()),
      "not a valid URL\r\nX-Injected: no",
      RouteKind::Fallback,
    );

    assert_eq!(response.headers()["x-ncro-upstream"], "unknown");
    assert_eq!(response.headers()["x-ncro-route"], "fallback");
  }

  fn health(url: &str, status: Status) -> UpstreamHealth {
    let mut h = UpstreamHealth::new(url.to_string(), 40);
    h.status = status;
    h
  }

  #[test]
  fn overall_status_ok_when_all_active() {
    let all = [health("a", Status::Active), health("b", Status::Active)];
    assert_eq!(overall_status(&all), "ok");
  }

  #[test]
  fn overall_status_degraded_when_any_down_or_degraded() {
    let mixed = [health("a", Status::Active), health("b", Status::Down)];
    assert_eq!(overall_status(&mixed), "degraded");
    let degraded = [health("a", Status::Degraded)];
    assert_eq!(overall_status(&degraded), "degraded");
  }

  #[test]
  fn overall_status_down_only_when_all_down() {
    let all = [health("a", Status::Down), health("b", Status::Down)];
    assert_eq!(overall_status(&all), "down");
  }

  #[test]
  fn overall_status_ok_when_empty() {
    assert_eq!(overall_status(&[]), "ok");
  }

  #[test]
  fn upstream_status_full_never_probed_has_no_age() {
    let full = upstream_status_full(
      health("a", Status::Active),
      Instant::now(),
      "http",
      false,
    );
    assert_eq!(full.last_probe_secs_ago, None);
  }

  #[test]
  fn upstream_status_full_computes_probe_age() {
    let now = Instant::now();
    let mut h = health("a", Status::Active);
    h.last_probe = now.checked_sub(Duration::from_secs(5));
    let full = upstream_status_full(h, now, "http", false);
    assert_eq!(full.last_probe_secs_ago, Some(5));
  }
}
