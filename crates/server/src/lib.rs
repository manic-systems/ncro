use std::{
  collections::{BTreeMap, HashMap},
  sync::Arc,
};

use axum::{
  Router as AxumRouter,
  body::Body,
  extract::{Path, State},
  http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode},
  response::{IntoResponse, Response},
  routing::get,
};
use bytes::Bytes;
use futures_util::TryStreamExt;
use ncro_config::UpstreamConfig;
use ncro_db::Db;
use ncro_health::{Prober, Status, UpstreamHealth};
use ncro_router::{Router, RouterError, store_hash_from_canonical_nar_url};
use ncro_s3::S3ClientPool;
use serde::Serialize;
use tower_http::timeout::{RequestBodyTimeoutLayer, ResponseBodyTimeoutLayer};

#[derive(Clone)]
pub struct AppState {
  router:         Router,
  prober:         Prober,
  db:             Db,
  upstreams:      Vec<UpstreamConfig>,
  fallback_cache: Option<UpstreamConfig>,
  s3:             S3ClientPool,
  nar_clients:    HashMap<String, reqwest::Client>,
  default_client: reqwest::Client,
  cache_priority: i32,
}

impl AppState {
  fn nar_client(&self, url: &str) -> &reqwest::Client {
    self.nar_clients.get(url).unwrap_or(&self.default_client)
  }
}

pub struct AppConfig {
  pub upstreams:      Vec<UpstreamConfig>,
  pub fallback_cache: Option<UpstreamConfig>,
  pub cache_priority: i32,
  pub read_timeout:   std::time::Duration,
  pub write_timeout:  std::time::Duration,
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
    cache_priority,
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
    cache_priority,
  };
  Ok(
    AxumRouter::new()
      .merge(
        AxumRouter::new()
          .route("/nix-cache-info", get(cache_info).head(cache_info))
          .route("/health", get(health))
          .route("/metrics", get(metrics_endpoint))
          .route("/{hash_narinfo}", get(narinfo).head(narinfo))
          .route_layer(ResponseBodyTimeoutLayer::new(write_timeout)),
      )
      .merge(AxumRouter::new().route("/nar/{*path}", get(nar).head(nar)))
      .layer(RequestBodyTimeoutLayer::new(read_timeout))
      .with_state(Arc::new(state)),
  )
}

async fn cache_info(State(state): State<Arc<AppState>>) -> Response {
  (
    [("content-type", "text/plain")],
    format!(
      "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: {}\n",
      state.cache_priority
    ),
  )
    .into_response()
}

#[derive(Serialize)]
struct HealthResponse {
  status:    String,
  upstreams: Vec<UpstreamStatus>,
}

#[derive(Serialize)]
struct UpstreamStatus {
  url:               String,
  status:            String,
  latency_ms:        f64,
  consecutive_fails: u32,
}

async fn health(State(state): State<Arc<AppState>>) -> Response {
  let sorted = state.prober.sorted_by_latency().await;
  let down_count = sorted.iter().filter(|h| h.status == Status::Down).count();
  let any_degraded = sorted.iter().any(|h| h.status == Status::Degraded);
  let status = if !sorted.is_empty() && down_count == sorted.len() {
    "down"
  } else if down_count > 0 || any_degraded {
    "degraded"
  } else {
    "ok"
  };
  axum::Json(HealthResponse {
    status:    status.to_string(),
    upstreams: sorted
      .into_iter()
      .map(|h| {
        UpstreamStatus {
          url:               h.url,
          status:            h.status.as_str().to_string(),
          latency_ms:        h.ema_latency,
          consecutive_fails: h.consecutive_fails,
        }
      })
      .collect(),
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
      if let Some(bytes) = result.narinfo_bytes {
        return (
          StatusCode::OK,
          [("content-type", "text/x-nix-narinfo")],
          Bytes::from(bytes),
        )
          .into_response();
      }
      proxy(
        state.nar_client(&result.url),
        req.method().clone(),
        req.headers(),
        format!("{}{}", result.url, req.uri().path()),
        upstream_auth(&state, &result.url),
      )
      .await
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
    .map_or_else(|| req.uri().path(), axum::http::uri::PathAndQuery::as_str)
    .to_string();

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
    if let Some(resp) = try_nar_upstream(
      state.nar_client(&entry.upstream_url),
      &state.s3,
      req.method().clone(),
      req.headers(),
      &entry.upstream_url,
      upstream_path,
      upstream_auth(&state, &entry.upstream_url),
    )
    .await
    {
      return resp;
    }
    Some(entry.upstream_url)
  };

  // Only a canonical path carries the store hash the other upstreams need.
  if let Some(store_hash) = store_hash_from_canonical_nar_url(&nar_url)
    && let Some(resp) = retry_by_store_hash(
      &state,
      store_hash,
      req.method(),
      req.headers(),
      routed_upstream.as_deref(),
    )
    .await
  {
    return resp;
  }

  // Try upstreams grouped by priority as a fallback (lower = preferred), within
  // each group sorted by EMA latency.
  let mut by_priority = BTreeMap::<i32, Vec<UpstreamHealth>>::new();
  for h in state.prober.sorted_by_latency().await {
    if h.status == Status::Down {
      continue;
    }
    by_priority.entry(h.priority).or_default().push(h);
  }
  for (_priority, group) in by_priority {
    for h in group {
      if let Some(resp) = try_nar_upstream(
        state.nar_client(&h.url),
        &state.s3,
        req.method().clone(),
        req.headers(),
        &h.url,
        &path_and_query,
        upstream_auth(&state, &h.url),
      )
      .await
      {
        return resp;
      }
    }
  }
  if let Some(fallback) = &state.fallback_cache
    && let Some(resp) = try_nar_upstream(
      state.nar_client(&fallback.url),
      &state.s3,
      req.method().clone(),
      req.headers(),
      &fallback.url,
      &path_and_query,
      upstream_auth(&state, &fallback.url),
    )
    .await
  {
    return resp;
  }
  StatusCode::NOT_FOUND.into_response()
}

async fn retry_by_store_hash(
  state: &AppState,
  store_hash: &str,
  method: &Method,
  headers: &HeaderMap,
  skip: Option<&str>,
) -> Option<Response> {
  let mut by_priority = BTreeMap::<i32, Vec<UpstreamHealth>>::new();
  for h in state.prober.sorted_by_latency().await {
    if h.status == Status::Down || Some(h.url.as_str()) == skip {
      continue;
    }
    by_priority.entry(h.priority).or_default().push(h);
  }
  for (_priority, group) in by_priority {
    for h in group {
      let Ok(path) = state.router.upstream_nar_path(&h.url, store_hash).await
      else {
        continue;
      };
      if let Some(resp) = try_nar_upstream(
        state.nar_client(&h.url),
        &state.s3,
        method.clone(),
        headers,
        &h.url,
        &path,
        upstream_auth(state, &h.url),
      )
      .await
      {
        tracing::warn!(
          store = store_hash,
          upstream = h.url,
          "nar re-routed after primary upstream failed"
        );
        return Some(resp);
      }
    }
  }
  None
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
        return Some(
          (
            StatusCode::OK,
            [("content-type", "text/x-nix-narinfo")],
            Bytes::from(bytes),
          )
            .into_response(),
        );
      }
      Some(
        proxy(
          state.nar_client(&result.url),
          req.method().clone(),
          req.headers(),
          format!("{}{}", result.url, req.uri().path()),
          upstream_auth(state, &result.url),
        )
        .await,
      )
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

async fn try_nar_upstream(
  client: &reqwest::Client,
  s3: &S3ClientPool,
  method: Method,
  headers: &HeaderMap,
  upstream: &str,
  path: &str,
  auth: Option<(String, Option<String>)>,
) -> Option<Response> {
  if s3.contains(upstream) {
    let key = path.trim_start_matches('/');
    if method == Method::HEAD {
      let metadata = s3.head_object_metadata(upstream, key).await.ok()??;
      return Some(response_from_s3_head(metadata));
    }
    if method != Method::GET {
      return None;
    }
    let range = headers.get("range").and_then(|value| value.to_str().ok());
    let object = s3.get_object(upstream, key, range).await.ok()??;
    return Some(response_from_s3(object));
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
  Some(response_from_reqwest(resp))
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

fn response_from_reqwest(resp: reqwest::Response) -> Response {
  let status = StatusCode::from_u16(resp.status().as_u16())
    .unwrap_or(StatusCode::BAD_GATEWAY);
  let headers = resp.headers().clone();
  let stream = resp.bytes_stream().map_err(std::io::Error::other);
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
    .body(Body::from_stream(stream))
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn response_from_s3(object: ncro_s3::S3Object) -> Response {
  let mut out = Response::builder()
    .status(StatusCode::from_u16(object.status).unwrap_or(StatusCode::OK));
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
    .body(Body::from_stream(S3ClientPool::body_stream(object.body)))
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
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
