//! Consumer-visible transfer regressions using real loopback HTTP bodies.
//! Route seeding is deliberately limited to narinfo resolution: every NAR is
//! downloaded through the public server application and two Axum caches.

use std::{
  error::Error,
  future::pending,
  io,
  slice::from_ref,
  sync::{
    Arc,
    Mutex,
    atomic::{AtomicUsize, Ordering},
  },
  time::Duration,
};

use axum::{
  Router as AxumRouter,
  body::Body,
  extract::{Path, State},
  http::{HeaderMap, StatusCode, header},
  response::{IntoResponse, Response},
  routing::get,
};
use bytes::Bytes;
use futures_util::stream;
use ncro_config::{
  HumanDuration,
  NarHedgingConfig,
  NarTransferConfig,
  UpstreamConfig,
};
use ncro_db::Db;
use ncro_health::Prober;
use ncro_router::{Hop, Router, RouterTuning};
use ncro_server::{AppConfig, app};
use sha2::{Digest, Sha256};
use tokio::{
  net::TcpListener,
  sync::Notify,
  task::JoinHandle,
  time::{sleep, timeout},
};

const HASH: &str = "0123456789abcdfghijklmnpqrsvwxyz";
const NEXT_HASH: &str = "1123456789abcdfghijklmnpqrsvwxyz";
const PAYLOAD_SIZE: usize = 1024 * 1024;
const PREFIX_SIZE: usize = 8192;
const TEST_DEADLINE: Duration = Duration::from_secs(10);
type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct ServerGuard {
  url:  String,
  task: JoinHandle<io::Result<()>>,
}

impl Drop for ServerGuard {
  fn drop(&mut self) {
    self.task.abort();
  }
}

async fn serve(router: AxumRouter) -> TestResult<ServerGuard> {
  let listener = TcpListener::bind("127.0.0.1:0").await?;
  let url = format!("http://{}", listener.local_addr()?);
  let task = tokio::spawn(async move { axum::serve(listener, router).await });
  Ok(ServerGuard { url, task })
}

#[derive(Clone, Copy)]
enum Delivery {
  Stall { prefix: usize },
  Truncate { prefix: usize },
  Paced { chunk: usize, delay: Duration },
}

#[derive(Clone, Copy)]
enum RangeReply {
  Valid,
  Ignore,
  WrongStart,
}

#[derive(Clone, Copy)]
enum Metadata {
  Matching,
  WrongFileHash,
  WrongFileSize,
  MissingFileHash,
}

#[derive(Debug, Clone)]
struct NarRequest {
  hash:  String,
  range: Option<String>,
}

struct CacheState {
  payload:          Bytes,
  file_hash:        String,
  delivery:         Delivery,
  range_reply:      RangeReply,
  metadata:         Metadata,
  requests:         Mutex<Vec<NarRequest>>,
  narinfo_requests: AtomicUsize,
  narinfo_delay_ms: AtomicUsize,
  reject_nar:       AtomicUsize,
  emitted:          AtomicUsize,
  active:           AtomicUsize,
  cancelled:        AtomicUsize,
  changed:          Notify,
}

impl CacheState {
  fn requests(&self) -> TestResult<Vec<NarRequest>> {
    Ok(
      self
        .requests
        .lock()
        .map_err(|_| io::Error::other("request log poisoned"))?
        .clone(),
    )
  }

  async fn wait_for_idle(&self) -> TestResult {
    timeout(TEST_DEADLINE, async {
      loop {
        let changed = self.changed.notified();
        if self.active.load(Ordering::SeqCst) == 0 {
          break;
        }
        changed.await;
      }
    })
    .await?;
    Ok(())
  }

  async fn wait_for_active(&self) -> TestResult {
    timeout(TEST_DEADLINE, async {
      loop {
        let changed = self.changed.notified();
        if self.active.load(Ordering::SeqCst) > 0 {
          break;
        }
        changed.await;
      }
    })
    .await?;
    Ok(())
  }

  async fn wait_for_backpressure(&self) -> TestResult {
    timeout(TEST_DEADLINE, async {
      loop {
        let before = self.emitted.load(Ordering::SeqCst);
        sleep(Duration::from_millis(100)).await;
        let after = self.emitted.load(Ordering::SeqCst);
        if before > 0
          && before == after
          && self.active.load(Ordering::SeqCst) > 0
        {
          return;
        }
      }
    })
    .await?;
    Ok(())
  }
}

struct Cache {
  state:  Arc<CacheState>,
  server: ServerGuard,
}

impl Cache {
  async fn start(payload: Bytes, delivery: Delivery) -> TestResult<Self> {
    Self::with_options(payload, delivery, RangeReply::Valid, Metadata::Matching)
      .await
  }

  async fn with_options(
    payload: Bytes,
    delivery: Delivery,
    range_reply: RangeReply,
    metadata: Metadata,
  ) -> TestResult<Self> {
    let digest = Sha256::digest(&payload);
    let file_hash = format!("sha256:{:x}", Bytes::copy_from_slice(&digest));
    let state = Arc::new(CacheState {
      payload,
      file_hash,
      delivery,
      range_reply,
      metadata,
      requests: Mutex::new(Vec::new()),
      narinfo_requests: AtomicUsize::new(0),
      narinfo_delay_ms: AtomicUsize::new(0),
      reject_nar: AtomicUsize::new(0),
      emitted: AtomicUsize::new(0),
      active: AtomicUsize::new(0),
      cancelled: AtomicUsize::new(0),
      changed: Notify::new(),
    });
    let server = serve(
      AxumRouter::new()
        .route("/{filename}", get(narinfo))
        .route("/nar/{filename}", get(nar_body))
        .with_state(Arc::clone(&state)),
    )
    .await?;
    Ok(Self { state, server })
  }

  fn upstream(&self, priority: i32) -> UpstreamConfig {
    UpstreamConfig {
      url: self.server.url.clone(),
      priority,
      ..UpstreamConfig::default()
    }
  }
}

async fn narinfo(
  State(state): State<Arc<CacheState>>,
  Path(filename): Path<String>,
) -> Response {
  let Some(hash) = filename.strip_suffix(".narinfo") else {
    return StatusCode::NOT_FOUND.into_response();
  };
  state.narinfo_requests.fetch_add(1, Ordering::SeqCst);
  sleep(Duration::from_millis(
    state.narinfo_delay_ms.load(Ordering::SeqCst) as u64,
  ))
  .await;
  let file_hash = if matches!(state.metadata, Metadata::WrongFileHash) {
    "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  } else {
    &state.file_hash
  };
  let file_size = state.payload.len()
    + usize::from(matches!(state.metadata, Metadata::WrongFileSize));
  let file_hash_line = if matches!(state.metadata, Metadata::MissingFileHash) {
    String::new()
  } else {
    format!("FileHash: {file_hash}\n")
  };
  let body = format!(
    "StorePath: /nix/store/{hash}-transfer\nURL: nar/{hash}.nar\nCompression: \
     none\n{file_hash_line}FileSize: {file_size}\nNarHash: {}\nNarSize: \
     {}\nReferences: \n",
    state.file_hash,
    state.payload.len(),
  );
  ([(header::CONTENT_TYPE, "text/x-nix-narinfo")], body).into_response()
}

fn parse_range(value: &str, size: usize) -> Option<(usize, usize)> {
  let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
  if start.is_empty() {
    let suffix = end.parse::<usize>().ok()?;
    return (suffix > 0)
      .then_some((size.saturating_sub(suffix), size.checked_sub(1)?));
  }
  let start = start.parse::<usize>().ok()?;
  let end = if end.is_empty() {
    size.checked_sub(1)?
  } else {
    end.parse::<usize>().ok()?
  };
  (start <= end && end < size).then_some((start, end))
}

struct CacheStream {
  state:       Arc<CacheState>,
  offset:      usize,
  end:         usize,
  sent_prefix: bool,
}

impl Drop for CacheStream {
  fn drop(&mut self) {
    if self.offset <= self.end {
      self.state.cancelled.fetch_add(1, Ordering::SeqCst);
    }
    self.state.active.fetch_sub(1, Ordering::SeqCst);
    self.state.changed.notify_one();
  }
}

async fn nar_body(
  State(state): State<Arc<CacheState>>,
  Path(filename): Path<String>,
  headers: HeaderMap,
) -> Response {
  let Some(hash) = filename.strip_suffix(".nar") else {
    return StatusCode::NOT_FOUND.into_response();
  };
  let range = headers
    .get(header::RANGE)
    .and_then(|value| value.to_str().ok())
    .map(str::to_owned);
  let Ok(mut requests) = state.requests.lock() else {
    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
  };
  requests.push(NarRequest {
    hash:  hash.to_owned(),
    range: range.clone(),
  });
  drop(requests);
  if state.reject_nar.load(Ordering::SeqCst) != 0 {
    return StatusCode::BAD_GATEWAY.into_response();
  }
  let size = state.payload.len();
  let requested = match range.as_deref() {
    Some(value) => {
      match parse_range(value, size) {
        Some(bounds) => Some(bounds),
        None => return StatusCode::RANGE_NOT_SATISFIABLE.into_response(),
      }
    },
    None => None,
  };
  let partial =
    requested.is_some() && !matches!(state.range_reply, RangeReply::Ignore);
  let (start, end) = if partial {
    requested.unwrap_or((0, size - 1))
  } else {
    (0, size - 1)
  };
  state.active.fetch_add(1, Ordering::SeqCst);
  let body_state = CacheStream {
    state: Arc::clone(&state),
    offset: start,
    end,
    sent_prefix: false,
  };
  let body = Body::from_stream(stream::unfold(body_state, |mut body| {
    async move {
      if body.offset > body.end {
        return None;
      }
      let chunk_size = match body.state.delivery {
        Delivery::Stall { prefix } | Delivery::Truncate { prefix }
          if !body.sent_prefix =>
        {
          prefix
        },
        Delivery::Stall { .. } => {
          pending::<()>().await;
          return None;
        },
        Delivery::Truncate { .. } => {
          // Ensure the prefix reaches the real HTTP consumer before the framing
          // error; this must not become a first-byte hedge/retry test.
          sleep(Duration::from_millis(30)).await;
          return None;
        },
        Delivery::Paced { chunk, delay } => {
          if body.sent_prefix && !delay.is_zero() {
            sleep(delay).await;
          }
          chunk
        },
      };
      let next = body.offset.saturating_add(chunk_size).min(body.end + 1);
      let bytes = body.state.payload.slice(body.offset..next);
      body.state.emitted.fetch_add(bytes.len(), Ordering::SeqCst);
      body.state.changed.notify_one();
      body.offset = next;
      body.sent_prefix = true;
      Some((Ok::<Bytes, io::Error>(bytes), body))
    }
  }));
  let mut response = body.into_response();
  *response.status_mut() = if partial {
    StatusCode::PARTIAL_CONTENT
  } else {
    StatusCode::OK
  };
  let Ok(content_length) = (end - start + 1).to_string().parse() else {
    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
  };
  response
    .headers_mut()
    .insert(header::CONTENT_LENGTH, content_length);
  response.headers_mut().insert(
    header::ACCEPT_RANGES,
    header::HeaderValue::from_static("bytes"),
  );
  if partial {
    let reported_start =
      start + usize::from(matches!(state.range_reply, RangeReply::WrongStart));
    let Ok(value) = format!("bytes {reported_start}-{end}/{size}").parse()
    else {
      return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    response.headers_mut().insert(header::CONTENT_RANGE, value);
  }
  response
}

fn payload(size: usize) -> Bytes {
  // Nonuniform bytes make offset duplication and skipped bytes observable.
  Bytes::from((0_u8..=250).cycle().take(size).collect::<Vec<_>>())
}

const fn fast() -> Delivery {
  Delivery::Paced {
    chunk: 32 * 1024,
    delay: Duration::from_millis(5),
  }
}

const fn transfer_config() -> NarTransferConfig {
  NarTransferConfig {
    enabled:             true,
    throughput_alpha:    0.3,
    sample_min_bytes:    1024,
    sample_min_duration: HumanDuration(Duration::from_millis(5)),
    stats_ttl:           HumanDuration(Duration::from_secs(30)),
    stall_timeout:       HumanDuration(Duration::from_millis(120)),
    // Recovery tests disable proactive slow switching without disabling
    // adaptive measurements or stall/error recovery.
    slow_window:         HumanDuration(Duration::from_secs(20)),
    switch_margin:       HumanDuration(Duration::from_millis(5)),
    min_improvement:     0.1,
    max_switches:        2,
    probe_bytes:         16 * 1024,
    probe_timeout:       HumanDuration(Duration::from_secs(1)),
  }
}

struct Proxy {
  server: ServerGuard,
  client: reqwest::Client,
}

impl Proxy {
  async fn start(
    upstreams: Vec<UpstreamConfig>,
    config: NarTransferConfig,
  ) -> TestResult<Self> {
    let db = Db::open(":memory:", 100, Duration::from_secs(1)).await?;
    let prober = Prober::new(0.3)?;
    prober.init_upstreams(&upstreams).await;
    let router = Router::new(
      db.clone(),
      prober.clone(),
      Duration::from_secs(60),
      Duration::from_secs(3),
      Duration::from_secs(1),
      RouterTuning {
        max_concurrent_races:      4,
        per_upstream_max_inflight: 4,
        in_memory_negative_ttl:    Duration::from_secs(1),
        upstream_cooldown:         Duration::from_millis(10),
      },
    )?;
    for upstream in &upstreams {
      router.register_upstream(upstream).await?;
    }
    let first = upstreams
      .first()
      .ok_or_else(|| io::Error::other("missing initial upstream"))?;
    for hash in [HASH, NEXT_HASH] {
      router
        .resolve(hash, from_ref(&first.url), Hop::Client)
        .await?;
    }
    let server = serve(app(router, prober, db, AppConfig {
      upstreams,
      fallback_cache: None,
      nar_hedging: NarHedgingConfig {
        enabled: false,
        ..NarHedgingConfig::default()
      },
      nar_transfer: config,
      cache_priority: 40,
      want_mass_query: false,
      read_timeout: Duration::from_secs(3),
      write_timeout: Duration::from_secs(3),
    })?)
    .await?;
    let client = reqwest::Client::builder().timeout(TEST_DEADLINE).build()?;
    Ok(Self { server, client })
  }

  async fn get(
    &self,
    hash: &str,
    range: Option<&str>,
  ) -> TestResult<reqwest::Response> {
    let mut request = self
      .client
      .get(format!("{}/nar/{hash}.nar", self.server.url));
    if let Some(range) = range {
      request = request.header(header::RANGE, range);
    }
    Ok(request.send().await?)
  }
}

async fn assert_failed_body(
  mut response: reqwest::Response,
  expected_prefix: &[u8],
) -> TestResult {
  assert!(
    response.status().is_success(),
    "failure must occur after response headers"
  );
  let (received, failed) = timeout(TEST_DEADLINE, async {
    let mut received = Vec::new();
    loop {
      match response.chunk().await {
        Ok(Some(chunk)) => received.extend_from_slice(&chunk),
        Ok(None) => return (received, false),
        Err(_) => return (received, true),
      }
    }
  })
  .await?;
  assert!(
    failed,
    "an unsafe continuation must fail the body, not silently finish"
  );
  assert_eq!(
    received, expected_prefix,
    "no unvalidated alternate bytes may escape"
  );
  Ok(())
}

async fn assert_recovery(
  delivery: Delivery,
  range: Option<&str>,
) -> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), delivery).await?;
  let alternate = Cache::start(data.clone(), fast()).await?;
  let proxy = Proxy::start(
    vec![source.upstream(1), alternate.upstream(2)],
    transfer_config(),
  )
  .await?;
  let response = proxy.get(HASH, range).await?;
  let (start, end) = range
    .and_then(|value| parse_range(value, data.len()))
    .unwrap_or((0, data.len() - 1));
  assert_eq!(
    response.status(),
    if range.is_some() {
      StatusCode::PARTIAL_CONTENT
    } else {
      StatusCode::OK
    }
  );
  assert_eq!(
    response.content_length(),
    Some(u64::try_from(end - start + 1)?)
  );
  if range.is_some() {
    assert_eq!(
      response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok()),
      Some(format!("bytes {start}-{end}/{}", data.len()).as_str())
    );
  }
  assert_eq!(response.bytes().await?, data.slice(start..=end));
  let source_requests = source.state.requests()?;
  assert_eq!(
    source_requests.len(),
    1,
    "first-byte hedging must not replace the source"
  );
  assert_eq!(source_requests[0].range.as_deref(), range);
  let alternate_requests = alternate.state.requests()?;
  assert_eq!(alternate_requests.len(), 1);
  assert_eq!(
    alternate_requests[0].range.as_deref(),
    Some(format!("bytes={}-{}", start + PREFIX_SIZE, end).as_str())
  );
  source.state.wait_for_idle().await?;
  alternate.state.wait_for_idle().await?;
  Ok(())
}

#[tokio::test]
async fn stalled_prefix_recovers_with_exact_range_bytes() -> TestResult {
  assert_recovery(
    Delivery::Stall {
      prefix: PREFIX_SIZE,
    },
    None,
  )
  .await
}

#[tokio::test]
async fn prematurely_terminated_body_resumes_without_duplication() -> TestResult
{
  assert_recovery(
    Delivery::Truncate {
      prefix: PREFIX_SIZE,
    },
    None,
  )
  .await
}

#[tokio::test]
async fn caller_range_preserves_absolute_offset_end_and_partial_status()
-> TestResult {
  assert_recovery(
    Delivery::Stall {
      prefix: PREFIX_SIZE,
    },
    Some("bytes=4096-131071"),
  )
  .await
}

#[tokio::test]
async fn suffix_range_recovers_using_absolute_tail_offset() -> TestResult {
  assert_recovery(
    Delivery::Stall {
      prefix: PREFIX_SIZE,
    },
    Some("bytes=-131072"),
  )
  .await
}

#[tokio::test]
async fn matching_missing_file_hashes_cannot_enable_resumption() -> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::with_options(
    data.clone(),
    Delivery::Truncate {
      prefix: PREFIX_SIZE,
    },
    RangeReply::Valid,
    Metadata::MissingFileHash,
  )
  .await?;
  let alternate = Cache::with_options(
    data.clone(),
    fast(),
    RangeReply::Valid,
    Metadata::MissingFileHash,
  )
  .await?;
  let proxy = Proxy::start(
    vec![source.upstream(1), alternate.upstream(2)],
    transfer_config(),
  )
  .await?;
  assert_failed_body(proxy.get(HASH, None).await?, &data[..PREFIX_SIZE])
    .await?;
  assert!(
    alternate.state.requests()?.is_empty(),
    "matching absent compressed hashes cannot establish byte equivalence"
  );
  source.state.wait_for_idle().await?;
  Ok(())
}

#[tokio::test]
async fn mismatched_compressed_hash_or_size_is_never_used_for_recovery()
-> TestResult {
  for metadata in [Metadata::WrongFileHash, Metadata::WrongFileSize] {
    let data = payload(PAYLOAD_SIZE);
    let source = Cache::start(data.clone(), Delivery::Stall {
      prefix: PREFIX_SIZE,
    })
    .await?;
    let alternate =
      Cache::with_options(data.clone(), fast(), RangeReply::Valid, metadata)
        .await?;
    let proxy = Proxy::start(
      vec![source.upstream(1), alternate.upstream(2)],
      transfer_config(),
    )
    .await?;
    assert_failed_body(proxy.get(HASH, None).await?, &data[..PREFIX_SIZE])
      .await?;
    assert!(alternate.state.narinfo_requests.load(Ordering::SeqCst) > 0);
    assert!(
      alternate.state.requests()?.is_empty(),
      "metadata mismatch must reject the alternate before fetching its NAR"
    );
    source.state.wait_for_idle().await?;
  }
  Ok(())
}

#[tokio::test]
async fn ignored_range_or_wrong_content_range_fails_instead_of_corrupting()
-> TestResult {
  for range_reply in [RangeReply::Ignore, RangeReply::WrongStart] {
    let data = payload(PAYLOAD_SIZE);
    let source = Cache::start(data.clone(), Delivery::Stall {
      prefix: PREFIX_SIZE,
    })
    .await?;
    let alternate = Cache::with_options(
      data.clone(),
      fast(),
      range_reply,
      Metadata::Matching,
    )
    .await?;
    let proxy = Proxy::start(
      vec![source.upstream(1), alternate.upstream(2)],
      transfer_config(),
    )
    .await?;
    assert_failed_body(proxy.get(HASH, None).await?, &data[..PREFIX_SIZE])
      .await?;
    let requests = alternate.state.requests()?;
    assert_eq!(requests.len(), 1);
    assert_eq!(
      requests[0].range.as_deref(),
      Some(format!("bytes={PREFIX_SIZE}-{}", data.len() - 1).as_str())
    );
    source.state.wait_for_idle().await?;
    alternate.state.wait_for_idle().await?;
  }
  Ok(())
}

#[tokio::test]
async fn sustained_slow_source_probes_unknown_alternate_and_reuses_fast_history()
-> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), Delivery::Paced {
    chunk: 4096,
    delay: Duration::from_millis(25),
  })
  .await?;
  let alternate = Cache::start(data.clone(), fast()).await?;
  let mut config = transfer_config();
  config.stall_timeout = HumanDuration(Duration::from_secs(2));
  config.slow_window = HumanDuration(Duration::from_millis(100));
  let proxy =
    Proxy::start(vec![source.upstream(1), alternate.upstream(1)], config)
      .await?;

  assert_eq!(proxy.get(HASH, None).await?.bytes().await?, data);
  let first_requests = alternate.state.requests()?;
  assert_eq!(
    first_requests.len(),
    1,
    "a successful probe should retain the replacement response through EOF"
  );
  let probe = first_requests
    .first()
    .and_then(|request| request.range.as_deref())
    .and_then(|value| parse_range(value, data.len()))
    .ok_or_else(|| io::Error::other("missing replacement probe Range"))?;
  assert!(
    probe.0 > 0,
    "the initial source must emit real data before probing"
  );
  assert_eq!(
    probe.1,
    data.len() - 1,
    "the retained probe response must cover the remaining object"
  );
  assert!(
    source.state.emitted.load(Ordering::SeqCst) < data.len(),
    "source must switch before the slow stream finishes"
  );
  source.state.wait_for_idle().await?;
  alternate.state.wait_for_idle().await?;
  let source_request_count = source.state.requests()?.len();
  let alternate_request_count = first_requests.len();

  // NEXT_HASH was independently routed to the slow cache before either NAR
  // transfer. Choosing the fast cache therefore requires shared passive
  // history, not just remembering a route winner for HASH.
  assert_eq!(proxy.get(NEXT_HASH, None).await?.bytes().await?, data);
  assert_eq!(
    source.state.requests()?.len(),
    source_request_count,
    "cached throughput must prefer the faster cache without restarting the \
     routed slow one"
  );
  let subsequent = alternate.state.requests()?;
  assert_eq!(subsequent.len(), alternate_request_count + 1);
  assert_eq!(subsequent[alternate_request_count].hash, NEXT_HASH);
  assert!(
    subsequent[alternate_request_count].range.is_none(),
    "the subsequent download should select the fast source immediately, \
     without another probe"
  );
  alternate.state.wait_for_idle().await?;
  Ok(())
}

#[tokio::test]
async fn unsuitable_unknown_probe_is_byte_bounded_and_cancelled() -> TestResult
{
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), Delivery::Paced {
    chunk: 4096,
    delay: Duration::from_millis(5),
  })
  .await?;
  let alternate_chunk = 1024;
  let alternate = Cache::start(data.clone(), Delivery::Paced {
    chunk: alternate_chunk,
    delay: Duration::from_millis(50),
  })
  .await?;
  let mut config = transfer_config();
  config.stall_timeout = HumanDuration(Duration::from_secs(4));
  config.slow_window = HumanDuration(Duration::from_millis(100));
  // A 16KiB probe ends well before this deadline at the fixture's pace. The
  // emitted-byte bound below distinguishes a byte cap from waiting for timeout.
  config.probe_timeout = HumanDuration(Duration::from_secs(3));
  let probe_bytes = usize::try_from(config.probe_bytes)?;
  let max_attempts = usize::try_from(config.max_switches)?;
  let proxy =
    Proxy::start(vec![source.upstream(1), alternate.upstream(1)], config)
      .await?;
  assert_eq!(proxy.get(HASH, None).await?.bytes().await?, data);
  alternate.state.wait_for_idle().await?;
  let requests = alternate.state.requests()?;
  assert!(
    !requests.is_empty(),
    "the unknown candidate must actually be probed"
  );
  assert!(
    requests.len() <= max_attempts,
    "rejected probes must consume the alternate request budget"
  );
  for request in &requests {
    let range = request
      .range
      .as_deref()
      .and_then(|value| parse_range(value, data.len()))
      .ok_or_else(|| io::Error::other("probe lacked a valid Range"))?;
    assert!(range.0 > 0);
  }
  assert_eq!(
    alternate.state.cancelled.load(Ordering::SeqCst),
    requests.len(),
    "unsuitable probe bodies must be dropped before EOF"
  );
  assert!(
    alternate.state.emitted.load(Ordering::SeqCst)
      <= requests.len() * (probe_bytes + 2 * alternate_chunk),
    "a rejected paced probe must stop after a bounded prefix, allowing at \
     most two chunks already in flight"
  );
  assert_eq!(
    source.state.requests()?.len(),
    1,
    "rejecting a worse source must continue the original HTTP response"
  );
  source.state.wait_for_idle().await?;
  Ok(())
}

#[tokio::test]
async fn alternate_opt_out_prevents_midstream_requests() -> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), Delivery::Stall {
    prefix: PREFIX_SIZE,
  })
  .await?;
  let alternate = Cache::start(data.clone(), fast()).await?;
  let mut alternate_config = alternate.upstream(2);
  alternate_config.allow_hedging = false;
  let proxy = Proxy::start(
    vec![source.upstream(1), alternate_config],
    transfer_config(),
  )
  .await?;
  assert_failed_body(proxy.get(HASH, None).await?, &data[..PREFIX_SIZE])
    .await?;
  assert!(alternate.state.requests()?.is_empty());
  source.state.wait_for_idle().await?;
  Ok(())
}

#[tokio::test]
async fn zero_switch_budget_disables_midstream_recovery() -> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), Delivery::Truncate {
    prefix: PREFIX_SIZE,
  })
  .await?;
  let alternate = Cache::start(data.clone(), fast()).await?;
  let mut config = transfer_config();
  config.max_switches = 0;
  let proxy =
    Proxy::start(vec![source.upstream(1), alternate.upstream(2)], config)
      .await?;
  assert_failed_body(proxy.get(HASH, None).await?, &data[..PREFIX_SIZE])
    .await?;
  assert!(alternate.state.requests()?.is_empty());
  source.state.wait_for_idle().await?;
  Ok(())
}

#[tokio::test]
async fn disabled_adaptive_transfer_leaves_truncated_body_failed() -> TestResult
{
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), Delivery::Truncate {
    prefix: PREFIX_SIZE,
  })
  .await?;
  let alternate = Cache::start(data.clone(), fast()).await?;
  let mut config = transfer_config();
  config.enabled = false;
  let proxy =
    Proxy::start(vec![source.upstream(1), alternate.upstream(2)], config)
      .await?;
  assert_failed_body(proxy.get(HASH, None).await?, &data[..PREFIX_SIZE])
    .await?;
  assert!(
    alternate.state.requests()?.is_empty(),
    "disabled adaptation must not attempt mid-stream recovery"
  );
  source.state.wait_for_idle().await?;
  Ok(())
}

#[tokio::test]
async fn downstream_backpressure_does_not_trigger_switch_and_cancellation_drops_source()
-> TestResult {
  let data = payload(32 * PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), Delivery::Paced {
    chunk: 32 * 1024,
    delay: Duration::ZERO,
  })
  .await?;
  let alternate = Cache::start(data.clone(), fast()).await?;
  let mut config = transfer_config();
  config.stall_timeout = HumanDuration(Duration::from_millis(60));
  let proxy =
    Proxy::start(vec![source.upstream(1), alternate.upstream(2)], config)
      .await?;
  let mut response = proxy.get(HASH, None).await?;
  let first = response
    .chunk()
    .await?
    .ok_or_else(|| io::Error::other("missing first chunk"))?;
  assert_eq!(first, data.slice(..first.len()));
  // Holding a response without consuming it is deliberate backpressure, not
  // an upstream stall. The large payload prevents socket buffers from hiding
  // it.
  source.state.wait_for_backpressure().await?;
  sleep(Duration::from_millis(400)).await;
  assert!(source.state.emitted.load(Ordering::SeqCst) < data.len());
  assert!(
    alternate.state.requests()?.is_empty(),
    "a consumer that stops polling must not launch recovery requests"
  );
  drop(response);
  source.state.wait_for_idle().await?;
  assert!(
    source.state.emitted.load(Ordering::SeqCst) < data.len(),
    "cancellation must stop upstream work before EOF"
  );
  assert!(alternate.state.requests()?.is_empty());
  Ok(())
}

#[tokio::test]
async fn consumer_cancellation_drops_inflight_probe_and_original_body()
-> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), Delivery::Paced {
    chunk: 4096,
    delay: Duration::from_millis(25),
  })
  .await?;
  // One byte proves a real probe response arrived, then leaves the probe
  // pending so cancellation is exercised while supervision owns both bodies.
  let alternate =
    Cache::start(data.clone(), Delivery::Stall { prefix: 1 }).await?;
  let mut config = transfer_config();
  config.stall_timeout = HumanDuration(Duration::from_secs(4));
  config.slow_window = HumanDuration(Duration::from_millis(100));
  config.probe_timeout = HumanDuration(Duration::from_secs(3));
  let proxy =
    Proxy::start(vec![source.upstream(1), alternate.upstream(1)], config)
      .await?;
  let mut response = proxy.get(HASH, None).await?;
  let first = response
    .chunk()
    .await?
    .ok_or_else(|| io::Error::other("missing original prefix"))?;
  assert_eq!(first, data.slice(..first.len()));
  let download = tokio::spawn(async move {
    while response.chunk().await?.is_some() {}
    Ok::<(), reqwest::Error>(())
  });
  let started = alternate.state.wait_for_active().await;
  // Abort even if the milestone failed, so this helper never detaches a task.
  download.abort();
  let outcome = download.await;
  started?;
  assert!(outcome.is_err_and(|error| error.is_cancelled()));
  alternate.state.wait_for_idle().await?;
  source.state.wait_for_idle().await?;
  assert_eq!(alternate.state.requests()?.len(), 1);
  assert_eq!(alternate.state.cancelled.load(Ordering::SeqCst), 1);
  assert_eq!(source.state.cancelled.load(Ordering::SeqCst), 1);
  assert!(source.state.emitted.load(Ordering::SeqCst) < data.len());
  Ok(())
}

#[tokio::test]
async fn regression_disabled_adaptation_does_not_shorten_narinfo_discovery()
-> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let cache = Cache::start(data.clone(), fast()).await?;
  cache.state.narinfo_delay_ms.store(30, Ordering::SeqCst);
  let mut config = transfer_config();
  config.enabled = false;
  config.probe_timeout = HumanDuration(Duration::from_millis(1));
  let proxy = Proxy::start(vec![cache.upstream(1)], config).await?;
  // This hash has no cached route: normal discovery must honor the upstream's
  // narinfo timeout rather than an unrelated, disabled adaptive-probe deadline.
  let hash = "2123456789abcdfghijklmnpqrsvwxyz";
  let response = proxy.get(hash, None).await?;
  assert_eq!(response.status(), StatusCode::OK);
  assert_eq!(response.bytes().await?, data);
  cache.state.wait_for_idle().await?;
  Ok(())
}

#[tokio::test]
async fn regression_reranked_opted_out_route_is_not_an_additional_attempt()
-> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), fast()).await?;
  let alternate = Cache::start(data, fast()).await?;
  alternate.state.reject_nar.store(1, Ordering::SeqCst);
  let mut routed = source.upstream(2);
  routed.allow_hedging = false;
  let proxy =
    Proxy::start(vec![routed, alternate.upstream(1)], transfer_config())
      .await?;
  // The priority-1 source becomes initial. The old routed priority-2 source
  // must not then be launched as an additional request after initial failure.
  let response = proxy.get(HASH, None).await?;
  assert_eq!(response.status(), StatusCode::NOT_FOUND);
  assert!(source.state.requests()?.is_empty());
  assert_eq!(alternate.state.requests()?.len(), 1);
  Ok(())
}

#[tokio::test]
async fn completed_replacement_survives_another_candidates_discovery_timeout()
-> TestResult {
  let data = payload(PAYLOAD_SIZE);
  let source = Cache::start(data.clone(), Delivery::Stall {
    prefix: PREFIX_SIZE,
  })
  .await?;
  let ready = Cache::start(data.clone(), fast()).await?;
  let slow = Cache::start(data.clone(), fast()).await?;
  slow.state.narinfo_delay_ms.store(250, Ordering::SeqCst);
  let mut config = transfer_config();
  config.probe_timeout = HumanDuration(Duration::from_millis(100));
  let proxy = Proxy::start(
    vec![source.upstream(1), ready.upstream(2), slow.upstream(3)],
    config,
  )
  .await?;
  assert_eq!(proxy.get(HASH, None).await?.bytes().await?, data);
  assert_eq!(ready.state.requests()?.len(), 1);
  assert!(slow.state.requests()?.is_empty());
  source.state.wait_for_idle().await?;
  ready.state.wait_for_idle().await?;
  Ok(())
}
