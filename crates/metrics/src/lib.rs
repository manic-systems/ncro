use std::sync::OnceLock;

use prometheus::{
  Encoder,
  HistogramOpts,
  HistogramVec,
  IntCounter,
  IntCounterVec,
  IntGauge,
  Opts,
  Registry,
  TextEncoder,
  core::Collector,
};

pub struct Metrics {
  registry:                                  Registry,
  pub narinfo_cache_hits:                    IntCounter,
  pub narinfo_cache_misses:                  IntCounter,
  pub narinfo_memory_negative_hits:          IntCounter,
  pub narinfo_singleflight_waiters:          IntCounter,
  pub narinfo_requests:                      IntCounterVec,
  pub nar_requests:                          IntCounter,
  pub nar_hedges:                            IntCounterVec,
  pub nar_hedge_failures:                    IntCounterVec,
  pub narinfo_upstream_attempts:             IntCounter,
  pub narinfo_upstream_attempts_per_resolve: HistogramVec,
  pub narinfo_race_wait_seconds:             HistogramVec,
  pub upstream_race_wins:                    IntCounterVec,
  pub route_entries:                         IntGauge,
  pub upstream_latency:                      HistogramVec,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Returns the global [`Metrics`] instance, initializing it on first call.
///
/// # Panics
///
/// Panics if any Prometheus metric or counter cannot be constructed (only
/// possible if metric names or label arrays contain invalid characters, which
/// cannot happen with the static constants used here).
#[expect(
  clippy::expect_used,
  reason = "metric names and labels are static constants validated during \
            startup"
)]
pub fn get() -> &'static Metrics {
  METRICS.get_or_init(|| {
    let registry = Registry::new();
    let narinfo_cache_hits = IntCounter::new(
      "ncro_narinfo_cache_hits_total",
      "Narinfo requests served from route cache.",
    )
    .expect("valid metric");
    let narinfo_cache_misses = IntCounter::new(
      "ncro_narinfo_cache_misses_total",
      "Narinfo requests requiring upstream race.",
    )
    .expect("valid metric");
    let narinfo_memory_negative_hits = IntCounter::new(
      "ncro_narinfo_memory_negative_hits_total",
      "Narinfo requests denied by short-lived in-memory negative cache.",
    )
    .expect("valid metric");
    let narinfo_singleflight_waiters = IntCounter::new(
      "ncro_narinfo_singleflight_waiters_total",
      "Narinfo requests that waited on an in-flight same-hash lookup.",
    )
    .expect("valid metric");
    let narinfo_requests = IntCounterVec::new(
      Opts::new("ncro_narinfo_requests_total", "Narinfo requests by status."),
      &["status"],
    )
    .expect("valid metric");
    let nar_requests =
      IntCounter::new("ncro_nar_requests_total", "NAR streaming requests.")
        .expect("valid metric");
    let nar_hedges = IntCounterVec::new(
      Opts::new("ncro_nar_hedges_total", "NAR hedge events."),
      &["event", "upstream"],
    )
    .expect("valid metric");
    let nar_hedge_failures = IntCounterVec::new(
      Opts::new(
        "ncro_nar_hedge_failures_total",
        "Failed NAR hedge attempts.",
      ),
      &["kind", "upstream"],
    )
    .expect("valid metric");
    let narinfo_upstream_attempts = IntCounter::new(
      "ncro_narinfo_upstream_attempts_total",
      "Total upstream narinfo attempts made during races.",
    )
    .expect("valid metric");
    let narinfo_upstream_attempts_per_resolve = HistogramVec::new(
      HistogramOpts::new(
        "ncro_narinfo_upstream_attempts_per_resolve",
        "Upstream attempts per narinfo resolution.",
      ),
      &["result"],
    )
    .expect("valid metric");
    let narinfo_race_wait_seconds = HistogramVec::new(
      HistogramOpts::new(
        "ncro_narinfo_race_wait_seconds",
        "Time spent waiting for race concurrency permit.",
      ),
      &["kind"],
    )
    .expect("valid metric");
    let upstream_race_wins = IntCounterVec::new(
      Opts::new(
        "ncro_upstream_race_wins_total",
        "Times each upstream won the narinfo race.",
      ),
      &["upstream"],
    )
    .expect("valid metric");
    let route_entries = IntGauge::new(
      "ncro_route_entries",
      "Current number of route entries in SQLite.",
    )
    .expect("valid metric");
    let upstream_latency = HistogramVec::new(
      HistogramOpts::new(
        "ncro_upstream_latency_seconds",
        "Upstream narinfo race latency.",
      ),
      &["upstream"],
    )
    .expect("valid metric");

    for collector in [
      Box::new(narinfo_cache_hits.clone()) as Box<dyn Collector>,
      Box::new(narinfo_cache_misses.clone()),
      Box::new(narinfo_memory_negative_hits.clone()),
      Box::new(narinfo_singleflight_waiters.clone()),
      Box::new(narinfo_requests.clone()),
      Box::new(nar_requests.clone()),
      Box::new(nar_hedges.clone()),
      Box::new(nar_hedge_failures.clone()),
      Box::new(narinfo_upstream_attempts.clone()),
      Box::new(narinfo_upstream_attempts_per_resolve.clone()),
      Box::new(narinfo_race_wait_seconds.clone()),
      Box::new(upstream_race_wins.clone()),
      Box::new(route_entries.clone()),
      Box::new(upstream_latency.clone()),
    ] {
      registry.register(collector).expect("register metric");
    }

    Metrics {
      registry,
      narinfo_cache_hits,
      narinfo_cache_misses,
      narinfo_memory_negative_hits,
      narinfo_singleflight_waiters,
      narinfo_requests,
      nar_requests,
      nar_hedges,
      nar_hedge_failures,
      narinfo_upstream_attempts,
      narinfo_upstream_attempts_per_resolve,
      narinfo_race_wait_seconds,
      upstream_race_wins,
      route_entries,
      upstream_latency,
    }
  })
}

#[must_use]
pub fn gather() -> String {
  let mut buf = Vec::new();
  let encoder = TextEncoder::new();
  if encoder.encode(&get().registry.gather(), &mut buf).is_err() {
    return String::new();
  }
  String::from_utf8_lossy(&buf).into_owned()
}
