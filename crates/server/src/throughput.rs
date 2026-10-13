#![expect(
  clippy::cast_precision_loss,
  reason = "Transfer forecasts intentionally approximate byte counts and \
            concurrency as floating-point rates."
)]

use std::{
  collections::HashMap,
  sync::{Arc, Mutex, MutexGuard, PoisonError},
  time::{Duration, Instant},
};

use ncro_config::NarTransferConfig;

const MAX_UPSTREAMS: usize = 1024;

#[derive(Clone)]
pub struct ThroughputStats {
  shared: Arc<Shared>,
}

struct Shared {
  entries:      Mutex<HashMap<Arc<str>, Entry>>,
  alpha:        f64,
  min_bytes:    u64,
  min_duration: Duration,
  ttl:          Duration,
}

struct Entry {
  active:        usize,
  last_activity: Instant,
  first_byte:    Option<(Duration, Instant)>,
  rate:          Option<Rate>,
}

#[derive(Clone, Copy)]
struct Rate {
  bytes_per_sec: f64,
  concurrency:   usize,
  updated:       Instant,
}

#[derive(Clone, Copy, Debug)]
pub struct Estimate {
  pub(super) bytes_per_sec: f64,
  pub(super) first_byte:    Duration,
}

impl Estimate {
  pub(super) fn completion_time(&self, remaining: u64) -> Duration {
    let transfer =
      Duration::try_from_secs_f64(remaining as f64 / self.bytes_per_sec)
        .unwrap_or(Duration::MAX);
    self.first_byte.saturating_add(transfer)
  }
}

/// A request's active count is released on cancellation as well as completion.
/// Only completed measurement windows are published; drop discards the tail.
pub struct TransferSample {
  stats:          ThroughputStats,
  upstream:       Option<Arc<str>>,
  bytes:          u64,
  active_elapsed: Duration,
  concurrency:    usize,
}

impl ThroughputStats {
  pub(super) fn new(config: &NarTransferConfig) -> Self {
    Self {
      shared: Arc::new(Shared {
        entries:      Mutex::new(HashMap::new()),
        alpha:        config.throughput_alpha,
        min_bytes:    config.sample_min_bytes,
        min_duration: config.sample_min_duration.0,
        ttl:          config.stats_ttl.0,
      }),
    }
  }

  fn entries(&self) -> MutexGuard<'_, HashMap<Arc<str>, Entry>> {
    self
      .shared
      .entries
      .lock()
      .unwrap_or_else(PoisonError::into_inner)
  }

  pub(super) fn start(&self, upstream: &str) -> TransferSample {
    self.start_at(upstream, Instant::now())
  }

  #[expect(
    clippy::expect_used,
    reason = "Registration and active-count updates hold the same registry \
              lock."
  )]
  fn start_at(&self, upstream: &str, now: Instant) -> TransferSample {
    let mut entries = self.entries();
    entries.retain(|_, entry| {
      entry.active != 0 || fresh(entry.last_activity, now, self.shared.ttl)
    });

    // A new source can displace old inactive statistics, never live counters.
    if !entries.contains_key(upstream) && entries.len() == MAX_UPSTREAMS {
      let oldest = entries
        .iter()
        .filter(|(_, entry)| entry.active == 0)
        .min_by_key(|(_, entry)| entry.last_activity)
        .map(|(key, _)| Arc::clone(key));
      if let Some(oldest) = oldest {
        entries.remove(&oldest);
      }
    }

    let key = if let Some((key, _)) = entries.get_key_value(upstream) {
      Some(Arc::clone(key))
    } else if entries.len() < MAX_UPSTREAMS {
      let key: Arc<str> = Arc::from(upstream);
      entries.insert(Arc::clone(&key), Entry {
        active:        0,
        last_activity: now,
        first_byte:    None,
        rate:          None,
      });
      Some(key)
    } else {
      // All registry slots are live. This request remains unmeasured rather
      // than making the registry unbounded or losing another request's count.
      None
    };

    let concurrency = key.as_ref().map_or(1, |key| {
      let entry = entries.get_mut(key).expect("registered upstream");
      entry.active = entry.active.saturating_add(1);
      entry.last_activity = now;
      entry.active
    });
    drop(entries);
    TransferSample {
      stats: self.clone(),
      upstream: key,
      bytes: 0,
      active_elapsed: Duration::ZERO,
      concurrency,
    }
  }

  /// Forecasts a new request, including its contention with current requests.
  pub(super) fn estimate(&self, upstream: &str) -> Option<Estimate> {
    self.estimate_at(upstream, Instant::now())
  }

  fn estimate_at(&self, upstream: &str, now: Instant) -> Option<Estimate> {
    let entries = self.entries();
    let entry = entries.get(upstream)?;
    let (first_byte, first_byte_at) = entry.first_byte?;
    let rate = entry.rate?;
    if !fresh(first_byte_at, now, self.shared.ttl)
      || !fresh(rate.updated, now, self.shared.ttl)
    {
      return None;
    }
    let next_concurrency = entry.active.saturating_add(1);
    let contention =
      (rate.concurrency as f64 / next_concurrency as f64).min(1.0);
    drop(entries);
    Some(Estimate {
      bytes_per_sec: rate.bytes_per_sec * contention,
      first_byte,
    })
  }
}

impl TransferSample {
  pub(super) fn first_byte(&self, elapsed: Duration) {
    self.first_byte_at(elapsed, Instant::now());
  }

  #[expect(
    clippy::expect_used,
    reason = "Active entries cannot be evicted before their sample is dropped."
  )]
  fn first_byte_at(&self, elapsed: Duration, now: Instant) {
    let Some(key) = &self.upstream else { return };
    let mut entries = self.stats.entries();
    let entry = entries.get_mut(key).expect("live upstream");
    entry.first_byte = Some((elapsed, now));
    entry.last_activity = now;
    drop(entries);
  }

  /// `active_elapsed` is upstream wait time, never downstream backpressure.
  pub(super) fn record(&mut self, bytes: u64, active_elapsed: Duration) {
    self.record_at(bytes, active_elapsed, Instant::now());
  }

  #[expect(
    clippy::expect_used,
    reason = "Active entries cannot be evicted before their sample is dropped."
  )]
  fn record_at(&mut self, bytes: u64, active_elapsed: Duration, now: Instant) {
    let Some(key) = &self.upstream else { return };
    self.bytes = self.bytes.saturating_add(bytes);
    self.active_elapsed = self.active_elapsed.saturating_add(active_elapsed);
    let mut entries = self.stats.entries();
    let entry = entries.get_mut(key).expect("live upstream");
    entry.last_activity = now;
    // Using the lowest observed concurrency is conservative even if other
    // requests start or finish between chunks. No concurrency normalization
    // may inflate the rate actually measured by this request.
    self.concurrency = self.concurrency.min(entry.active);
    if self.bytes < self.stats.shared.min_bytes
      || self.active_elapsed < self.stats.shared.min_duration
    {
      return;
    }
    let rate = self.bytes as f64 / self.active_elapsed.as_secs_f64();
    if valid_rate(rate) {
      let previous = entry
        .rate
        .filter(|previous| fresh(previous.updated, now, self.stats.shared.ttl));
      let (bytes_per_sec, concurrency) =
        previous.map_or((rate, self.concurrency), |previous| {
          (
            previous.bytes_per_sec.mul_add(
              1.0 - self.stats.shared.alpha,
              rate * self.stats.shared.alpha,
            ),
            previous.concurrency.min(self.concurrency),
          )
        });
      if valid_rate(bytes_per_sec) {
        entry.rate = Some(Rate {
          bytes_per_sec,
          concurrency,
          updated: now,
        });
      }
    }
    self.bytes = 0;
    self.active_elapsed = Duration::ZERO;
    self.concurrency = entry.active;
    drop(entries);
  }

  #[expect(
    clippy::expect_used,
    reason = "A registered sample releases its active count exactly once."
  )]
  fn release_at(&mut self, now: Instant) {
    let Some(key) = self.upstream.take() else {
      return;
    };
    let mut entries = self.stats.entries();
    let entry = entries.get_mut(&key).expect("live upstream");
    entry.active = entry.active.saturating_sub(1);
    entry.last_activity = now;
    drop(entries);
  }
}

impl Drop for TransferSample {
  fn drop(&mut self) {
    self.release_at(Instant::now());
  }
}

fn fresh(updated: Instant, now: Instant, ttl: Duration) -> bool {
  now.saturating_duration_since(updated) < ttl
}

fn valid_rate(rate: f64) -> bool {
  rate.is_finite() && rate > 0.0
}

#[cfg(test)]
mod tests {
  use ncro_config::HumanDuration;

  use super::*;

  fn assert_rate(
    stats: &ThroughputStats,
    upstream: &str,
    now: Instant,
    expected: f64,
  ) {
    let actual = stats.estimate_at(upstream, now);
    assert!(
      actual.is_some_and(|estimate| {
        (estimate.bytes_per_sec - expected).abs()
          <= expected.abs().max(1.0) * 1e-9
      }),
      "expected {expected} bytes/s, got {actual:?}"
    );
  }

  fn stats() -> ThroughputStats {
    ThroughputStats::new(&NarTransferConfig {
      sample_min_bytes: 100,
      sample_min_duration: HumanDuration(Duration::from_secs(1)),
      stats_ttl: HumanDuration(Duration::from_secs(10)),
      throughput_alpha: 0.25,
      ..NarTransferConfig::default()
    })
  }

  fn measure(
    stats: &ThroughputStats,
    upstream: &str,
    bytes: u64,
    first_byte: Duration,
    now: Instant,
  ) {
    let mut sample = stats.start_at(upstream, now);
    sample.first_byte_at(first_byte, now);
    sample.record_at(bytes, Duration::from_secs(1), now);
    sample.release_at(now);
  }

  #[test]
  fn requires_both_window_thresholds_and_discards_small_tails() {
    let stats = stats();
    let now = Instant::now();
    let mut small = stats.start_at("small", now);
    small.first_byte_at(Duration::from_millis(10), now);
    small.record_at(99, Duration::from_secs(1), now);
    assert!(stats.estimate_at("small", now).is_none());
    drop(small);
    assert!(stats.estimate_at("small", now).is_none());

    let mut fast = stats.start_at("fast", now);
    fast.first_byte_at(Duration::from_millis(20), now);
    fast.record_at(100, Duration::from_millis(999), now);
    assert!(stats.estimate_at("fast", now).is_none());
    fast.record_at(0, Duration::from_millis(1), now);
    assert_rate(&stats, "fast", now, 50.0);
    drop(fast);
    assert_rate(&stats, "fast", now, 100.0);
  }

  #[test]
  fn rejects_invalid_rates_and_saturates_byte_totals() {
    assert!(!valid_rate(0.0));
    assert!(!valid_rate(-1.0));
    assert!(!valid_rate(f64::INFINITY));
    assert!(!valid_rate(f64::NAN));
    let invalid_stats = ThroughputStats::new(&NarTransferConfig {
      sample_min_bytes: 0,
      sample_min_duration: HumanDuration(Duration::ZERO),
      ..NarTransferConfig::default()
    });
    let now = Instant::now();
    let mut sample = invalid_stats.start_at("invalid", now);
    sample.first_byte_at(Duration::ZERO, now);
    sample.record_at(1, Duration::ZERO, now);
    assert!(invalid_stats.estimate_at("invalid", now).is_none());
    sample.record_at(0, Duration::from_secs(1), now);
    assert!(invalid_stats.estimate_at("invalid", now).is_none());

    let stats = stats();
    let mut sample = stats.start_at("large", now);
    sample.first_byte_at(Duration::ZERO, now);
    sample.record_at(u64::MAX, Duration::ZERO, now);
    sample.record_at(u64::MAX, Duration::from_secs(1), now);
    sample.release_at(now);
    assert_rate(&stats, "large", now, u64::MAX as f64);
  }

  #[test]
  fn estimates_require_fresh_first_byte_and_throughput() {
    let stats = stats();
    let now = Instant::now();
    let mut sample = stats.start_at("source", now);
    sample.record_at(100, Duration::from_secs(1), now);
    assert!(stats.estimate_at("source", now).is_none());
    sample.first_byte_at(Duration::from_millis(30), now);
    assert!(stats.estimate_at("source", now).is_some());
    let expiry = now + Duration::from_secs(10);
    assert!(stats.estimate_at("source", expiry).is_none());
    sample.first_byte_at(Duration::from_millis(40), expiry);
    assert!(stats.estimate_at("source", expiry).is_none());
    sample.record_at(400, Duration::from_secs(1), expiry);
    sample.release_at(expiry);
    assert_rate(&stats, "source", expiry, 400.0);
    assert!(
      stats
        .estimate_at("source", expiry + Duration::from_secs(10))
        .is_none()
    );

    let mut stale_latency = stats.start_at("stale-latency", now);
    stale_latency.first_byte_at(Duration::from_millis(10), now);
    stale_latency.record_at(100, Duration::from_secs(1), now);
    stale_latency.record_at(200, Duration::from_secs(1), expiry);
    assert!(stats.estimate_at("stale-latency", expiry).is_none());
    stale_latency.first_byte_at(Duration::from_millis(20), expiry);
    stale_latency.release_at(expiry);
    assert_eq!(
      stats
        .estimate_at("stale-latency", expiry)
        .map(|estimate| estimate.first_byte),
      Some(Duration::from_millis(20))
    );
  }

  #[test]
  fn small_requests_favor_latency_large_requests_favor_throughput() {
    let fast_start = Estimate {
      bytes_per_sec: 100.0,
      first_byte:    Duration::from_millis(10),
    };
    let fast_body = Estimate {
      bytes_per_sec: 1000.0,
      first_byte:    Duration::from_secs(1),
    };
    assert!(fast_start.completion_time(10) < fast_body.completion_time(10));
    assert!(
      fast_start.completion_time(10_000) > fast_body.completion_time(10_000)
    );
    assert_eq!(fast_start.completion_time(0), fast_start.first_byte);
    let very_slow = Estimate {
      bytes_per_sec: f64::MIN_POSITIVE,
      first_byte:    Duration::from_secs(1),
    };
    assert_eq!(very_slow.completion_time(u64::MAX), Duration::MAX);
  }

  #[test]
  fn windows_update_live_and_apply_ewma() {
    let stats = stats();
    let now = Instant::now();
    let mut sample = stats.start_at("source", now);
    sample.first_byte_at(Duration::from_millis(10), now);
    sample.record_at(100, Duration::from_secs(1), now);
    assert_rate(&stats, "source", now, 50.0);
    sample.record_at(500, Duration::from_secs(1), now);
    assert_rate(&stats, "source", now, 100.0);
    sample.record_at(99, Duration::from_secs(1), now);
    drop(sample);
    assert_rate(&stats, "source", now, 200.0);
  }

  #[test]
  fn dropping_cancelled_samples_releases_shared_active_counts() {
    let stats = stats();
    let now = Instant::now();
    measure(&stats, "source", 100, Duration::ZERO, now);
    let clone = stats.clone();
    let first = stats.start_at("source", now);
    let second = clone.start_at("source", now);
    assert_eq!(
      stats.entries().get("source").map(|entry| entry.active),
      Some(2)
    );
    assert_rate(&stats, "source", now, 100.0 / 3.0);
    drop(second);
    assert_eq!(
      stats.entries().get("source").map(|entry| entry.active),
      Some(1)
    );
    drop(first);
    assert_eq!(
      stats.entries().get("source").map(|entry| entry.active),
      Some(0)
    );
    assert_rate(&stats, "source", now, 100.0);
  }

  #[test]
  fn lower_concurrency_never_inflates_measured_rate() {
    let stats = stats();
    let now = Instant::now();
    let other = stats.start_at("source", now);
    let mut sample = stats.start_at("source", now);
    sample.first_byte_at(Duration::ZERO, now);
    sample.record_at(100, Duration::from_secs(1), now);
    assert_rate(&stats, "source", now, 200.0 / 3.0);
    drop(other);
    assert_rate(&stats, "source", now, 100.0);
    drop(sample);
    assert_rate(&stats, "source", now, 100.0);
  }

  #[test]
  fn concurrency_changes_use_the_conservative_window_minimum() {
    let stats = stats();
    let now = Instant::now();
    let mut sample = stats.start_at("source", now);
    sample.first_byte_at(Duration::ZERO, now);
    sample.record_at(50, Duration::from_millis(500), now);
    let other = stats.start_at("source", now);
    sample.record_at(50, Duration::from_millis(500), now);
    assert_rate(&stats, "source", now, 100.0 / 3.0);
    drop(other);
    drop(sample);
    assert_rate(&stats, "source", now, 100.0);
  }

  #[test]
  fn expiry_removes_only_inactive_registry_entries() {
    let stats = stats();
    let now = Instant::now();
    measure(&stats, "inactive", 100, Duration::ZERO, now);
    let active = stats.start_at("active", now);
    let later = now + Duration::from_secs(10);
    let newest = stats.start_at("newest", later);
    let entries = stats.entries();
    assert!(!entries.contains_key("inactive"));
    assert!(entries.contains_key("active"));
    assert!(entries.contains_key("newest"));
    drop(entries);
    drop(active);
    drop(newest);
  }

  #[test]
  fn registry_cap_never_evicts_live_state() {
    let stats = stats();
    let now = Instant::now();
    let mut samples = Vec::new();
    for index in 0..MAX_UPSTREAMS {
      samples.push(stats.start_at(&index.to_string(), now));
    }
    let mut overflow = stats.start_at("overflow", now);
    overflow.first_byte_at(Duration::ZERO, now);
    overflow.record_at(100, Duration::from_secs(1), now);
    assert!(overflow.upstream.is_none());
    assert_eq!(stats.entries().len(), MAX_UPSTREAMS);
    drop(samples.pop());
    let replacement = stats.start_at("replacement", now);
    assert!(replacement.upstream.is_some());
    let entries = stats.entries();
    assert_eq!(entries.len(), MAX_UPSTREAMS);
    assert!(entries.contains_key("0"));
    assert!(!entries.contains_key(&(MAX_UPSTREAMS - 1).to_string()[..]));
    drop(entries);
  }
}
