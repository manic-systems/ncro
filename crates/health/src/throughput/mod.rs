//! NAR throughput estimates and the upstream choice they drive.
//!
//! Nix decompresses while it downloads, so a transfer's rate only measures the
//! upstream when ncro spent most of it waiting on the upstream.

use std::time::{Duration, Instant};

use crate::UpstreamHealth;

/// Transfers smaller than this are dominated by time to first byte.
const MIN_SAMPLE_BYTES: u64 = 1 << 20;

/// Shorter tails after a reported interval mostly time already buffered chunks.
const MIN_TAIL: Duration = Duration::from_millis(100);

/// Reporting while streaming notices a slowdown before transfers finish.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Largest NAR worth sending to an upstream whose throughput is unknown.
const EXPLORE_MAX_BYTES: u64 = 16 << 20;

const STALE_AFTER: Duration = Duration::from_mins(10);

/// A sample's weight is `1 - e^(-elapsed/τ)` with this `τ`.
const DECAY_SECS: f64 = 5.0;

/// Upstream wait share above which the upstream set the pace.
const UPSTREAM_PACED_WAIT: f64 = 0.5;

/// Who set the pace of the transfers behind an estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pace {
  /// ncro mostly waited on the upstream, so the rate is the upstream's.
  Upstream,
  /// The client set the pace, so the upstream is at least this fast.
  Client,
}

#[derive(Debug, Clone, Copy)]
pub struct Throughput {
  /// Rate of one transfer.
  pub bytes_per_sec: f64,
  pub pace:          Pace,
  pub measured_at:   Instant,
}

/// A stretch of one proxied NAR body.
#[derive(Debug, Clone, Copy)]
pub struct TransferSample {
  pub bytes:         u64,
  pub elapsed:       Duration,
  /// Time spent waiting for the upstream to produce the next chunk.
  pub upstream_wait: Duration,
}

/// Splits one streaming NAR body into [`TransferSample`]s.
#[derive(Debug, Clone, Copy)]
pub struct Meter {
  interval_start: Instant,
  bytes:          u64,
  upstream_wait:  Duration,
  total:          u64,
}

impl Meter {
  /// Start metering at the first body chunk.
  #[must_use]
  pub const fn new(now: Instant) -> Self {
    Self {
      interval_start: now,
      bytes:          0,
      upstream_wait:  Duration::ZERO,
      total:          0,
    }
  }

  /// When the current interval is due to report.
  #[must_use]
  pub fn interval_end(&self) -> Instant {
    self.interval_start + SAMPLE_INTERVAL
  }

  /// Count `bytes` that took `upstream_wait`, returning the interval once due.
  pub fn record(
    &mut self,
    bytes: u64,
    upstream_wait: Duration,
    now: Instant,
  ) -> Option<TransferSample> {
    self.bytes += bytes;
    self.total += bytes;
    self.upstream_wait += upstream_wait;
    (now >= self.interval_end()).then(|| self.take(now))
  }

  /// The partial interval left when the body ends, if worth a sample.
  pub fn finish(&mut self, now: Instant) -> Option<TransferSample> {
    let tail = now.saturating_duration_since(self.interval_start);
    let whole = self.total == self.bytes;
    (self.total >= MIN_SAMPLE_BYTES
      && self.bytes > 0
      && (whole || tail >= MIN_TAIL))
      .then(|| self.take(now))
  }

  fn take(&mut self, now: Instant) -> TransferSample {
    let elapsed = now.saturating_duration_since(self.interval_start);
    let sample = TransferSample {
      bytes: self.bytes,
      elapsed,
      upstream_wait: self.upstream_wait.min(elapsed),
    };
    *self = Self {
      total: self.total,
      ..Self::new(now)
    };
    sample
  }
}

impl Throughput {
  fn is_fresh(&self, now: Instant) -> bool {
    now.saturating_duration_since(self.measured_at) < STALE_AFTER
  }

  /// Fold `sample` into `previous`. Client-paced samples only raise it.
  #[must_use]
  #[expect(
    clippy::cast_precision_loss,
    reason = "NAR sizes are far below 2^52 bytes"
  )]
  pub fn update(
    previous: Option<Self>,
    sample: TransferSample,
    now: Instant,
  ) -> Option<Self> {
    if sample.elapsed.is_zero() {
      return previous;
    }
    let elapsed = sample.elapsed.as_secs_f64();
    let rate = sample.bytes as f64 / elapsed;
    let pace =
      if sample.upstream_wait.as_secs_f64() >= UPSTREAM_PACED_WAIT * elapsed {
        Pace::Upstream
      } else {
        Pace::Client
      };
    let fresh = previous.filter(|t| t.is_fresh(now));
    Some(match (fresh, pace) {
      (Some(t), Pace::Upstream) if t.pace == Pace::Upstream => {
        let weight = -(-elapsed / DECAY_SECS).exp_m1();
        Self {
          bytes_per_sec: weight
            .mul_add(rate - t.bytes_per_sec, t.bytes_per_sec),
          pace,
          measured_at: now,
        }
      },
      (Some(t), Pace::Client) => {
        Self {
          bytes_per_sec: t.bytes_per_sec.max(rate),
          measured_at: now,
          ..t
        }
      },
      (_, pace) => {
        Self {
          bytes_per_sec: rate,
          pace,
          measured_at: now,
        }
      },
    })
  }
}

/// The candidate expected to deliver `bytes` first, exploring on mid-sized NARs.
#[must_use]
#[expect(
  clippy::cast_precision_loss,
  reason = "NAR sizes are far below 2^52 bytes"
)]
pub fn pick_for_transfer(
  candidates: &[UpstreamHealth],
  bytes: u64,
  now: Instant,
) -> Option<&UpstreamHealth> {
  let fresh =
    |health: &UpstreamHealth| health.throughput.filter(|t| t.is_fresh(now));
  if (MIN_SAMPLE_BYTES..=EXPLORE_MAX_BYTES).contains(&bytes)
    && let Some(unmeasured) = candidates
      .iter()
      .filter(|health| {
        health.inflight == 0
          && fresh(health).is_none_or(|t| t.pace == Pace::Client)
      })
      .min_by(|a, b| a.ema_latency.total_cmp(&b.ema_latency))
  {
    return Some(unmeasured);
  }
  candidates
    .iter()
    .filter_map(|health| {
      let rate = fresh(health)?.bytes_per_sec;
      Some((health, health.ema_latency / 1000.0 + bytes as f64 / rate))
    })
    .min_by(|a, b| a.1.total_cmp(&b.1))
    .map(|(health, _)| health)
}

#[cfg(test)] mod sim;
