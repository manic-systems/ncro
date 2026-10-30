//! Deterministic simulation of NAR routing on a virtual clock, with workers
//! standing in for Nix's `http-connections`, through the real [`Meter`],
//! [`Throughput::update`] and [`pick_for_transfer`].
#![expect(
  clippy::cast_precision_loss,
  clippy::cast_possible_truncation,
  clippy::cast_sign_loss,
  reason = "simulated sizes and rates are small positive numbers"
)]

use std::time::{Duration, Instant};

use super::{Meter, Throughput, pick_for_transfer};
use crate::UpstreamHealth;

const MB: f64 = 1_000_000.0;
const REQUESTS: usize = 2_000;
const SEEDS: u64 = 8;

/// Nix decompressing xz, and zstd, which it drains faster than any upstream.
const XZ_AND_ZSTD: &[f64] = &[6.0 * MB, 400.0 * MB];

/// xorshift64*, so a seed always replays the same workload.
struct Rng(u64);

impl Rng {
  fn new(seed: u64) -> Self {
    Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
  }

  fn unit(&mut self) -> f64 {
    self.0 ^= self.0 >> 12;
    self.0 ^= self.0 << 25;
    self.0 ^= self.0 >> 27;
    (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64
      / (1_u64 << 53) as f64
  }

  /// Log-uniform between 4 KiB and 512 MiB, roughly the spread of a closure.
  fn nar_size(&mut self) -> u64 {
    let (lo, hi) = (4096_f64.ln(), (512.0 * 1024.0 * 1024.0_f64).ln());
    self.unit().mul_add(hi - lo, lo).exp() as u64
  }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Sharing {
  /// Concurrent transfers split the upstream's rate.
  Aggregate,
  /// Every transfer gets the upstream's rate, as with a per-connection limit.
  PerConnection,
}

struct SimUpstream {
  url:        &'static str,
  latency_ms: f64,
  /// Bytes per second once `started` NARs have been dispatched.
  rate:       fn(usize) -> f64,
}

struct Scenario {
  upstreams:    Vec<SimUpstream>,
  sharing:      Sharing,
  workers:      usize,
  client_rates: &'static [f64],
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Policy {
  Throughput,
  /// Greedy on the true rates and current load.
  TrueRates,
}

struct Active {
  upstream:    usize,
  remaining:   f64,
  client_rate: f64,
  ttfb_left:   f64,
  /// Set once the first byte arrives.
  meter:       Option<Meter>,
}

impl Scenario {
  fn share(&self, upstream: usize, started: usize, inflight: u32) -> f64 {
    let rate = (self.upstreams[upstream].rate)(started);
    match self.sharing {
      Sharing::Aggregate => rate / f64::from(inflight.max(1)),
      Sharing::PerConnection => rate,
    }
  }

  /// Rate of each active transfer, and whether the upstream limited it.
  fn rates(&self, active: &[Active], started: usize) -> Vec<(f64, bool)> {
    let mut out = vec![(0.0, false); active.len()];
    for upstream in 0..self.upstreams.len() {
      let mut streaming = (0..active.len())
        .filter(|&i| {
          active[i].upstream == upstream && active[i].meter.is_some()
        })
        .collect::<Vec<_>>();
      let rate = (self.upstreams[upstream].rate)(started);
      if self.sharing == Sharing::PerConnection {
        for i in streaming {
          let cap = active[i].client_rate;
          out[i] = (rate.min(cap), rate < cap);
        }
        continue;
      }
      streaming.sort_by(|&a, &b| {
        active[a].client_rate.total_cmp(&active[b].client_rate)
      });
      let mut left = rate;
      let count = streaming.len();
      for (served, i) in streaming.into_iter().enumerate() {
        let fair = left / (count - served) as f64;
        let cap = active[i].client_rate;
        out[i] = (fair.min(cap), fair < cap);
        left -= fair.min(cap);
      }
    }
    out
  }

  fn simulate(&self, policy: Policy, seed: u64) -> Duration {
    let mut rng = Rng::new(seed);
    let start = Instant::now();
    let mut clock = 0.0_f64;
    let at = |secs: f64| start + Duration::from_secs_f64(secs);
    let mut inflight = vec![0_u32; self.upstreams.len()];
    let mut healths = self
      .upstreams
      .iter()
      .map(|upstream| {
        let mut health = UpstreamHealth::new(upstream.url.to_string(), 1);
        health.ema_latency = upstream.latency_ms;
        health
      })
      .collect::<Vec<_>>();
    let by_latency = (0..self.upstreams.len())
      .min_by(|&a, &b| {
        self.upstreams[a]
          .latency_ms
          .total_cmp(&self.upstreams[b].latency_ms)
      })
      .unwrap_or_default();
    let mut active = Vec::<Active>::new();
    let mut started = 0;

    loop {
      while active.len() < self.workers && started < REQUESTS {
        let bytes = rng.nar_size();
        let client_rate = self.client_rates
          [(rng.unit() * self.client_rates.len() as f64) as usize];
        let now = at(clock);
        for (health, &count) in healths.iter_mut().zip(&inflight) {
          health.inflight = count;
        }
        let choice = match policy {
          Policy::Throughput => {
            pick_for_transfer(&healths, bytes, now)
              .and_then(|picked| {
                healths.iter().position(|h| h.url == picked.url)
              })
              .unwrap_or(by_latency)
          },
          Policy::TrueRates => {
            (0..self.upstreams.len())
              .min_by(|&a, &b| {
                let cost = |i: usize| {
                  let rate =
                    self.share(i, started, inflight[i] + 1).min(client_rate);
                  self.upstreams[i].latency_ms / 1000.0 + bytes as f64 / rate
                };
                cost(a).total_cmp(&cost(b))
              })
              .unwrap_or_default()
          },
        };
        inflight[choice] += 1;
        active.push(Active {
          upstream: choice,
          remaining: bytes as f64,
          client_rate,
          ttfb_left: self.upstreams[choice].latency_ms / 1000.0,
          meter: None,
        });
        started += 1;
      }
      if active.is_empty() {
        break;
      }

      let rates = self.rates(&active, started);
      let step = active
        .iter()
        .zip(&rates)
        .map(|(transfer, &(rate, _))| {
          let Some(meter) = &transfer.meter else {
            return transfer.ttfb_left;
          };
          let report = (meter.interval_end() - start).as_secs_f64() - clock;
          (transfer.remaining / rate).min(report)
        })
        .fold(f64::INFINITY, f64::min)
        .max(1e-9);
      clock += step;
      let now = at(clock);

      let mut index = 0;
      while index < active.len() {
        let (rate, upstream_limited) = rates[index];
        let transfer = &mut active[index];
        let Some(meter) = &mut transfer.meter else {
          transfer.ttfb_left -= step;
          if transfer.ttfb_left <= 1e-9 {
            transfer.meter = Some(Meter::new(now));
          }
          index += 1;
          continue;
        };
        let moved = (rate * step).min(transfer.remaining);
        transfer.remaining -= moved;
        let waited = if upstream_limited { 0.9 } else { 0.2 } * step;
        let done = transfer.remaining <= 0.5;
        let sample = meter
          .record(moved as u64, Duration::from_secs_f64(waited), now)
          .or_else(|| done.then(|| meter.finish(now)).flatten());
        let upstream = transfer.upstream;
        if let Some(sample) = sample {
          let health = &mut healths[upstream];
          health.throughput =
            Throughput::update(health.throughput, sample, now);
        }
        if done {
          inflight[upstream] -= 1;
          active.swap_remove(index);
        } else {
          index += 1;
        }
      }
    }
    Duration::from_secs_f64(clock)
  }
}

/// With `slow_spell`, `far-wide` drops to 2 MB/s for a third of the run.
fn upstreams(slow_spell: bool) -> Vec<SimUpstream> {
  let far_rate: fn(usize) -> f64 = if slow_spell {
    |started| {
      if (700..1_400).contains(&started) {
        2.0 * MB
      } else {
        80.0 * MB
      }
    }
  } else {
    |_| 80.0 * MB
  };
  vec![
    SimUpstream {
      url:        "near-thin",
      latency_ms: 30.0,
      rate:       |_| 4.0 * MB,
    },
    SimUpstream {
      url:        "far-wide",
      latency_ms: 120.0,
      rate:       far_rate,
    },
    SimUpstream {
      url:        "middle",
      latency_ms: 60.0,
      rate:       |_| 20.0 * MB,
    },
  ]
}

#[test]
fn routing_stays_near_true_rate_routing() {
  let cases = [
    (false, Sharing::Aggregate, 1, 1.05),
    (false, Sharing::Aggregate, 4, 1.10),
    (false, Sharing::Aggregate, 25, 1.07),
    (false, Sharing::PerConnection, 1, 1.05),
    (false, Sharing::PerConnection, 4, 1.07),
    (false, Sharing::PerConnection, 25, 1.15),
    (true, Sharing::Aggregate, 1, 1.08),
    (true, Sharing::Aggregate, 4, 1.25),
    (true, Sharing::Aggregate, 25, 1.07),
    (true, Sharing::PerConnection, 1, 1.08),
    (true, Sharing::PerConnection, 4, 1.14),
    (true, Sharing::PerConnection, 25, 1.33),
  ];
  for (slow_spell, sharing, workers, bound) in cases {
    let scenario = Scenario {
      upstreams: upstreams(slow_spell),
      sharing,
      workers,
      client_rates: XZ_AND_ZSTD,
    };
    for seed in 0..SEEDS {
      let ratio = scenario.simulate(Policy::Throughput, seed).as_secs_f64()
        / scenario.simulate(Policy::TrueRates, seed).as_secs_f64();
      assert!(
        ratio < bound,
        "slow spell {slow_spell}, {workers} workers, seed {seed}: {ratio:.3} \
         times true-rate routing"
      );
    }
  }
}
