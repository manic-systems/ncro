#![expect(
  clippy::cast_precision_loss,
  reason = "Completion forecasts intentionally use approximate floating-point \
            byte rates."
)]

use std::{
  collections::{HashSet, VecDeque},
  io,
  mem::take,
  sync::Arc,
  time::{Duration, Instant},
};

use axum::{
  body::{Body, BodyDataStream},
  http::{HeaderMap, HeaderValue, Method, StatusCode},
  response::Response,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use ncro_narinfo::NarInfo;
use tokio::time::{Instant as TokioInstant, timeout};

use super::{
  AppState,
  NarCandidate,
  hedge_candidates,
  open_nar_candidate,
  rank_nar_candidates,
  throughput::TransferSample,
};

pub struct Context {
  pub expected:   Option<NarInfo>,
  pub store_hash: Option<String>,
  pub path:       String,
}

/// Account only for actively awaited upstream reads, never time between client
/// polls.
pub fn observe(
  response: Response,
  sample: TransferSample,
  first_byte: Duration,
) -> Response {
  sample.first_byte(first_byte);
  let (parts, body) = response.into_parts();
  let body = stream::unfold(
    (body.into_data_stream(), sample, true),
    |(mut body, mut sample, first)| {
      async move {
        let started = Instant::now();
        let next = body.next().await?;
        // The first chunk was already read by the first-byte race; setup is not
        // bandwidth.
        if !first {
          sample.record(
            next.as_ref().map_or(0, |bytes| bytes.len() as u64),
            started.elapsed(),
          );
        }
        Some((next, (body, sample, false)))
      }
    },
  );
  Response::from_parts(parts, Body::from_stream(body))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Span {
  start: u64,
  end:   u64,
  total: u64,
}

impl Span {
  const fn length(self) -> u64 {
    self.end - self.start + 1
  }

  fn from_response(
    response: &Response,
    request: &HeaderMap,
    expected: &NarInfo,
  ) -> Option<Self> {
    // FileHash identifies the compressed representation, unlike the signed
    // NarHash.
    if expected.file_size == 0
      || !expected
        .file_hash
        .split_once(':')
        .is_some_and(|(algorithm, hash)| {
          !algorithm.is_empty() && !hash.is_empty()
        })
    {
      return None;
    }
    let headers = response.headers();
    if headers
      .get("content-encoding")
      .is_some_and(|value| value != "identity")
    {
      return None;
    }
    let span = match response.status() {
      StatusCode::OK if !request.contains_key("range") => {
        Self {
          start: 0,
          end:   expected.file_size - 1,
          total: expected.file_size,
        }
      },
      StatusCode::PARTIAL_CONTENT => {
        let actual =
          parse_content_range(headers.get("content-range")?.to_str().ok()?)?;
        let requested = requested_span(
          request.get("range")?.to_str().ok()?,
          expected.file_size,
        )?;
        if actual != requested {
          return None;
        }
        actual
      },
      _ => return None,
    };
    if span.total != expected.file_size {
      return None;
    }
    if let Some(length) = headers.get("content-length")
      && length.to_str().ok()?.parse::<u64>().ok()? != span.length()
    {
      return None;
    }
    Some(span)
  }

  fn validates(self, response: &Response, cursor: u64) -> bool {
    if response.status() != StatusCode::PARTIAL_CONTENT
      || response
        .headers()
        .get("content-encoding")
        .is_some_and(|value| value != "identity")
    {
      return false;
    }
    let actual = response
      .headers()
      .get("content-range")
      .and_then(|value| value.to_str().ok())
      .and_then(parse_content_range);
    if actual
      != Some(Self {
        start: cursor,
        end:   self.end,
        total: self.total,
      })
    {
      return false;
    }
    response
      .headers()
      .get("content-length")
      .is_none_or(|length| {
        length
          .to_str()
          .ok()
          .and_then(|value| value.parse::<u64>().ok())
          == Some(self.end - cursor + 1)
      })
  }
}

fn parse_content_range(value: &str) -> Option<Span> {
  let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
  let (start, end) = range.split_once('-')?;
  let span = Span {
    start: start.parse().ok()?,
    end:   end.parse().ok()?,
    total: total.parse().ok()?,
  };
  (span.start <= span.end && span.end < span.total).then_some(span)
}

pub fn requested_length(headers: &HeaderMap, total: u64) -> u64 {
  headers
    .get("range")
    .and_then(|value| value.to_str().ok())
    .and_then(|value| requested_span(value, total))
    .map_or(total, Span::length)
}

fn requested_span(value: &str, total: u64) -> Option<Span> {
  let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
  let last = total.checked_sub(1)?;
  let span = if start.is_empty() {
    let suffix = end.parse::<u64>().ok()?;
    if suffix == 0 {
      return None;
    }
    Span {
      start: total.saturating_sub(suffix),
      end: last,
      total,
    }
  } else {
    Span {
      start: start.parse().ok()?,
      end: if end.is_empty() {
        last
      } else {
        end.parse::<u64>().ok()?.min(last)
      },
      total,
    }
  };
  (span.start <= span.end && span.end < total).then_some(span)
}

struct Source {
  body:     BodyDataStream,
  buffered: VecDeque<Bytes>,
  first:    bool,
}

impl Source {
  fn new(body: Body) -> Self {
    Self {
      body:     body.into_data_stream(),
      buffered: VecDeque::new(),
      first:    true,
    }
  }

  async fn next(&mut self) -> Result<Option<(Bytes, Duration)>, io::Error> {
    if let Some(bytes) = self.buffered.pop_front() {
      return Ok(Some((bytes, Duration::ZERO)));
    }
    let started = Instant::now();
    loop {
      match self.body.next().await {
        Some(Ok(bytes)) if bytes.is_empty() => {},
        Some(Ok(bytes)) => {
          let active = if take(&mut self.first) {
            Duration::ZERO
          } else {
            started.elapsed()
          };
          return Ok(Some((bytes, active)));
        },
        Some(Err(error)) => return Err(io::Error::other(error)),
        None => return Ok(None),
      }
    }
  }
}

struct Transfer {
  state:         Arc<AppState>,
  context:       Context,
  headers:       HeaderMap,
  source:        Source,
  candidate:     NarCandidate,
  span:          Option<Span>,
  emitted:       u64,
  attempts:      u32,
  discoveries:   u32,
  tried:         HashSet<String>,
  window_bytes:  u64,
  window_active: Duration,
  done:          bool,
}

pub fn supervise(
  state: Arc<AppState>,
  candidate: NarCandidate,
  headers: HeaderMap,
  context: Context,
  response: Response,
) -> Response {
  let span = context
    .expected
    .as_ref()
    .and_then(|expected| Span::from_response(&response, &headers, expected));
  let (parts, body) = response.into_parts();
  let mut tried = HashSet::new();
  tried.insert(candidate.upstream.clone());
  let transfer = Transfer {
    state,
    context,
    headers,
    source: Source::new(body),
    candidate,
    span,
    emitted: 0,
    attempts: 0,
    discoveries: 0,
    tried,
    window_bytes: 0,
    window_active: Duration::ZERO,
    done: false,
  };
  let body = stream::unfold(transfer, |mut transfer| {
    async move {
      if transfer.done {
        return None;
      }
      match transfer.next().await {
        Ok(Some(bytes)) => Some((Ok(bytes), transfer)),
        Ok(None) => None,
        Err(error) => {
          transfer.done = true;
          Some((Err(error), transfer))
        },
      }
    }
  });
  Response::from_parts(parts, Body::from_stream(body))
}

impl Transfer {
  fn can_switch(&self) -> bool {
    self.span.is_some()
      && self.context.store_hash.is_some()
      && self.attempts < self.state.nar_transfer.max_switches
      && self.discoveries < self.state.nar_transfer.max_switches
  }

  async fn next(&mut self) -> io::Result<Option<Bytes>> {
    loop {
      if self.span.is_some_and(|span| self.emitted == span.length()) {
        return Ok(None);
      }
      self.maybe_replace_slow().await;
      let next =
        timeout(self.state.nar_transfer.stall_timeout.0, self.source.next())
          .await;
      match next {
        Ok(Ok(Some((bytes, active)))) => {
          if self.span.is_some_and(|span| {
            bytes.len() as u64 > span.length() - self.emitted
          }) {
            return Err(io::Error::new(
              io::ErrorKind::InvalidData,
              "NAR body exceeded expected range",
            ));
          }
          self.emitted = self.emitted.saturating_add(bytes.len() as u64);
          // Buffered probe bytes are not a new bandwidth sample.
          if !active.is_zero() {
            self.window_bytes =
              self.window_bytes.saturating_add(bytes.len() as u64);
            self.window_active = self.window_active.saturating_add(active);
          }
          // Return the bytes already read before probing: the replacement
          // starts at the next emitted offset, never before or after
          // this chunk.
          return Ok(Some(bytes));
        },
        Ok(Ok(None)) if self.span.is_none() => return Ok(None),
        _ => {
          if !self.can_switch() || !self.replace(None).await {
            return Err(io::Error::new(
              io::ErrorKind::UnexpectedEof,
              "NAR stream stalled or ended before completion; no safe \
               replacement",
            ));
          }
        },
      }
    }
  }

  async fn maybe_replace_slow(&mut self) {
    let config = &self.state.nar_transfer;
    if !self.can_switch()
      || self.window_active < config.slow_window.0
      || self.window_bytes == 0
    {
      return;
    }
    let rate = self.window_bytes as f64 / self.window_active.as_secs_f64();
    self.window_bytes = 0;
    self.window_active = Duration::ZERO;
    if rate.is_finite() && rate > 0.0 {
      self.replace(Some(rate)).await;
    }
  }

  async fn replace(&mut self, current_rate: Option<f64>) -> bool {
    let Some(span) = self.span else {
      return false;
    };
    let cursor = span.start + self.emitted;
    if cursor > span.end {
      return false;
    }
    let remaining = span.end - cursor + 1;
    let started = Instant::now();
    self.discoveries += 1;
    let mut candidates = hedge_candidates(
      &self.state,
      self.context.store_hash.as_deref(),
      &self.context.path,
      &self.candidate.upstream,
      false,
      self.context.expected.as_ref(),
      Some(TokioInstant::now() + self.state.nar_transfer.probe_timeout.0),
    )
    .await;
    candidates.retain(|candidate| !self.tried.contains(&candidate.upstream));
    rank_nar_candidates(&self.state, &mut candidates, remaining);
    let mut headers = self.headers.clone();
    let Ok(range) =
      HeaderValue::from_str(&format!("bytes={cursor}-{}", span.end))
    else {
      return false;
    };
    headers.insert("range", range);
    headers.insert("accept-encoding", HeaderValue::from_static("identity"));
    for candidate in candidates {
      if self.attempts >= self.state.nar_transfer.max_switches {
        break;
      }
      if let Some(current_rate) = current_rate
        && let Some(estimate) =
          self.state.throughput.estimate(&candidate.upstream)
      {
        let old = remaining as f64 / current_rate;
        let new = estimate.completion_time(remaining).as_secs_f64()
          + started.elapsed().as_secs_f64();
        if !worth_switching(old, new, &self.state) {
          continue;
        }
      }
      self.attempts += 1;
      self.tried.insert(candidate.upstream.clone());
      let attempt = async {
        let response =
          open_nar_candidate(&self.state, Method::GET, &headers, &candidate)
            .await?;
        if !span.validates(&response, cursor) {
          return None;
        }
        let mut source = Source::new(response.into_body());
        if current_rate.is_some() {
          let target = self.state.nar_transfer.probe_bytes.min(remaining);
          let mut prefix = VecDeque::new();
          let mut buffered = 0_u64;
          while buffered < target {
            if prefix.len() >= 256 {
              return None;
            }
            let (mut bytes, _) = source.next().await.ok()??;
            let take =
              usize::try_from((target - buffered).min(bytes.len() as u64))
                .ok()?;
            let first = bytes.split_to(take);
            buffered += first.len() as u64;
            prefix.push_back(first);
            if !bytes.is_empty() {
              source.buffered.push_back(bytes);
            }
          }
          prefix.append(&mut source.buffered);
          source.buffered = prefix;
          let elapsed = started.elapsed().as_secs_f64();
          let rate = buffered as f64 / elapsed.max(f64::EPSILON);
          let old = remaining as f64 / current_rate?;
          let new = elapsed + (remaining - buffered) as f64 / rate;
          if !worth_switching(old, new, &self.state) {
            return None;
          }
        }
        Some(source)
      };
      if let Ok(Some(source)) =
        timeout(self.state.nar_transfer.probe_timeout.0, attempt).await
      {
        tracing::info!(
          from = self.candidate.upstream,
          to = candidate.upstream,
          cursor,
          slow = current_rate.is_some(),
          "NAR stream switched source"
        );
        self.source = source;
        self.candidate = candidate;
        self.window_bytes = 0;
        self.window_active = Duration::ZERO;
        return true;
      }
    }
    false
  }
}

fn worth_switching(old: f64, new: f64, state: &AppState) -> bool {
  old.is_finite()
    && new.is_finite()
    && old > 0.0
    && old - new >= state.nar_transfer.switch_margin.0.as_secs_f64()
    && new <= old * (1.0 - state.nar_transfer.min_improvement)
}

#[cfg(test)]
mod tests {
  use super::{Span, parse_content_range, requested_span};

  #[test]
  fn range_parser_rejects_unknown_totals_reversed_and_out_of_bounds() {
    for value in [
      "bytes 4-3/10",
      "bytes 0-10/10",
      "bytes 0-9/*",
      "bytes */10",
      "bytes 0-9/0",
      "bytes 0-9/10,20-29/30",
    ] {
      assert_eq!(parse_content_range(value), None, "{value}");
    }
    assert_eq!(
      parse_content_range("bytes 4-9/10"),
      Some(Span {
        start: 4,
        end:   9,
        total: 10,
      })
    );
  }

  #[test]
  fn request_ranges_preserve_absolute_offsets_and_clamp_suffixes() {
    assert_eq!(
      requested_span("bytes=4-7", 10),
      Some(Span {
        start: 4,
        end:   7,
        total: 10,
      })
    );
    assert_eq!(
      requested_span("bytes=4-", 10),
      Some(Span {
        start: 4,
        end:   9,
        total: 10,
      })
    );
    assert_eq!(
      requested_span("bytes=-30", 10),
      Some(Span {
        start: 0,
        end:   9,
        total: 10,
      })
    );
    for value in ["bytes=10-", "bytes=8-4", "bytes=-0", "bytes=0-2,4-6"] {
      assert_eq!(requested_span(value, 10), None, "{value}");
    }
  }
}
