use std::{
  fmt::Write as _,
  sync::{Arc, Mutex},
};

use tracing::{
  Level,
  field::{Field, Visit},
};
use tracing_subscriber::layer::{Context, Layer};

/// Buffers warnings emitted before the real logging subscriber is installed.
///
/// Logging is configured from the parsed config, so warnings emitted during
/// `Config::load` happen before any subscriber exists. Errors abort startup via
/// config validation, so only warnings need to survive this window. Installing
/// this as a scoped default during config loading captures them;
/// [`BufferLayer::replay`] re-emits them through the real subscriber once it is
/// configured.
#[derive(Clone, Default)]
pub struct BufferLayer {
  warnings: Arc<Mutex<Vec<String>>>,
}

impl BufferLayer {
  /// Re-emit every buffered warning through the current subscriber, then clear
  /// the buffer.
  pub fn replay(&self) {
    if let Ok(mut warnings) = self.warnings.lock() {
      for message in warnings.drain(..) {
        tracing::warn!("{message}");
      }
    }
  }
}

impl<S: tracing::Subscriber> Layer<S> for BufferLayer {
  fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
    if *event.metadata().level() != Level::WARN {
      return;
    }

    let mut visitor = MessageVisitor(String::new());
    event.record(&mut visitor);

    if let Ok(mut warnings) = self.warnings.lock() {
      warnings.push(visitor.0);
    }
  }
}

/// Extracts the `message` field of an event into a plain string.
struct MessageVisitor(String);

impl Visit for MessageVisitor {
  fn record_debug(&mut self, field: &Field, value: &dyn core::fmt::Debug) {
    if field.name() == "message" {
      let _ = write!(self.0, "{value:?}");
    }
  }
}
