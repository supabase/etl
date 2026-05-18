use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub const MAX_LOG_LINES: usize = 2000;

pub struct TuiLogLayer {
    buffer: Arc<Mutex<VecDeque<String>>>,
}

struct FieldVisitor {
    message: String,
    fields: Vec<(String, String)>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self { message: String::new(), fields: Vec::new() }
    }

    fn into_line(self) -> String {
        if self.fields.is_empty() {
            self.message
        } else {
            let extras: Vec<String> =
                self.fields.into_iter().map(|(k, v)| format!("{k}={v}")).collect();
            format!("{} {}", self.message, extras.join(" "))
        }
    }
}

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields.push((field.name().to_owned(), format!("{value:?}")));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            self.fields.push((field.name().to_owned(), value.to_owned()));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for TuiLogLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let level = event.metadata().level();
        let target = event.metadata().target();
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);
        let line = format!("[{level}] {target}: {}", visitor.into_line());
        let mut buf = self.buffer.lock().unwrap();
        buf.push_back(line);
        if buf.len() > MAX_LOG_LINES {
            buf.pop_front();
        }
    }
}

pub fn init_tracing(log_buffer: Arc<Mutex<VecDeque<String>>>) {
    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
    }

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "snowflake=info".into()),
        )
        .with(TuiLogLayer { buffer: log_buffer })
        .init();
}
