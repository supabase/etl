use etl_config::Environment;
use serde::ser::{SerializeMap, Serializer as _};
use std::io;
use std::io::Error;
use std::sync::OnceLock;
use std::{
    backtrace::{Backtrace, BacktraceStatus},
    fmt as stdfmt,
    panic::PanicHookInfo,
    sync::Once,
};
use thiserror::Error;
use tracing::Event;
use tracing::subscriber::{SetGlobalDefaultError, set_global_default};
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{self, InitError},
};
use tracing_log::NormalizeEvent;
use tracing_log::{LogTracer, log_tracer::SetLoggerError};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt as tracing_fmt;
use tracing_subscriber::fmt::{
    FmtContext, FormattedFields, MakeWriter, fmt,
    format::{self, FormatEvent, FormatFields, Writer},
    time::FormatTime,
};
use tracing_subscriber::registry::{LookupSpan, SpanRef};

/// JSON field name for project identification in logs.
const PROJECT_KEY_IN_LOG: &str = "project";
/// JSON field name for pipeline identification in logs.
const PIPELINE_KEY_IN_LOG: &str = "pipeline_id";

/// Global item to make sure that test tracing is initialized only once.
static INIT_TEST_TRACING: Once = Once::new();
/// Global project reference storage
static PROJECT_REF: OnceLock<String> = OnceLock::new();
/// Global pipeline id storage.
static PIPELINE_ID: OnceLock<u64> = OnceLock::new();

/// Errors that can occur during tracing initialization.
#[derive(Debug, Error)]
pub enum TracingError {
    #[error("failed to build rolling file appender: {0}")]
    InitAppender(#[from] InitError),

    #[error("failed to init log tracer: {0}")]
    InitLogTracer(#[from] SetLoggerError),

    #[error("failed to set global default subscriber: {0}")]
    SetGlobalDefault(#[from] SetGlobalDefaultError),

    #[error("an io error occurred: {0}")]
    Io(#[from] Error),
}

/// Log flusher handle for ensuring logs are written before shutdown.
///
/// Production mode returns a [`WorkerGuard`] that must be kept alive to ensure
/// logs are flushed. Development mode doesn't require flushing.
#[must_use]
pub enum LogFlusher {
    /// Production flusher that ensures logs are written to files.
    Flusher(WorkerGuard),
    /// Development flusher that doesn't require explicit flushing.
    NullFlusher,
}

/// Initializes tracing for test environments.
///
/// Call once at the beginning of tests. Set `ENABLE_TRACING=1` to view tracing output:
/// ```bash
/// ENABLE_TRACING=1 cargo test test_name
/// ```
pub fn init_test_tracing() {
    INIT_TEST_TRACING.call_once(|| {
        if std::env::var("ENABLE_TRACING").is_ok() {
            // Needed because if no env is set, it defaults to prod, which logs to files instead of terminal,
            // and we need to log to terminal when `ENABLE_TRACING` env var is set.
            Environment::Dev.set();
            let _log_flusher =
                init_tracing("test").expect("Failed to initialize tracing for tests");
        }
    });
}

/// Sets the global project reference for all tracing events.
///
/// The project reference will be injected into all structured log entries
/// for identification and filtering purposes.
pub fn set_global_project_ref(project_ref: &str) {
    let _ = PROJECT_REF.set(project_ref.into());
}

/// Returns the current global project reference.
///
/// Returns `None` if no project reference has been set.
pub fn get_global_project_ref() -> Option<&'static str> {
    PROJECT_REF.get().map(|s| s.as_str())
}

/// Sets the global pipeline id for all tracing events.
///
/// The pipeline id will be injected into all structured log entries
/// as a top-level field named "pipeline_id" for identification and filtering.
pub fn set_global_pipeline_id(pipeline_id: u64) {
    let _ = PIPELINE_ID.set(pipeline_id);
}

/// Returns the current global pipeline id.
///
/// Returns `None` if no pipeline id has been set.
pub fn get_global_pipeline_id() -> Option<u64> {
    PIPELINE_ID.get().copied()
}

/// Event formatter that emits global context directly into JSON log entries.
struct JsonContextFormatter<T> {
    timer: T,
}

impl<T> JsonContextFormatter<T> {
    /// Creates a new JSON formatter with context injection.
    fn new(timer: T) -> Self {
        Self { timer }
    }
}

impl<S, N, T> FormatEvent<S, N> for JsonContextFormatter<T>
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
    T: FormatTime,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> stdfmt::Result {
        let mut timestamp = String::new();
        self.timer.format_time(&mut Writer::new(&mut timestamp))?;

        let normalized_meta = event.normalized_metadata();
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());

        let spans = ctx.event_scope().map(|scope| {
            scope
                .from_root()
                .map(|span| span_to_json_value::<S, N>(&span))
                .collect::<Vec<_>>()
        });

        let mut output = Vec::new();
        let mut serializer = serde_json::Serializer::new(&mut output);
        let mut map = serializer.serialize_map(None).map_err(|_| stdfmt::Error)?;

        map.serialize_entry("timestamp", &timestamp)
            .map_err(|_| stdfmt::Error)?;
        map.serialize_entry("level", &meta.level().as_str())
            .map_err(|_| stdfmt::Error)?;

        if let Some(project_ref) = get_global_project_ref() {
            map.serialize_entry(PROJECT_KEY_IN_LOG, project_ref)
                .map_err(|_| stdfmt::Error)?;
        }

        if let Some(pipeline_id) = get_global_pipeline_id() {
            map.serialize_entry(PIPELINE_KEY_IN_LOG, &pipeline_id)
                .map_err(|_| stdfmt::Error)?;
        }

        map.serialize_entry("fields", &SerializableEvent(event))
            .map_err(|_| stdfmt::Error)?;

        if let Some(span) = spans.as_ref().and_then(|spans| spans.last()) {
            map.serialize_entry("span", span)
                .map_err(|_| stdfmt::Error)?;
        }

        if let Some(spans) = spans.as_ref() {
            map.serialize_entry("spans", spans)
                .map_err(|_| stdfmt::Error)?;
        }

        map.end().map_err(|_| stdfmt::Error)?;

        let output = String::from_utf8(output).map_err(|_| stdfmt::Error)?;
        writeln!(writer, "{output}")
    }
}

/// Serializable event field map.
struct SerializableEvent<'a>(&'a Event<'a>);

impl serde::Serialize for SerializableEvent<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut visitor = JsonFieldVisitor::default();
        self.0.record(&mut visitor);
        visitor.values.serialize(serializer)
    }
}

/// Converts a span reference into the JSON shape used by the JSON formatter.
fn span_to_json_value<S, N>(span: &SpanRef<'_, S>) -> serde_json::Value
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    let mut map = serde_json::Map::new();

    let ext = span.extensions();
    let data = ext
        .get::<FormattedFields<N>>()
        .expect("formatted span fields must exist");

    if let Ok(serde_json::Value::Object(fields)) = serde_json::from_str::<serde_json::Value>(data) {
        map.extend(fields);
    }

    map.insert(
        "name".to_string(),
        serde_json::Value::String(span.metadata().name().to_string()),
    );

    serde_json::Value::Object(map)
}

/// JSON visitor for event fields.
#[derive(Default)]
struct JsonFieldVisitor {
    values: serde_json::Map<String, serde_json::Value>,
}

impl tracing::field::Visit for JsonFieldVisitor {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.values
            .insert(field.name().to_string(), serde_json::Value::from(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.values
            .insert(field.name().to_string(), serde_json::Value::from(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.values
            .insert(field.name().to_string(), serde_json::Value::from(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.values
            .insert(field.name().to_string(), serde_json::Value::from(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.values.insert(
            normalize_field_name(field.name()),
            serde_json::Value::from(value),
        );
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn stdfmt::Debug) {
        // `tracing-log` forwards log crate metadata as synthetic `log.*` fields.
        // These are not user event payload, so we skip them here.
        if field.name().starts_with("log.") {
            return;
        }

        self.values.insert(
            normalize_field_name(field.name()),
            serde_json::Value::from(format!("{value:?}")),
        );
    }
}

/// Normalizes raw tracing field names to their emitted representation.
fn normalize_field_name(field_name: &str) -> String {
    field_name
        .strip_prefix("r#")
        .unwrap_or(field_name)
        .to_string()
}

/// Initializes tracing for the application.
///
/// Sets up structured logging with environment-appropriate configuration.
/// Production environments log to rotating files, development to console.
pub fn init_tracing(app_name: &str) -> Result<LogFlusher, TracingError> {
    init_tracing_with_top_level_fields(app_name, None, None)
}

/// Initializes tracing with optional top-level fields.
///
/// Like [`init_tracing`] but allows specifying multiple top-level fields that will be added to each
/// log entry.
pub fn init_tracing_with_top_level_fields(
    app_name: &str,
    project_ref: Option<&str>,
    pipeline_id: Option<u64>,
) -> Result<LogFlusher, TracingError> {
    // Set a global project reference if provided.
    if let Some(project) = project_ref {
        set_global_project_ref(project);
    }

    // Set global pipeline id if provided.
    if let Some(pipeline_id) = pipeline_id {
        set_global_pipeline_id(pipeline_id);
    }

    // Initialize the log tracer to capture logs from the `log` crate
    // and send them to the `tracing` subscriber. This captures logs
    // from libraries that use the `log` crate.
    LogTracer::init()?;

    let is_prod = Environment::load()?.is_prod();

    // Set the default log level to `info` if not specified in the `RUST_LOG` environment variable.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    let log_flusher = if is_prod {
        configure_prod_tracing(filter, app_name)?
    } else {
        configure_dev_tracing(filter)?
    };

    set_tracing_panic_hook();

    // Return the log flusher to ensure logs are flushed before the application exits
    // without this the logs in memory may not be flushed to the file.
    Ok(log_flusher)
}

/// Configures tracing for production environments.
///
/// Sets up structured JSON logging to rotating daily files with project injection.
fn configure_prod_tracing(filter: EnvFilter, app_name: &str) -> Result<LogFlusher, TracingError> {
    let filename_suffix = "log";
    let log_dir = "logs";

    let file_appender = rolling::Builder::new()
        .filename_prefix(app_name)
        .filename_suffix(filename_suffix)
        .rotation(rolling::Rotation::DAILY)
        .max_log_files(5)
        .build(log_dir)?;

    // Create a non-blocking appender to avoid blocking the logging thread
    // when writing to the file. This is important for performance.
    let (file_appender, guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = fmt()
        .with_env_filter(filter)
        .fmt_fields(format::JsonFields::new())
        .event_format(JsonContextFormatter::new(tracing_fmt::time::SystemTime))
        .with_writer(move || file_appender.make_writer())
        .finish();

    set_global_default(subscriber)?;

    Ok(LogFlusher::Flusher(guard))
}

/// Configures tracing for development environments.
///
/// Sets up pretty-printed console logging with ANSI colors for readability.
fn configure_dev_tracing(filter: EnvFilter) -> Result<LogFlusher, TracingError> {
    let format = format::format()
        .with_level(true)
        .with_ansi(true)
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_target(true);

    let subscriber = fmt()
        .with_env_filter(filter)
        .event_format(format)
        .with_writer(io::stdout)
        .finish();

    set_global_default(subscriber)?;

    Ok(LogFlusher::NullFlusher)
}

/// Sets up custom panic hook for structured panic logging.
///
/// Replaces the default panic hook to ensure panic information is captured
/// by the tracing system instead of only going to stderr.
fn set_tracing_panic_hook() {
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        panic_hook(info);
        prev_hook(info);
    }));
}

/// Custom panic hook that logs panic information using tracing.
///
/// Captures panic payload, location, and backtrace information as structured
/// log entries for better debugging and monitoring.
fn panic_hook(panic_info: &PanicHookInfo) {
    let backtrace = Backtrace::capture();
    let (backtrace, note) = match backtrace.status() {
        BacktraceStatus::Captured => (Some(backtrace), None),
        BacktraceStatus::Disabled => (
            None,
            Some("run with RUST_BACKTRACE=1 to display backtraces"),
        ),
        BacktraceStatus::Unsupported => {
            (None, Some("backtraces are not supported on this platform"))
        }
        _ => (None, Some("backtrace status is unknown")),
    };

    let payload = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
        s
    } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
        s
    } else {
        "unknown panic payload"
    };

    let location = panic_info.location().map(|location| location.to_string());

    tracing::error!(
        panic.payload = payload,
        payload.location = location,
        panic.backtrace = backtrace.map(display),
        panic.note = note,
        "a panic occurred",
    );
}
