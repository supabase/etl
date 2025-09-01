use etl_config::Environment;
use std::io::Error;
use std::io::Write;
use std::sync::OnceLock;
use std::{
    backtrace::{Backtrace, BacktraceStatus},
    panic::PanicHookInfo,
    sync::Once,
};
use thiserror::Error;
use tracing::subscriber::{SetGlobalDefaultError, set_global_default};
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{self, InitError},
};
use tracing_log::{LogTracer, log_tracer::SetLoggerError};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{EnvFilter, FmtSubscriber, Registry, fmt, layer::SubscriberExt};

/// JSON field name for project identification in logs.
const PROJECT_KEY_IN_LOG: &str = "project";
/// JSON field name for pipeline identification in logs.
const PIPELINE_KEY_IN_LOG: &str = "pipeline_id";

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

static INIT_TEST_TRACING: Once = Once::new();

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

/// Global project reference storage
static PROJECT_REF: OnceLock<String> = OnceLock::new();
/// Global pipeline id storage.
static PIPELINE_ID: OnceLock<u64> = OnceLock::new();

/// Sets the global project reference for all tracing events.
///
/// The project reference will be injected into all structured log entries
/// for identification and filtering purposes.
pub fn set_global_project_ref(project_ref: String) {
    let _ = PROJECT_REF.set(project_ref);
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

/// Writer wrapper that injects project field into JSON log entries.
///
/// Parses JSON log entries and adds a project field if one doesn't already exist,
/// enabling project-based filtering and identification in log aggregation systems.
struct ProjectInjectingWriter<W> {
    inner: W,
}

impl<W> ProjectInjectingWriter<W> {
    /// Creates a new project-injecting writer wrapping the inner writer.
    fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W> Write for ProjectInjectingWriter<W>
where
    W: Write,
{
    /// Writes log data, injecting project and pipeline fields into JSON entries.
    ///
    /// Attempts to parse the buffer as JSON and inject a project field if:
    /// - A global project reference is set
    /// - The content is valid JSON
    /// - No project field already exists
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Only try to inject fields if the content looks like JSON
        if let Ok(json_str) = std::str::from_utf8(buf) {
            // Try to parse as JSON
            if let Ok(serde_json::Value::Object(mut map)) =
                serde_json::from_str::<serde_json::Value>(json_str)
            {
                let mut modified = false;

                // Inject project if available and not present
                if let Some(project_ref) = get_global_project_ref()
                    && !map.contains_key(PROJECT_KEY_IN_LOG)
                {
                    map.insert(
                        PROJECT_KEY_IN_LOG.to_string(),
                        serde_json::Value::String(project_ref.to_string()),
                    );
                    modified = true;
                }

                // Inject pipeline_id if available and not present
                if let Some(pipeline_id) = get_global_pipeline_id()
                    && !map.contains_key(PIPELINE_KEY_IN_LOG)
                {
                    map.insert(
                        PIPELINE_KEY_IN_LOG.to_string(),
                        serde_json::Value::Number(serde_json::Number::from(pipeline_id)),
                    );
                    modified = true;
                }

                if modified {
                    // Try to serialize back to JSON
                    if let Ok(modified) = serde_json::to_string(&map) {
                        // Preserve trailing newline if present
                        let output = if json_str.ends_with('\n') {
                            format!("{modified}\n")
                        } else {
                            modified
                        };

                        // Write the modified JSON and return the original buffer length
                        return match self.inner.write(output.as_bytes()) {
                            Ok(_) => Ok(buf.len()),
                            Err(e) => Err(e),
                        };
                    }
                }
            }
        }

        // Fallback to original content
        self.inner.write(buf)
    }

    /// Flushes the underlying writer.
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
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
    project_ref: Option<String>,
    pipeline_id: Option<u64>,
) -> Result<LogFlusher, TracingError> {
    // Set global project reference if provided.
    if let Some(ref project) = project_ref {
        set_global_project_ref(project.clone());
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
        // rotate the log file every day
        .rotation(rolling::Rotation::DAILY)
        // keep a maximum of 5 log files
        .max_log_files(5)
        .build(log_dir)?;

    // Create a non-blocking appender to avoid blocking the logging thread
    // when writing to the file. This is important for performance.
    let (file_appender, guard) = tracing_appender::non_blocking(file_appender);

    let format = fmt::format()
        .with_level(true)
        // ANSI colors are only for terminal output
        .with_ansi(false)
        // Disable target to reduce noise in the logs
        .with_target(false);

    let subscriber = Registry::default().with(filter).with(
        fmt::layer()
            .event_format(format)
            .with_writer(move || ProjectInjectingWriter::new(file_appender.make_writer()))
            .json()
            .with_current_span(true)
            .with_span_list(true),
    );

    set_global_default(subscriber)?;

    Ok(LogFlusher::Flusher(guard))
}

/// Configures tracing for development environments.
///
/// Sets up pretty-printed console logging with ANSI colors for readability.
fn configure_dev_tracing(filter: EnvFilter) -> Result<LogFlusher, TracingError> {
    let format = fmt::format()
        // Emit the log level in the log output
        .with_level(true)
        // Enable ANSI colors for terminal output
        .with_ansi(true)
        // Make it pretty
        .pretty()
        // Disable line number, file, and target in the log output
        // to reduce noise in the logs
        .with_line_number(false)
        .with_file(false)
        .with_target(true);

    let subscriber_builder = FmtSubscriber::builder()
        .event_format(format)
        .with_env_filter(filter);

    let subscriber = subscriber_builder.finish();

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
