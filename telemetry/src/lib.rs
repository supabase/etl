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
use tracing_subscriber::{
    EnvFilter, FmtSubscriber,
    fmt::{self, format::FmtSpan},
};

const DEV_ENV_NAME: &str = "dev";
const PROD_ENV_NAME: &str = "prod";

#[derive(Debug, Error)]
pub enum TracingError {
    #[error("failed to build rolling file appender: {0}")]
    InitAppender(#[from] InitError),

    #[error("failed to init log tracer: {0}")]
    InitLogTracer(#[from] SetLoggerError),

    #[error("failed to set global default subscriber: {0}")]
    SetGlobalDefault(#[from] SetGlobalDefaultError),
}

#[must_use]
pub enum LogFlusher {
    Flusher(WorkerGuard),
    NullFlusher,
}

static INIT_TEST_TRACING: Once = Once::new();

/// Call this function once at the beginning of a test and then set the ENABLE_TRACING
/// environment variable to 1 to view tracing in the terminal:
///
/// ENABLE_TRACING=1 cargo test <test_name>
///
pub fn init_test_tracing() {
    INIT_TEST_TRACING.call_once(|| {
        if std::env::var("ENABLE_TRACING").is_ok() {
            let _log_flusher =
                init_tracing("test", false).expect("Failed to initialize tracing for tests");
        }
    });
}

/// Initializes tracing for the application.
pub fn init_tracing(app_name: &str, emit_on_span_close: bool) -> Result<LogFlusher, TracingError> {
    // Initialize the log tracer to capture logs from the `log` crate
    // and send them to the `tracing` subscriber. This captures logs
    // from libraries that use the `log` crate.
    LogTracer::init()?;

    let is_prod =
        std::env::var("APP_ENVIRONMENT").unwrap_or_else(|_| DEV_ENV_NAME.into()) == PROD_ENV_NAME;

    // Set the default log level to `info` if not specified in the RUST_LOG environment variable.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    let log_flusher = if is_prod {
        configure_prod_tracing(filter, app_name, emit_on_span_close)?
    } else {
        configure_dev_tracing(filter, emit_on_span_close)?
    };

    set_tracing_panic_hook();

    // Return the log flusher to ensure logs are flushed before the application exits
    // without this the logs in memory may not be flushed to the file
    Ok(log_flusher)
}

fn configure_prod_tracing(
    filter: EnvFilter,
    app_name: &str,
    emit_on_span_close: bool,
) -> Result<LogFlusher, TracingError> {
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

    let subscriber_builder = FmtSubscriber::builder()
        .event_format(format)
        .with_writer(file_appender)
        .json()
        .with_env_filter(filter);

    let subscriber_builder = if emit_on_span_close {
        subscriber_builder.with_span_events(FmtSpan::CLOSE)
    } else {
        subscriber_builder
    };

    let subscriber = subscriber_builder.finish();

    set_global_default(subscriber)?;

    Ok(LogFlusher::Flusher(guard))
}

fn configure_dev_tracing(
    filter: EnvFilter,
    emit_on_span_close: bool,
) -> Result<LogFlusher, TracingError> {
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

    let subscriber_builder = if emit_on_span_close {
        subscriber_builder.with_span_events(FmtSpan::CLOSE)
    } else {
        subscriber_builder
    };

    let subscriber = subscriber_builder.finish();

    set_global_default(subscriber)?;

    Ok(LogFlusher::NullFlusher)
}

/// The default panic hook logs the panic information to stderr, which means
/// it will not be sent to our logging system. This function replaces the default panic
/// hook with a custom one that logs the panic information using `tracing`.
/// It also calls the original panic hook after logging the panic information.
fn set_tracing_panic_hook() {
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        panic_hook(info);
        prev_hook(info);
    }));
}

/// A custom panic hook that logs the panic information using `tracing`.
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
        panic.backtrace = backtrace.map(tracing::field::display),
        panic.note = note,
        "a panic occurred",
    );
}
