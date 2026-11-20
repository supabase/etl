//! Wait-time profiler for tokio locks.
//!
//! With `--cfg tokio_unstable` and tokio's `tracing` feature, tokio opens a
//! `runtime.resource` span when a `Mutex`, `RwLock`, or `Semaphore` is
//! created, a `runtime.resource.async_op` span for each acquire, and a
//! `runtime.resource.async_op.poll` span that it enters on every poll of that
//! acquire. The poll span closes when the acquire future is dropped, which is
//! right after it returns the guard. So its lifetime is the time spent
//! acquiring, and more than one poll means the task had to wait.
//!
//! Waiting tasks do not block threads, so thread profilers cannot see this
//! time. The profiler records it per lock, keyed by where the lock was
//! created. For a sample of the waits it also records which workspace code was
//! waiting.

use std::{
    backtrace::Backtrace,
    collections::HashMap,
    fmt,
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result};
use tracing::{
    Metadata, Subscriber,
    field::{Field, Visit},
    level_filters::LevelFilter,
    span::{Attributes, Id},
};
use tracing_subscriber::{
    Layer,
    filter::filter_fn,
    layer::{Context, SubscriberExt as _},
    registry::LookupSpan,
    util::SubscriberInitExt as _,
};

/// Tokio span opened for each instrumented synchronization primitive.
const RESOURCE_SPAN: &str = "runtime.resource";
/// Tokio span opened for each acquire of a resource.
const ASYNC_OP_SPAN: &str = "runtime.resource.async_op";
/// Tokio span entered once per poll of an acquire.
const ASYNC_OP_POLL_SPAN: &str = "runtime.resource.async_op.poll";
/// Call stacks kept per lock, operation, and thread.
const MAX_STACK_SAMPLES: usize = 16;
/// Marker of git dependencies, followed by `<name>-<hash>/<rev>/`.
const GIT_CHECKOUTS: &str = "/.cargo/git/checkouts/";
/// Waiting call sites shown per lock.
const CALL_SITES_SHOWN: usize = 3;
/// Power-of-two nanosecond buckets; the last one is unbounded.
const WAIT_BUCKETS: usize = 64;

/// Per-thread statistics merged when a report is taken.
type ThreadStats = Arc<Mutex<HashMap<OpKey, OpStats>>>;

/// Every thread's statistics, so a report can merge them.
static ALL_THREAD_STATS: LazyLock<Mutex<Vec<ThreadStats>>> = LazyLock::new(Mutex::default);

/// Locks profiler state; every update leaves it valid, so poisoning is
/// ignored.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Registers a new thread's statistics with [`ALL_THREAD_STATS`].
fn register_thread_stats() -> ThreadStats {
    let stats = ThreadStats::default();
    lock(&ALL_THREAD_STATS).push(Arc::clone(&stats));
    stats
}

thread_local! {
    /// This thread's statistics. Each thread writes only its own map, so
    /// recording does not add a shared lock to the code being measured.
    static THREAD_STATS: ThreadStats = register_thread_stats();
}

/// Lock types whose acquires are profiled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockKind {
    /// `tokio::sync::Mutex`.
    Mutex,
    /// `tokio::sync::RwLock`.
    RwLock,
    /// `tokio::sync::Semaphore`.
    Semaphore,
}

impl LockKind {
    /// Parses tokio's `concrete_type` field.
    fn parse(concrete_type: &str) -> Option<Self> {
        match concrete_type {
            "Mutex" => Some(Self::Mutex),
            "RwLock" => Some(Self::RwLock),
            "Semaphore" => Some(Self::Semaphore),
            _ => None,
        }
    }
}

impl fmt::Display for LockKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Mutex => "Mutex",
            Self::RwLock => "RwLock",
            Self::Semaphore => "Semaphore",
        })
    }
}

/// One lock, identified by where it was created, and one acquire operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OpKey {
    /// Lock type.
    kind: LockKind,
    /// Source location of the lock's constructor call.
    created_at: Arc<str>,
    /// Tokio's operation name, such as `Mutex::lock`.
    op: &'static str,
}

/// Extension on a resource span.
struct Resource {
    /// Lock type.
    kind: LockKind,
    /// Source location of the lock's constructor call.
    created_at: Arc<str>,
}

/// Extension on an acquire span.
struct AsyncOp {
    /// Lock and operation being acquired.
    key: OpKey,
}

/// Extension on an acquire's poll span while the acquire is in progress.
struct PendingAcquire {
    /// Lock and operation being acquired.
    key: OpKey,
    /// When the acquire started.
    started: Instant,
    /// Times the acquire was polled.
    polls: u32,
}

/// Aggregated acquires of one lock operation.
struct OpStats {
    /// Completed or cancelled acquires.
    acquires: u64,
    /// Acquires that returned pending at least once.
    waits: u64,
    /// Total time spent in acquires that waited.
    wait_total: Duration,
    /// Longest wait.
    wait_max: Duration,
    /// Wait counts by power-of-two nanosecond bucket.
    wait_buckets: [u64; WAIT_BUCKETS],
    /// Call stacks of sampled waits, resolved only when reporting.
    stacks: Vec<Backtrace>,
}

impl Default for OpStats {
    fn default() -> Self {
        Self {
            acquires: 0,
            waits: 0,
            wait_total: Duration::ZERO,
            wait_max: Duration::ZERO,
            wait_buckets: [0; WAIT_BUCKETS],
            stacks: Vec::new(),
        }
    }
}

impl OpStats {
    /// Records one finished acquire.
    fn record(&mut self, waited: Option<Duration>) {
        self.acquires += 1;
        let Some(wait) = waited else {
            return;
        };

        self.waits += 1;
        self.wait_total += wait;
        self.wait_max = self.wait_max.max(wait);
        self.wait_buckets[wait_bucket(wait)] += 1;
        if self.stacks.len() < MAX_STACK_SAMPLES {
            self.stacks.push(Backtrace::force_capture());
        }
    }

    /// Adds another thread's statistics for the same operation.
    fn merge(&mut self, other: &mut Self) {
        self.acquires += other.acquires;
        self.waits += other.waits;
        self.wait_total += other.wait_total;
        self.wait_max = self.wait_max.max(other.wait_max);
        for (bucket, count) in self.wait_buckets.iter_mut().zip(other.wait_buckets) {
            *bucket += count;
        }
        self.stacks.append(&mut other.stacks);
    }

    /// Returns an upper bound for the wait at quantile `q` in `0.0..=1.0`.
    fn wait_quantile(&self, q: f64) -> Duration {
        // Waits are counts well below 2^52, so the conversions are exact.
        let target = ((self.waits as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (index, count) in self.wait_buckets.iter().enumerate() {
            seen += count;
            if seen >= target {
                return bucket_upper_bound(index).min(self.wait_max);
            }
        }
        self.wait_max
    }
}

/// Returns the bucket holding `wait`: bucket `i` holds `2^i..2^(i+1)` ns.
fn wait_bucket(wait: Duration) -> usize {
    let nanos = u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX).max(1);
    // `ilog2` of a nonzero `u64` is at most 63, below `WAIT_BUCKETS`.
    nanos.ilog2() as usize
}

/// Returns the exclusive upper bound of a bucket.
fn bucket_upper_bound(index: usize) -> Duration {
    let nanos = u32::try_from(index + 1).ok().and_then(|shift| 1_u64.checked_shl(shift));
    nanos.map_or(Duration::MAX, Duration::from_nanos)
}

/// Collects the tokio span fields the profiler needs.
#[derive(Default)]
struct FieldVisitor {
    /// `concrete_type` of a resource.
    concrete_type: Option<String>,
    /// `loc.file` of a resource.
    file: Option<String>,
    /// `loc.line` of a resource.
    line: Option<u64>,
    /// `source` of an acquire.
    source: Option<&'static str>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "concrete_type" => self.concrete_type = Some(value.to_owned()),
            "loc.file" => self.file = Some(value.to_owned()),
            "source" => self.source = Some(op_name(value)),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "loc.line" {
            self.line = Some(value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

/// Maps tokio's operation name to a static string so acquires do not allocate.
fn op_name(source: &str) -> &'static str {
    match source {
        "Mutex::lock" => "Mutex::lock",
        "Mutex::lock_owned" => "Mutex::lock_owned",
        "RwLock::read" => "RwLock::read",
        "RwLock::read_owned" => "RwLock::read_owned",
        "RwLock::write" => "RwLock::write",
        "RwLock::write_owned" => "RwLock::write_owned",
        "Semaphore::acquire" => "Semaphore::acquire",
        "Semaphore::acquire_many" => "Semaphore::acquire_many",
        "Semaphore::acquire_owned" => "Semaphore::acquire_owned",
        "Semaphore::acquire_many_owned" => "Semaphore::acquire_many_owned",
        _ => "other",
    }
}

/// Shortens a source path to its workspace or dependency-relative form.
fn short_path(path: &str) -> &str {
    if let Some(index) = path.find("/crates/etl") {
        return &path[index + 1..];
    }
    if let Some(index) = path.find("/index.crates.io-")
        && let Some(crate_start) = path[index + 1..].find('/')
    {
        return &path[index + 1 + crate_start + 1..];
    }
    if let Some(index) = path.find(GIT_CHECKOUTS) {
        let checkout = &path[index + GIT_CHECKOUTS.len()..];
        if let Some((_, rest)) = checkout.split_once('/')
            && let Some((_, rest)) = rest.split_once('/')
        {
            return rest;
        }
    }
    path
}

/// Tracing layer that records tokio lock acquires.
struct LockProfiler;

impl<S> Layer<S> for LockProfiler
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        match attrs.metadata().name() {
            RESOURCE_SPAN => {
                let mut fields = FieldVisitor::default();
                attrs.record(&mut fields);
                // Tokio-internal semaphores have no location and no acquire
                // spans of their own.
                let (Some(kind), Some(file), Some(line)) = (
                    fields.concrete_type.as_deref().and_then(LockKind::parse),
                    fields.file,
                    fields.line,
                ) else {
                    return;
                };
                let created_at = format!("{}:{line}", short_path(&file)).into();
                span.extensions_mut().insert(Resource { kind, created_at });
            }
            ASYNC_OP_SPAN => {
                let mut fields = FieldVisitor::default();
                attrs.record(&mut fields);
                let Some(parent) = span.parent() else {
                    return;
                };
                let key = {
                    let extensions = parent.extensions();
                    let Some(resource) = extensions.get::<Resource>() else {
                        return;
                    };
                    OpKey {
                        kind: resource.kind,
                        created_at: Arc::clone(&resource.created_at),
                        op: fields.source.unwrap_or("other"),
                    }
                };
                span.extensions_mut().insert(AsyncOp { key });
            }
            ASYNC_OP_POLL_SPAN => {
                let Some(parent) = span.parent() else {
                    return;
                };
                let key = {
                    let extensions = parent.extensions();
                    let Some(async_op) = extensions.get::<AsyncOp>() else {
                        return;
                    };
                    async_op.key.clone()
                };
                span.extensions_mut().insert(PendingAcquire {
                    key,
                    started: Instant::now(),
                    polls: 0,
                });
            }
            _ => {}
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        if span.name() != ASYNC_OP_POLL_SPAN {
            return;
        }
        if let Some(pending) = span.extensions_mut().get_mut::<PendingAcquire>() {
            pending.polls = pending.polls.saturating_add(1);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(pending) = span.extensions_mut().remove::<PendingAcquire>() else {
            return;
        };

        let waited = (pending.polls > 1).then(|| pending.started.elapsed());
        // The poll span closes while the waiting caller drops the acquire
        // future, so a stack captured here shows the caller.
        THREAD_STATS.with(|stats| lock(stats).entry(pending.key).or_default().record(waited));
    }
}

/// Selects tokio's resource spans for the profiler.
fn is_tokio_resource_span(metadata: &Metadata<'_>) -> bool {
    metadata.is_span() && metadata.name().starts_with(RESOURCE_SPAN)
}

/// Installs the profiler and a warning-level log layer as the global
/// subscriber.
///
/// Tokio emits the profiled spans only when built with `--cfg tokio_unstable`.
pub fn install() -> Result<()> {
    let profiler = LockProfiler.with_filter(filter_fn(is_tokio_resource_span));
    let logs = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(LevelFilter::WARN);
    tracing_subscriber::registry()
        .with(profiler)
        .with(logs)
        .try_init()
        .context("failed to install the lock profiler")
}

/// Discards everything recorded so far.
pub fn reset() {
    for stats in lock(&ALL_THREAD_STATS).iter() {
        lock(stats).clear();
    }
}

/// One lock operation in a report.
pub struct LockReportRow {
    /// Lock type.
    pub kind: LockKind,
    /// Source location of the lock's constructor call.
    pub created_at: Arc<str>,
    /// Tokio's operation name.
    pub op: &'static str,
    /// Completed or cancelled acquires.
    pub acquires: u64,
    /// Acquires that had to wait.
    pub waits: u64,
    /// Total time spent in acquires that waited.
    pub wait_total: Duration,
    /// Upper bound of the median wait.
    pub wait_p50: Duration,
    /// Upper bound of the 99th-percentile wait.
    pub wait_p99: Duration,
    /// Longest wait.
    pub wait_max: Duration,
    /// Most common waiting call sites with their sample counts.
    pub call_sites: Vec<(String, usize)>,
    /// Waits whose call stacks were sampled.
    pub sampled_waits: usize,
}

/// Lock operations sorted by total wait, longest first.
pub struct LockReport {
    /// Rows for every lock operation that was acquired.
    pub rows: Vec<LockReportRow>,
}

impl LockReport {
    /// Returns the total number of profiled acquires.
    pub fn acquires(&self) -> u64 {
        self.rows.iter().map(|row| row.acquires).sum()
    }
}

/// Merges every thread's statistics into a report.
pub fn snapshot() -> LockReport {
    let mut merged: HashMap<OpKey, OpStats> = HashMap::new();
    for stats in lock(&ALL_THREAD_STATS).iter() {
        for (key, mut op_stats) in lock(stats).drain() {
            merged.entry(key).or_default().merge(&mut op_stats);
        }
    }

    let mut rows: Vec<_> = merged
        .into_iter()
        .map(|(key, stats)| {
            let mut sites: HashMap<String, usize> = HashMap::new();
            for stack in &stats.stacks {
                *sites.entry(call_site(stack)).or_default() += 1;
            }
            let mut call_sites: Vec<_> = sites.into_iter().collect();
            call_sites.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            call_sites.truncate(CALL_SITES_SHOWN);

            LockReportRow {
                kind: key.kind,
                created_at: key.created_at,
                op: key.op,
                acquires: stats.acquires,
                waits: stats.waits,
                wait_total: stats.wait_total,
                wait_p50: stats.wait_quantile(0.50),
                wait_p99: stats.wait_quantile(0.99),
                wait_max: stats.wait_max,
                call_sites,
                sampled_waits: stats.stacks.len(),
            }
        })
        .collect();
    rows.sort_by(|a, b| {
        b.wait_total
            .cmp(&a.wait_total)
            .then_with(|| b.acquires.cmp(&a.acquires))
            .then_with(|| a.created_at.cmp(&b.created_at))
            .then_with(|| a.op.cmp(b.op))
    });

    LockReport { rows }
}

/// Returns the file part of a `path:line` frame.
fn source_file(frame: &str) -> &str {
    frame.rsplit_once(':').map_or(frame, |(file, _)| file)
}

/// Returns where a sampled wait happened: the innermost workspace frame, then
/// the nearest frame in another file, which is usually the lock wrapper's
/// caller.
///
/// `std::backtrace` exposes frames only through its text form, where each
/// frame's location is a line starting with `at `.
fn call_site(stack: &Backtrace) -> String {
    let text = stack.to_string();
    let mut frames = text
        .lines()
        .filter_map(|line| line.trim().strip_prefix("at "))
        .filter(|location| {
            location.contains("/crates/etl") && !location.contains("/crates/etl-benchmarks/")
        })
        .map(|location| {
            let location = short_path(location);
            // Drop the column so one call site is one entry.
            location.rsplit_once(':').map_or(location, |(path_and_line, _)| path_and_line)
        });

    let Some(innermost) = frames.next() else {
        return "unresolved (no workspace frame in the sampled stack)".to_owned();
    };
    let innermost_file = source_file(innermost);
    match frames.find(|frame| source_file(frame) != innermost_file) {
        Some(caller) => format!("{innermost} <- {caller}"),
        None => innermost.to_owned(),
    }
}
