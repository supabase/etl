//! Instruction, allocation, and wall-clock counters for measured passes.

#[cfg(target_env = "msvc")]
use std::alloc::System as InnerAllocator;
#[cfg(target_os = "macos")]
use std::mem::MaybeUninit;
use std::{
    alloc::{GlobalAlloc, Layout},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc as InnerAllocator;

/// Allocator calls that requested memory since process start.
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

/// Bytes requested by those allocator calls.
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Global allocator that counts memory requests, then forwards them to the
/// allocator the replicator ships with.
///
/// `alloc`, `alloc_zeroed`, and `realloc` each count as one allocation, as
/// DHAT counts blocks. The two relaxed atomic adds cost the same in every
/// build being compared.
pub struct CountingAllocator;

/// Counts one allocator call that requested `size` bytes.
fn record_allocation(size: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
}

// SAFETY: every method forwards its arguments unchanged to `InnerAllocator`,
// which upholds the `GlobalAlloc` contract. The counters never touch the
// returned memory.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        // SAFETY: the caller upholds the `GlobalAlloc::alloc` contract.
        unsafe { InnerAllocator.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds the `GlobalAlloc::dealloc` contract, and
        // `ptr` came from `InnerAllocator` because every request is forwarded.
        unsafe { InnerAllocator.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        // SAFETY: the caller upholds the `GlobalAlloc::alloc_zeroed` contract.
        unsafe { InnerAllocator.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(new_size);
        // SAFETY: the caller upholds the `GlobalAlloc::realloc` contract, and
        // `ptr` came from `InnerAllocator` because every request is forwarded.
        unsafe { InnerAllocator.realloc(ptr, layout, new_size) }
    }
}

/// Returns the instructions this process has retired so far.
///
/// `None` means the platform exposes no unprivileged in-process counter, or
/// the kernel refused the read.
#[cfg(target_os = "macos")]
fn instructions_retired() -> Option<u64> {
    let pid = libc::c_int::try_from(std::process::id()).ok()?;
    let mut info = MaybeUninit::<libc::rusage_info_v4>::uninit();

    // SAFETY: `info` is writable storage for a `rusage_info_v4`, which is the
    // structure the kernel fills for the `RUSAGE_INFO_V4` flavor.
    let status =
        unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V4, info.as_mut_ptr().cast()) };
    if status != 0 {
        return None;
    }

    // SAFETY: a zero status means the kernel initialized the whole structure.
    let info = unsafe { info.assume_init() };

    Some(info.ri_instructions)
}

/// Returns the instructions this process has retired so far.
///
/// Always `None`: only macOS has an in-process counter wired up. Elsewhere,
/// run one scenario under `perf stat -e instructions:u` or Cachegrind.
#[cfg(not(target_os = "macos"))]
fn instructions_retired() -> Option<u64> {
    None
}

/// Process counters at one instant.
#[derive(Debug, Clone, Copy)]
pub(super) struct Counters {
    /// Instructions retired, when the platform reports them.
    instructions: Option<u64>,
    /// Allocator calls that requested memory.
    allocations: u64,
    /// Bytes requested by those calls.
    allocated_bytes: u64,
    /// Wall-clock reading.
    at: Instant,
}

impl Counters {
    /// Reads every counter.
    pub(super) fn read() -> Self {
        Self {
            instructions: instructions_retired(),
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            at: Instant::now(),
        }
    }

    /// Returns what the process consumed between `earlier` and this reading.
    pub(super) fn since(self, earlier: Self) -> Usage {
        Usage {
            instructions: self
                .instructions
                .zip(earlier.instructions)
                .map(|(later, earlier)| later.saturating_sub(earlier)),
            allocations: self.allocations.saturating_sub(earlier.allocations),
            allocated_bytes: self.allocated_bytes.saturating_sub(earlier.allocated_bytes),
            elapsed: self.at.saturating_duration_since(earlier.at),
        }
    }
}

/// Resources one measured pass consumed.
#[derive(Debug, Clone, Copy)]
pub(super) struct Usage {
    /// Instructions retired, when the platform reports them.
    pub(super) instructions: Option<u64>,
    /// Allocator calls that requested memory.
    pub(super) allocations: u64,
    /// Bytes requested by those calls.
    pub(super) allocated_bytes: u64,
    /// Wall-clock time.
    pub(super) elapsed: Duration,
}
