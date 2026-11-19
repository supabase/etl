//! Global allocator setup shared by the end-to-end benchmark binaries.

/// Installs jemalloc as the global allocator with the replicator's tuning.
///
/// Call it once at the top of a benchmark binary so the benchmark allocates
/// the way `etl-replicator` does. Jemalloc reads its options from a
/// `malloc_conf` symbol in the final binary, so the items are emitted there
/// instead of living in this library. The option string must match
/// `crates/etl-replicator/src/main.rs`, where each option is explained.
#[macro_export]
macro_rules! replicator_allocator {
    () => {
        /// Jemalloc, as the replicator uses.
        #[cfg(not(target_env = "msvc"))]
        #[global_allocator]
        static GLOBAL: ::tikv_jemallocator::Jemalloc = ::tikv_jemallocator::Jemalloc;

        /// The replicator's jemalloc options.
        #[cfg(all(target_os = "linux", not(target_env = "msvc")))]
        #[unsafe(export_name = "malloc_conf")]
        static MALLOC_CONF: &[u8] = b"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";

        /// The replicator's jemalloc options, under the prefixed symbol macOS
        /// uses.
        #[cfg(all(target_os = "macos", not(target_env = "msvc")))]
        #[unsafe(export_name = "_rjem_malloc_conf")]
        static MALLOC_CONF: &[u8] = b"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";
    };
}
