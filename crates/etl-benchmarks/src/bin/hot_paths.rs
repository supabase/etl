use etl_benchmarks::hot_paths::CountingAllocator;

/// Counts allocations for the benchmark report.
#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn main() -> anyhow::Result<()> {
    etl_benchmarks::hot_paths::main()
}
