#[tokio::main]
#[cfg_attr(feature = "hotpath", hotpath::main(limit = 0, report = "all"))]
async fn main() -> anyhow::Result<()> {
    etl_benchmarks::common::init_hotpath_tokio_runtime();
    #[cfg_attr(
        feature = "hotpath",
        allow(
            clippy::large_futures,
            reason = "Benchmark profiling startup futures are intentionally stack-allocated to \
                      avoid per-run heap boxing."
        )
    )]
    etl_benchmarks::table_copy::main().await
}
