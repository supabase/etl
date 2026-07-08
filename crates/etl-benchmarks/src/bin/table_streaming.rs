#[tokio::main]
#[cfg_attr(feature = "hotpath", hotpath::main(limit = 0, report = "all"))]
async fn main() -> anyhow::Result<()> {
    etl_benchmarks::common::init_hotpath_tokio_runtime();
    etl_benchmarks::table_streaming::main().await
}
