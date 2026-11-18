#[tokio::main]
async fn main() -> anyhow::Result<()> {
    etl_benchmarks::lock_contention::main().await
}
