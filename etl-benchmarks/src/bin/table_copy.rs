#[tokio::main]
async fn main() -> anyhow::Result<()> {
    etl_benchmarks::table_copy::main().await
}
