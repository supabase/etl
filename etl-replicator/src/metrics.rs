use etl_telemetry::metrics::init_default_metrics;

pub fn init_metrics() -> anyhow::Result<()> {
    init_default_metrics()?;
    Ok(())
}
