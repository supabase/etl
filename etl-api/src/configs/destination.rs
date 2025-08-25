use etl_config::SerializableSecretString;

#[derive(Debug)]
pub enum CreateDestinationDestinationConfig {
    Memory,
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: SerializableSecretString,
        max_staleness_mins: Option<u16>,
        max_concurrent_streams: Option<usize>,
    },
}