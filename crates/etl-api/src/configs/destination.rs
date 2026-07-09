use etl_config::{
    SerializableSecretString,
    shared::{ClickHouseEngine, DestinationConfig, DuckLakeMaintenanceMode, IcebergConfig},
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use thiserror::Error;
use url::Url;
use utoipa::ToSchema;

use crate::configs::{
    encryption::{
        Decrypt, DecryptionError, Encrypt, EncryptedValue, EncryptionError, EncryptionKeyring,
        decrypt_text, encrypt_text,
    },
    store::Store,
    update::UpdateField,
};

/// Returns the default connection pool size for BigQuery destinations.
pub const fn default_connection_pool_size() -> usize {
    DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE
}

/// Returns the default connection pool size for DuckLake destinations.
pub const fn default_ducklake_pool_size() -> u32 {
    DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiDestinationConfig {
    BigQuery {
        #[schema(example = "my-gcp-project")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        project_id: String,
        #[schema(example = "my_dataset")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        dataset_id: String,
        #[schema(example = "{\"type\": \"service_account\", \"project_id\": \"my-project\"}")]
        service_account_key: SerializableSecretString,
        #[schema(example = 15)]
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
        #[schema(example = 8)]
        #[serde(skip_serializing_if = "Option::is_none")]
        connection_pool_size: Option<usize>,
    },
    #[serde(rename = "clickhouse")]
    ClickHouse {
        /// ClickHouse HTTP(S) endpoint URL.
        #[schema(value_type = String, example = "http://test:8123")]
        #[serde(deserialize_with = "crate::utils::trim_http_url")]
        url: Url,
        /// ClickHouse user name
        #[schema(example = "foo")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        user: String,
        /// ClickHouse password (omit for passwordless access)
        #[serde(skip_serializing_if = "Option::is_none")]
        password: Option<SerializableSecretString>,
        /// ClickHouse target database
        #[schema(example = "my_db")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        database: String,
        /// Table engine used for replicated tables.
        #[schema(value_type = String, example = "replacing_merge_tree")]
        #[serde(default)]
        engine: ClickHouseEngine,
    },
    Iceberg {
        #[serde(flatten)]
        config: ApiIcebergConfig,
    },
    Ducklake {
        #[schema(value_type = String, example = "postgres://localhost:5432/ducklake_catalog")]
        #[serde(deserialize_with = "crate::utils::trim_secret_string")]
        catalog_url: SerializableSecretString,
        #[schema(example = "s3://bucket/path")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        data_path: String,
        #[schema(example = 4)]
        #[serde(skip_serializing_if = "Option::is_none")]
        pool_size: Option<u32>,
        #[schema(example = "my-access-key")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_secret_string"
        )]
        s3_access_key_id: Option<SerializableSecretString>,
        #[schema(example = "my-secret-key")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_secret_string"
        )]
        s3_secret_access_key: Option<SerializableSecretString>,
        #[schema(example = "us-east-1")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        s3_region: Option<String>,
        #[schema(example = "127.0.0.1:5000/s3")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        s3_endpoint: Option<String>,
        #[schema(example = "path")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        s3_url_style: Option<String>,
        #[schema(example = false)]
        #[serde(skip_serializing_if = "Option::is_none")]
        s3_use_ssl: Option<bool>,
        #[schema(example = "ducklake")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        metadata_schema: Option<String>,
        #[schema(example = "500MB")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        maintenance_target_file_size: Option<String>,
        #[schema(example = "7 days")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        expire_snapshots_older_than: Option<String>,
        #[schema(example = "kubernetes")]
        #[serde(default)]
        maintenance_mode: DuckLakeMaintenanceMode,
    },
    Snowflake {
        #[schema(example = "ORGNAME-ACCOUNTNAME")]
        #[serde(deserialize_with = "crate::utils::trim_snowflake_account_id")]
        account_id: String,
        #[schema(example = "ETL_USER")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        user: String,
        #[schema(example = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADA...")]
        private_key: SerializableSecretString,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        private_key_passphrase: Option<SerializableSecretString>,
        #[schema(example = "ANALYTICS")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        database: String,
        #[schema(example = "PUBLIC")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        schema: String,
        #[schema(example = "ETL_ROLE")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        role: Option<String>,
    },
}

/// Errors returned while merging destination update configuration.
#[derive(Debug, Error)]
pub enum DestinationConfigUpdateError {
    /// A required field was omitted when no stored value can be preserved.
    #[error("Missing required field `{field}` for {destination} destination")]
    MissingRequiredField { destination: &'static str, field: &'static str },

    /// A required secret was omitted when no stored value can be preserved.
    #[error("Missing required secret field `{field}` for {destination} destination")]
    MissingRequiredSecret { destination: &'static str, field: &'static str },

    /// A required field was explicitly cleared.
    #[error("Field `{field}` cannot be cleared for {destination} destination")]
    RequiredFieldCleared { destination: &'static str, field: &'static str },
}

fn deserialize_update_trimmed_string<'de, D>(
    deserializer: D,
) -> Result<UpdateField<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
        .map(|value| UpdateField::from_option(value.map(|value| value.trim().to_owned())))
}

fn deserialize_update_secret_string<'de, D>(
    deserializer: D,
) -> Result<UpdateField<SerializableSecretString>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(|value| {
        UpdateField::from_option(
            value.map(|value| SerializableSecretString::from(value.trim().to_owned())),
        )
    })
}

fn deserialize_update_http_url<'de, D>(deserializer: D) -> Result<UpdateField<Url>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    value
        .map(|value| {
            let url = Url::parse(value.trim()).map_err(D::Error::custom)?;
            match url.scheme() {
                "http" | "https" => Ok(url),
                scheme => Err(D::Error::custom(format!(
                    "url must use http or https scheme, got {scheme}"
                ))),
            }
        })
        .transpose()
        .map(UpdateField::from_option)
}

fn deserialize_update_snowflake_account_id<'de, D>(
    deserializer: D,
) -> Result<UpdateField<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    value
        .map(|value| {
            let value = value.trim().to_owned();
            etl_config::shared::validate_snowflake_account_id(&value).map_err(D::Error::custom)?;
            Ok(value)
        })
        .transpose()
        .map(UpdateField::from_option)
}

fn deserialize_update_supabase_project_ref<'de, D>(
    deserializer: D,
) -> Result<UpdateField<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    value
        .map(|value| {
            let value = value.trim().to_owned();
            etl_config::shared::validate_supabase_project_ref(&value).map_err(D::Error::custom)?;
            Ok(value)
        })
        .transpose()
        .map(UpdateField::from_option)
}

/// Patch-style destination configuration used by update endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UpdateApiDestinationConfig {
    BigQuery {
        #[schema(example = "my-gcp-project")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        project_id: UpdateField<String>,
        #[schema(example = "my_dataset")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        dataset_id: UpdateField<String>,
        #[schema(example = "{\"type\": \"service_account\", \"project_id\": \"my-project\"}")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        service_account_key: UpdateField<SerializableSecretString>,
        #[schema(example = 15)]
        #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
        max_staleness_mins: UpdateField<u16>,
        #[schema(example = 8)]
        #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
        connection_pool_size: UpdateField<usize>,
    },
    #[serde(rename = "clickhouse")]
    ClickHouse {
        #[schema(value_type = String, example = "http://test:8123")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_http_url"
        )]
        url: UpdateField<Url>,
        #[schema(example = "foo")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        user: UpdateField<String>,
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        password: UpdateField<SerializableSecretString>,
        #[schema(example = "my_db")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        database: UpdateField<String>,
        #[schema(value_type = String, example = "replacing_merge_tree")]
        #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
        engine: UpdateField<ClickHouseEngine>,
    },
    Iceberg {
        #[serde(flatten)]
        config: UpdateApiIcebergConfig,
    },
    Ducklake {
        #[schema(value_type = String, example = "postgres://localhost:5432/ducklake_catalog")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        catalog_url: UpdateField<SerializableSecretString>,
        #[schema(example = "s3://bucket/path")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        data_path: UpdateField<String>,
        #[schema(example = 4)]
        #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
        pool_size: UpdateField<u32>,
        #[schema(example = "my-access-key")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        s3_access_key_id: UpdateField<SerializableSecretString>,
        #[schema(example = "my-secret-key")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        s3_secret_access_key: UpdateField<SerializableSecretString>,
        #[schema(example = "us-east-1")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        s3_region: UpdateField<String>,
        #[schema(example = "127.0.0.1:5000/s3")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        s3_endpoint: UpdateField<String>,
        #[schema(example = "path")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        s3_url_style: UpdateField<String>,
        #[schema(example = false)]
        #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
        s3_use_ssl: UpdateField<bool>,
        #[schema(example = "ducklake")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        metadata_schema: UpdateField<String>,
        #[schema(example = "500MB")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        maintenance_target_file_size: UpdateField<String>,
        #[schema(example = "7 days")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        expire_snapshots_older_than: UpdateField<String>,
        #[schema(example = "kubernetes")]
        #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
        maintenance_mode: UpdateField<DuckLakeMaintenanceMode>,
    },
    Snowflake {
        #[schema(example = "ORGNAME-ACCOUNTNAME")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_snowflake_account_id"
        )]
        account_id: UpdateField<String>,
        #[schema(example = "ETL_USER")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        user: UpdateField<String>,
        #[schema(example = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADA...")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        private_key: UpdateField<SerializableSecretString>,
        #[serde(default, skip_serializing_if = "UpdateField::is_preserve")]
        #[schema(value_type = Option<String>)]
        private_key_passphrase: UpdateField<SerializableSecretString>,
        #[schema(example = "ANALYTICS")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        database: UpdateField<String>,
        #[schema(example = "PUBLIC")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        schema: UpdateField<String>,
        #[schema(example = "ETL_ROLE")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        role: UpdateField<String>,
    },
}

impl UpdateApiDestinationConfig {
    /// Builds a replacement update from an API destination configuration.
    ///
    /// Optional fields that are absent in the API config are cleared.
    pub fn from_api_config(value: ApiDestinationConfig) -> Self {
        match value {
            ApiDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => Self::BigQuery {
                project_id: UpdateField::Set(project_id),
                dataset_id: UpdateField::Set(dataset_id),
                service_account_key: UpdateField::Set(service_account_key),
                max_staleness_mins: UpdateField::from_option(max_staleness_mins),
                connection_pool_size: UpdateField::from_option(connection_pool_size),
            },
            ApiDestinationConfig::ClickHouse { url, user, password, database, engine } => {
                Self::ClickHouse {
                    url: UpdateField::Set(url),
                    user: UpdateField::Set(user),
                    password: UpdateField::from_option(password),
                    database: UpdateField::Set(database),
                    engine: UpdateField::Set(engine),
                }
            }
            ApiDestinationConfig::Iceberg { config } => {
                Self::Iceberg { config: UpdateApiIcebergConfig::from_api_config(config) }
            }
            ApiDestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            } => Self::Ducklake {
                catalog_url: UpdateField::Set(catalog_url),
                data_path: UpdateField::Set(data_path),
                pool_size: UpdateField::from_option(pool_size),
                s3_access_key_id: UpdateField::from_option(s3_access_key_id),
                s3_secret_access_key: UpdateField::from_option(s3_secret_access_key),
                s3_region: UpdateField::from_option(s3_region),
                s3_endpoint: UpdateField::from_option(s3_endpoint),
                s3_url_style: UpdateField::from_option(s3_url_style),
                s3_use_ssl: UpdateField::from_option(s3_use_ssl),
                metadata_schema: UpdateField::from_option(metadata_schema),
                maintenance_target_file_size: UpdateField::from_option(
                    maintenance_target_file_size,
                ),
                expire_snapshots_older_than: UpdateField::from_option(expire_snapshots_older_than),
                maintenance_mode: UpdateField::Set(maintenance_mode),
            },
            ApiDestinationConfig::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => Self::Snowflake {
                account_id: UpdateField::Set(account_id),
                user: UpdateField::Set(user),
                private_key: UpdateField::Set(private_key),
                private_key_passphrase: UpdateField::from_option(private_key_passphrase),
                database: UpdateField::Set(database),
                schema: UpdateField::Set(schema),
                role: UpdateField::from_option(role),
            },
        }
    }

    /// Merges this update into a stored destination configuration.
    pub(crate) fn merge_into_stored(
        self,
        stored: StoredDestinationConfig,
    ) -> Result<StoredDestinationConfig, DestinationConfigUpdateError> {
        match (self, stored) {
            (
                Self::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key,
                    max_staleness_mins,
                    connection_pool_size,
                },
                StoredDestinationConfig::BigQuery {
                    project_id: stored_project_id,
                    dataset_id: stored_dataset_id,
                    service_account_key: stored_key,
                    max_staleness_mins: stored_max_staleness_mins,
                    connection_pool_size: stored_connection_pool_size,
                },
            ) => Ok(StoredDestinationConfig::BigQuery {
                project_id: apply_required_field(
                    project_id,
                    stored_project_id,
                    "BigQuery",
                    "project_id",
                )?,
                dataset_id: apply_required_field(
                    dataset_id,
                    stored_dataset_id,
                    "BigQuery",
                    "dataset_id",
                )?,
                service_account_key: apply_required_secret(
                    service_account_key,
                    stored_key,
                    "BigQuery",
                    "service_account_key",
                )?,
                max_staleness_mins: max_staleness_mins.apply_to_option(stored_max_staleness_mins),
                connection_pool_size: connection_pool_size
                    .apply_to_value(stored_connection_pool_size, default_connection_pool_size),
            }),
            (
                Self::ClickHouse { url, user, password, database, engine },
                StoredDestinationConfig::ClickHouse {
                    url: stored_url,
                    user: stored_user,
                    password: stored_password,
                    database: stored_database,
                    engine: stored_engine,
                },
            ) => Ok(StoredDestinationConfig::ClickHouse {
                url: apply_required_field(url, stored_url, "ClickHouse", "url")?,
                user: apply_required_field(user, stored_user, "ClickHouse", "user")?,
                password: password.apply_to_option(stored_password),
                database: apply_required_field(
                    database,
                    stored_database,
                    "ClickHouse",
                    "database",
                )?,
                engine: engine.apply_to_value(stored_engine, ClickHouseEngine::default),
            }),
            (
                Self::Iceberg { config },
                StoredDestinationConfig::Iceberg { config: stored_config },
            ) => config
                .merge_into_stored(stored_config)
                .map(|config| StoredDestinationConfig::Iceberg { config }),
            (
                Self::Ducklake {
                    catalog_url,
                    data_path,
                    pool_size,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                    s3_endpoint,
                    s3_url_style,
                    s3_use_ssl,
                    metadata_schema,
                    maintenance_target_file_size,
                    expire_snapshots_older_than,
                    maintenance_mode,
                },
                StoredDestinationConfig::Ducklake {
                    catalog_url: stored_catalog_url,
                    data_path: stored_data_path,
                    pool_size: stored_pool_size,
                    s3_access_key_id: stored_s3_access_key_id,
                    s3_secret_access_key: stored_s3_secret_access_key,
                    s3_region: stored_s3_region,
                    s3_endpoint: stored_s3_endpoint,
                    s3_url_style: stored_s3_url_style,
                    s3_use_ssl: stored_s3_use_ssl,
                    metadata_schema: stored_metadata_schema,
                    maintenance_target_file_size: stored_maintenance_target_file_size,
                    expire_snapshots_older_than: stored_expire_snapshots_older_than,
                    maintenance_mode: stored_maintenance_mode,
                },
            ) => Ok(StoredDestinationConfig::Ducklake {
                catalog_url: apply_required_secret(
                    catalog_url,
                    stored_catalog_url,
                    "DuckLake",
                    "catalog_url",
                )?,
                data_path: apply_required_field(
                    data_path,
                    stored_data_path,
                    "DuckLake",
                    "data_path",
                )?,
                pool_size: pool_size.apply_to_value(stored_pool_size, default_ducklake_pool_size),
                s3_access_key_id: s3_access_key_id.apply_to_option(stored_s3_access_key_id),
                s3_secret_access_key: s3_secret_access_key
                    .apply_to_option(stored_s3_secret_access_key),
                s3_region: s3_region.apply_to_option(stored_s3_region),
                s3_endpoint: s3_endpoint.apply_to_option(stored_s3_endpoint),
                s3_url_style: s3_url_style.apply_to_option(stored_s3_url_style),
                s3_use_ssl: s3_use_ssl.apply_to_option(stored_s3_use_ssl),
                metadata_schema: metadata_schema.apply_to_option(stored_metadata_schema),
                maintenance_target_file_size: maintenance_target_file_size
                    .apply_to_option(stored_maintenance_target_file_size),
                expire_snapshots_older_than: expire_snapshots_older_than
                    .apply_to_option(stored_expire_snapshots_older_than),
                maintenance_mode: maintenance_mode
                    .apply_to_value(stored_maintenance_mode, DuckLakeMaintenanceMode::default),
            }),
            (
                Self::Snowflake {
                    account_id,
                    user,
                    private_key,
                    private_key_passphrase,
                    database,
                    schema,
                    role,
                },
                StoredDestinationConfig::Snowflake {
                    account_id: stored_account_id,
                    user: stored_user,
                    private_key: stored_private_key,
                    private_key_passphrase: stored_private_key_passphrase,
                    database: stored_database,
                    schema: stored_schema,
                    role: stored_role,
                },
            ) => Ok(StoredDestinationConfig::Snowflake {
                account_id: apply_required_field(
                    account_id,
                    stored_account_id,
                    "Snowflake",
                    "account_id",
                )?,
                user: apply_required_field(user, stored_user, "Snowflake", "user")?,
                private_key: apply_required_secret(
                    private_key,
                    stored_private_key,
                    "Snowflake",
                    "private_key",
                )?,
                private_key_passphrase: private_key_passphrase
                    .apply_to_option(stored_private_key_passphrase),
                database: apply_required_field(database, stored_database, "Snowflake", "database")?,
                schema: apply_required_field(schema, stored_schema, "Snowflake", "schema")?,
                role: role.apply_to_option(stored_role),
            }),
            (config, _) => config.into_stored_requiring_secrets(),
        }
    }

    /// Restores fields that were preserved by this update from raw storage.
    pub(crate) fn restore_preserved_fields(
        &self,
        stored_config: &serde_json::Value,
        updated_config: &mut serde_json::Value,
    ) {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => {
                restore_preserved_destination_field(
                    project_id,
                    stored_config,
                    updated_config,
                    "big_query",
                    "project_id",
                );
                restore_preserved_destination_field(
                    dataset_id,
                    stored_config,
                    updated_config,
                    "big_query",
                    "dataset_id",
                );
                restore_preserved_destination_field(
                    service_account_key,
                    stored_config,
                    updated_config,
                    "big_query",
                    "service_account_key",
                );
                restore_preserved_destination_field(
                    max_staleness_mins,
                    stored_config,
                    updated_config,
                    "big_query",
                    "max_staleness_mins",
                );
                restore_preserved_destination_field(
                    connection_pool_size,
                    stored_config,
                    updated_config,
                    "big_query",
                    "connection_pool_size",
                );
            }
            Self::ClickHouse { url, user, password, database, engine } => {
                restore_preserved_destination_field(
                    url,
                    stored_config,
                    updated_config,
                    "click_house",
                    "url",
                );
                restore_preserved_destination_field(
                    user,
                    stored_config,
                    updated_config,
                    "click_house",
                    "user",
                );
                restore_preserved_destination_field(
                    password,
                    stored_config,
                    updated_config,
                    "click_house",
                    "password",
                );
                restore_preserved_destination_field(
                    database,
                    stored_config,
                    updated_config,
                    "click_house",
                    "database",
                );
                restore_preserved_destination_field(
                    engine,
                    stored_config,
                    updated_config,
                    "click_house",
                    "engine",
                );
            }
            Self::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            } => {
                restore_preserved_destination_field(
                    catalog_url,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "catalog_url",
                );
                restore_preserved_destination_field(
                    data_path,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "data_path",
                );
                restore_preserved_destination_field(
                    pool_size,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "pool_size",
                );
                restore_preserved_destination_field(
                    s3_access_key_id,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "s3_access_key_id",
                );
                restore_preserved_destination_field(
                    s3_secret_access_key,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "s3_secret_access_key",
                );
                restore_preserved_destination_field(
                    s3_region,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "s3_region",
                );
                restore_preserved_destination_field(
                    s3_endpoint,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "s3_endpoint",
                );
                restore_preserved_destination_field(
                    s3_url_style,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "s3_url_style",
                );
                restore_preserved_destination_field(
                    s3_use_ssl,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "s3_use_ssl",
                );
                restore_preserved_destination_field(
                    metadata_schema,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "metadata_schema",
                );
                restore_preserved_destination_field(
                    maintenance_target_file_size,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "maintenance_target_file_size",
                );
                restore_preserved_destination_field(
                    expire_snapshots_older_than,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "expire_snapshots_older_than",
                );
                restore_preserved_destination_field(
                    maintenance_mode,
                    stored_config,
                    updated_config,
                    "ducklake",
                    "maintenance_mode",
                );
            }
            Self::Iceberg { config } => {
                config.restore_preserved_fields(stored_config, updated_config);
            }
            Self::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => {
                restore_preserved_destination_field(
                    account_id,
                    stored_config,
                    updated_config,
                    "snowflake",
                    "account_id",
                );
                restore_preserved_destination_field(
                    user,
                    stored_config,
                    updated_config,
                    "snowflake",
                    "user",
                );
                restore_preserved_destination_field(
                    private_key,
                    stored_config,
                    updated_config,
                    "snowflake",
                    "private_key",
                );
                restore_preserved_destination_field(
                    private_key_passphrase,
                    stored_config,
                    updated_config,
                    "snowflake",
                    "private_key_passphrase",
                );
                restore_preserved_destination_field(
                    database,
                    stored_config,
                    updated_config,
                    "snowflake",
                    "database",
                );
                restore_preserved_destination_field(
                    schema,
                    stored_config,
                    updated_config,
                    "snowflake",
                    "schema",
                );
                restore_preserved_destination_field(
                    role,
                    stored_config,
                    updated_config,
                    "snowflake",
                    "role",
                );
            }
        }
    }

    /// Converts this update into a new stored configuration, requiring secrets
    /// that cannot be preserved from an existing destination of the same kind.
    fn into_stored_requiring_secrets(
        self,
    ) -> Result<StoredDestinationConfig, DestinationConfigUpdateError> {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => Ok(StoredDestinationConfig::BigQuery {
                project_id: require_field(project_id, "BigQuery", "project_id")?,
                dataset_id: require_field(dataset_id, "BigQuery", "dataset_id")?,
                service_account_key: require_secret_update(
                    service_account_key,
                    "BigQuery",
                    "service_account_key",
                )?,
                max_staleness_mins: max_staleness_mins.into_option(),
                connection_pool_size: connection_pool_size
                    .apply_to_value(default_connection_pool_size(), default_connection_pool_size),
            }),
            Self::ClickHouse { url, user, password, database, engine } => {
                Ok(StoredDestinationConfig::ClickHouse {
                    url: require_field(url, "ClickHouse", "url")?,
                    user: require_field(user, "ClickHouse", "user")?,
                    password: password.into_option(),
                    database: require_field(database, "ClickHouse", "database")?,
                    engine: engine
                        .apply_to_value(ClickHouseEngine::default(), ClickHouseEngine::default),
                })
            }
            Self::Iceberg { config } => config
                .into_stored_requiring_secrets()
                .map(|config| StoredDestinationConfig::Iceberg { config }),
            Self::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            } => Ok(StoredDestinationConfig::Ducklake {
                catalog_url: require_secret_update(catalog_url, "DuckLake", "catalog_url")?,
                data_path: require_field(data_path, "DuckLake", "data_path")?,
                pool_size: pool_size
                    .apply_to_value(default_ducklake_pool_size(), default_ducklake_pool_size),
                s3_access_key_id: s3_access_key_id.into_option(),
                s3_secret_access_key: s3_secret_access_key.into_option(),
                s3_region: s3_region.into_option(),
                s3_endpoint: s3_endpoint.into_option(),
                s3_url_style: s3_url_style.into_option(),
                s3_use_ssl: s3_use_ssl.into_option(),
                metadata_schema: metadata_schema.into_option(),
                maintenance_target_file_size: maintenance_target_file_size.into_option(),
                expire_snapshots_older_than: expire_snapshots_older_than.into_option(),
                maintenance_mode: maintenance_mode.apply_to_value(
                    DuckLakeMaintenanceMode::default(),
                    DuckLakeMaintenanceMode::default,
                ),
            }),
            Self::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => Ok(StoredDestinationConfig::Snowflake {
                account_id: require_field(account_id, "Snowflake", "account_id")?,
                user: require_field(user, "Snowflake", "user")?,
                private_key: require_secret_update(private_key, "Snowflake", "private_key")?,
                private_key_passphrase: private_key_passphrase.into_option(),
                database: require_field(database, "Snowflake", "database")?,
                schema: require_field(schema, "Snowflake", "schema")?,
                role: role.into_option(),
            }),
        }
    }
}

fn restore_preserved_destination_field<T>(
    field: &UpdateField<T>,
    stored_config: &serde_json::Value,
    updated_config: &mut serde_json::Value,
    variant: &str,
    field_name: &str,
) {
    let Some(updated_variant) =
        updated_config.as_object_mut().and_then(|object| object.get_mut(variant))
    else {
        return;
    };

    if let Some(stored_variant) = stored_config.as_object().and_then(|object| object.get(variant)) {
        field.restore_preserved_value(stored_variant, updated_variant, field_name);
    } else {
        field.restore_preserved_value(&serde_json::Value::Null, updated_variant, field_name);
    }
}

fn apply_required_field<T>(
    update: UpdateField<T>,
    stored: T,
    destination: &'static str,
    field: &'static str,
) -> Result<T, DestinationConfigUpdateError> {
    match update {
        UpdateField::Preserve => Ok(stored),
        UpdateField::Clear => {
            Err(DestinationConfigUpdateError::RequiredFieldCleared { destination, field })
        }
        UpdateField::Set(value) => Ok(value),
    }
}

fn apply_required_secret<T>(
    update: UpdateField<T>,
    stored: T,
    destination: &'static str,
    field: &'static str,
) -> Result<T, DestinationConfigUpdateError> {
    match update {
        UpdateField::Preserve => Ok(stored),
        UpdateField::Clear => {
            Err(DestinationConfigUpdateError::RequiredFieldCleared { destination, field })
        }
        UpdateField::Set(value) => Ok(value),
    }
}

fn require_field<T>(
    value: UpdateField<T>,
    destination: &'static str,
    field: &'static str,
) -> Result<T, DestinationConfigUpdateError> {
    match value {
        UpdateField::Set(value) => Ok(value),
        UpdateField::Preserve => {
            Err(DestinationConfigUpdateError::MissingRequiredField { destination, field })
        }
        UpdateField::Clear => {
            Err(DestinationConfigUpdateError::RequiredFieldCleared { destination, field })
        }
    }
}

fn require_secret_update<T>(
    value: UpdateField<T>,
    destination: &'static str,
    field: &'static str,
) -> Result<T, DestinationConfigUpdateError> {
    match value {
        UpdateField::Set(value) => Ok(value),
        UpdateField::Preserve => {
            Err(DestinationConfigUpdateError::MissingRequiredSecret { destination, field })
        }
        UpdateField::Clear => {
            Err(DestinationConfigUpdateError::RequiredFieldCleared { destination, field })
        }
    }
}

impl From<StoredDestinationConfig> for ApiDestinationConfig {
    fn from(value: StoredDestinationConfig) -> Self {
        match value {
            StoredDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size: Some(connection_pool_size),
            },
            StoredDestinationConfig::ClickHouse { url, user, password, database, engine } => {
                Self::ClickHouse { url, user, password, database, engine }
            }
            StoredDestinationConfig::Iceberg { config } => match config {
                StoredIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                } => ApiDestinationConfig::Iceberg {
                    config: ApiIcebergConfig::Supabase {
                        project_ref,
                        warehouse_name,
                        namespace,
                        catalog_token,
                        s3_access_key_id,
                        s3_secret_access_key,
                        s3_region,
                    },
                },
                StoredIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_endpoint,
                } => ApiDestinationConfig::Iceberg {
                    config: ApiIcebergConfig::Rest {
                        catalog_uri,
                        warehouse_name,
                        namespace,
                        s3_endpoint,
                        s3_access_key_id,
                        s3_secret_access_key,
                    },
                },
            },
            StoredDestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            } => Self::Ducklake {
                catalog_url,
                data_path,
                pool_size: Some(pool_size),
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            },
            StoredDestinationConfig::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => Self::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum StoredDestinationConfig {
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: SerializableSecretString,
        max_staleness_mins: Option<u16>,
        connection_pool_size: usize,
    },
    ClickHouse {
        url: Url,
        user: String,
        password: Option<SerializableSecretString>,
        database: String,
        engine: ClickHouseEngine,
    },
    Iceberg {
        config: StoredIcebergConfig,
    },
    Ducklake {
        catalog_url: SerializableSecretString,
        data_path: String,
        pool_size: u32,
        s3_access_key_id: Option<SerializableSecretString>,
        s3_secret_access_key: Option<SerializableSecretString>,
        s3_region: Option<String>,
        s3_endpoint: Option<String>,
        s3_url_style: Option<String>,
        s3_use_ssl: Option<bool>,
        metadata_schema: Option<String>,
        maintenance_target_file_size: Option<String>,
        expire_snapshots_older_than: Option<String>,
        maintenance_mode: DuckLakeMaintenanceMode,
    },
    Snowflake {
        account_id: String,
        user: String,
        private_key: SerializableSecretString,
        private_key_passphrase: Option<SerializableSecretString>,
        database: String,
        schema: String,
        role: Option<String>,
    },
}

impl StoredDestinationConfig {
    pub fn into_etl_config(self) -> DestinationConfig {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key: service_account_key.into(),
                max_staleness_mins,
                connection_pool_size,
            },
            Self::ClickHouse { url, user, password, database, engine } => {
                DestinationConfig::ClickHouse {
                    url,
                    user,
                    password: password.map(Into::into),
                    database,
                    engine,
                }
            }
            Self::Iceberg { config } => match config {
                StoredIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                } => DestinationConfig::Iceberg {
                    config: IcebergConfig::Supabase {
                        project_ref,
                        warehouse_name,
                        namespace,
                        catalog_token: catalog_token.into(),
                        s3_access_key_id: s3_access_key_id.into(),
                        s3_secret_access_key: s3_secret_access_key.into(),
                        s3_region,
                    },
                },
                StoredIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_endpoint,
                } => DestinationConfig::Iceberg {
                    config: IcebergConfig::Rest {
                        catalog_uri,
                        warehouse_name,
                        namespace,
                        s3_access_key_id: s3_access_key_id.into(),
                        s3_secret_access_key: s3_secret_access_key.into(),
                        s3_endpoint,
                    },
                },
            },
            Self::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            } => DestinationConfig::Ducklake {
                catalog_url: catalog_url.into(),
                data_path,
                pool_size,
                s3_access_key_id: s3_access_key_id.map(Into::into),
                s3_secret_access_key: s3_secret_access_key.map(Into::into),
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            },
            Self::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => DestinationConfig::Snowflake {
                account_id,
                user,
                private_key: private_key.into(),
                private_key_passphrase: private_key_passphrase.map(Into::into),
                database,
                schema,
                role,
            },
        }
    }
}

impl From<ApiDestinationConfig> for StoredDestinationConfig {
    fn from(value: ApiDestinationConfig) -> Self {
        match value {
            ApiDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size: connection_pool_size
                    .unwrap_or(DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE),
            },
            ApiDestinationConfig::ClickHouse { url, user, password, database, engine } => {
                Self::ClickHouse { url, user, password, database, engine }
            }
            ApiDestinationConfig::Iceberg { config } => match config {
                ApiIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                } => Self::Iceberg {
                    config: StoredIcebergConfig::Supabase {
                        project_ref,
                        warehouse_name,
                        namespace,
                        catalog_token,
                        s3_access_key_id,
                        s3_secret_access_key,
                        s3_region,
                    },
                },
                ApiIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_endpoint,
                    s3_access_key_id,
                    s3_secret_access_key,
                } => Self::Iceberg {
                    config: StoredIcebergConfig::Rest {
                        catalog_uri,
                        warehouse_name,
                        namespace,
                        s3_access_key_id,
                        s3_secret_access_key,
                        s3_endpoint,
                    },
                },
            },
            ApiDestinationConfig::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            } => Self::Ducklake {
                catalog_url,
                data_path,
                pool_size: pool_size.unwrap_or(DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE),
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            },
            ApiDestinationConfig::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => Self::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            },
        }
    }
}

impl Encrypt<EncryptedStoredDestinationConfig> for StoredDestinationConfig {
    fn encrypt(
        self,
        encryption_key: &EncryptionKeyring,
    ) -> Result<EncryptedStoredDestinationConfig, EncryptionError> {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => {
                let encrypted_service_account_key =
                    encrypt_text(service_account_key.expose_secret().to_owned(), encryption_key)?;

                Ok(EncryptedStoredDestinationConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key: encrypted_service_account_key,
                    max_staleness_mins,
                    connection_pool_size,
                })
            }
            Self::ClickHouse { url, user, password, database, engine } => {
                let encrypted_password = password
                    .map(|p| encrypt_text(p.expose_secret().to_owned(), encryption_key))
                    .transpose()?;

                Ok(EncryptedStoredDestinationConfig::ClickHouse {
                    url,
                    user,
                    password: encrypted_password,
                    database,
                    engine,
                })
            }
            Self::Iceberg { config } => match config {
                StoredIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                } => {
                    let encrypted_catalog_token =
                        encrypt_text(catalog_token.expose_secret().to_owned(), encryption_key)?;
                    let encrypted_s3_access_key_id =
                        encrypt_text(s3_access_key_id.expose_secret().to_owned(), encryption_key)?;
                    let encrypted_s3_secret_access_key = encrypt_text(
                        s3_secret_access_key.expose_secret().to_owned(),
                        encryption_key,
                    )?;
                    Ok(EncryptedStoredDestinationConfig::Iceberg {
                        config: EncryptedStoredIcebergConfig::Supabase {
                            project_ref,
                            warehouse_name,
                            namespace,
                            catalog_token: encrypted_catalog_token,
                            s3_access_key_id: encrypted_s3_access_key_id,
                            s3_secret_access_key: encrypted_s3_secret_access_key,
                            s3_region,
                        },
                    })
                }
                StoredIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_endpoint,
                } => {
                    let encrypted_s3_access_key_id =
                        encrypt_text(s3_access_key_id.expose_secret().to_owned(), encryption_key)?;
                    let encrypted_s3_secret_access_key = encrypt_text(
                        s3_secret_access_key.expose_secret().to_owned(),
                        encryption_key,
                    )?;
                    Ok(EncryptedStoredDestinationConfig::Iceberg {
                        config: EncryptedStoredIcebergConfig::Rest {
                            catalog_uri,
                            warehouse_name,
                            namespace,
                            s3_access_key_id: encrypted_s3_access_key_id,
                            s3_secret_access_key: encrypted_s3_secret_access_key,
                            s3_endpoint,
                        },
                    })
                }
            },
            Self::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            } => {
                let encrypted_catalog_url =
                    encrypt_text(catalog_url.expose_secret().to_owned(), encryption_key)?;
                let s3_access_key_id = s3_access_key_id
                    .map(|value| encrypt_text(value.expose_secret().to_owned(), encryption_key))
                    .transpose()?;
                let s3_secret_access_key = s3_secret_access_key
                    .map(|value| encrypt_text(value.expose_secret().to_owned(), encryption_key))
                    .transpose()?;

                Ok(EncryptedStoredDestinationConfig::Ducklake {
                    catalog_url: encrypted_catalog_url,
                    data_path,
                    pool_size,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                    s3_endpoint,
                    s3_url_style,
                    s3_use_ssl,
                    metadata_schema,
                    maintenance_target_file_size,
                    expire_snapshots_older_than,
                    maintenance_mode,
                })
            }
            Self::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => {
                let encrypted_private_key =
                    encrypt_text(private_key.expose_secret().to_owned(), encryption_key)?;
                let encrypted_private_key_passphrase = private_key_passphrase
                    .map(|p| encrypt_text(p.expose_secret().to_owned(), encryption_key))
                    .transpose()?;

                Ok(EncryptedStoredDestinationConfig::Snowflake {
                    account_id,
                    user,
                    private_key: encrypted_private_key,
                    private_key_passphrase: encrypted_private_key_passphrase,
                    database,
                    schema,
                    role,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedStoredDestinationConfig {
    BigQuery {
        project_id: String,
        dataset_id: String,
        service_account_key: EncryptedValue,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
        #[serde(default = "default_connection_pool_size")]
        connection_pool_size: usize,
    },
    ClickHouse {
        url: Url,
        user: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        password: Option<EncryptedValue>,
        database: String,
        #[serde(default)]
        engine: ClickHouseEngine,
    },
    Iceberg {
        #[serde(flatten)]
        config: EncryptedStoredIcebergConfig,
    },
    Ducklake {
        catalog_url: EncryptedValue,
        data_path: String,
        #[serde(default = "default_ducklake_pool_size")]
        pool_size: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        s3_access_key_id: Option<EncryptedValue>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        s3_secret_access_key: Option<EncryptedValue>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        s3_region: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        s3_endpoint: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        s3_url_style: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        s3_use_ssl: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata_schema: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        maintenance_target_file_size: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expire_snapshots_older_than: Option<String>,
        #[serde(default)]
        maintenance_mode: DuckLakeMaintenanceMode,
    },
    Snowflake {
        account_id: String,
        user: String,
        private_key: EncryptedValue,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        private_key_passphrase: Option<EncryptedValue>,
        database: String,
        schema: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role: Option<String>,
    },
}

impl Store for EncryptedStoredDestinationConfig {}

impl Decrypt<StoredDestinationConfig> for EncryptedStoredDestinationConfig {
    fn decrypt(
        self,
        encryption_key: &EncryptionKeyring,
    ) -> Result<StoredDestinationConfig, DecryptionError> {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: encrypted_service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => {
                let service_account_key = SerializableSecretString::from(decrypt_text(
                    encrypted_service_account_key,
                    encryption_key,
                )?);

                Ok(StoredDestinationConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key,
                    max_staleness_mins,
                    connection_pool_size,
                })
            }
            Self::Iceberg { config } => match config {
                EncryptedStoredIcebergConfig::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token: encrypted_catalog_token,
                    s3_access_key_id: encrypted_s3_access_key_id,
                    s3_secret_access_key: encrypted_s3_secret_access_key,
                    s3_region,
                } => {
                    let catalog_token = SerializableSecretString::from(decrypt_text(
                        encrypted_catalog_token,
                        encryption_key,
                    )?);

                    let s3_access_key_id = SerializableSecretString::from(decrypt_text(
                        encrypted_s3_access_key_id,
                        encryption_key,
                    )?);

                    let s3_secret_access_key = SerializableSecretString::from(decrypt_text(
                        encrypted_s3_secret_access_key,
                        encryption_key,
                    )?);

                    Ok(StoredDestinationConfig::Iceberg {
                        config: StoredIcebergConfig::Supabase {
                            project_ref,
                            warehouse_name,
                            namespace,
                            catalog_token,
                            s3_access_key_id,
                            s3_secret_access_key,
                            s3_region,
                        },
                    })
                }
                EncryptedStoredIcebergConfig::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id: encrypted_s3_access_key_id,
                    s3_secret_access_key: encrypted_s3_secret_access_key,
                    s3_endpoint,
                } => {
                    let s3_access_key_id = SerializableSecretString::from(decrypt_text(
                        encrypted_s3_access_key_id,
                        encryption_key,
                    )?);

                    let s3_secret_access_key = SerializableSecretString::from(decrypt_text(
                        encrypted_s3_secret_access_key,
                        encryption_key,
                    )?);

                    Ok(StoredDestinationConfig::Iceberg {
                        config: StoredIcebergConfig::Rest {
                            catalog_uri,
                            warehouse_name,
                            namespace,
                            s3_access_key_id,
                            s3_secret_access_key,
                            s3_endpoint,
                        },
                    })
                }
            },
            EncryptedStoredDestinationConfig::ClickHouse {
                url,
                user,
                password,
                database,
                engine,
            } => {
                let password = password
                    .map(|p| decrypt_text(p, encryption_key))
                    .transpose()?
                    .map(SerializableSecretString::from);

                Ok(StoredDestinationConfig::ClickHouse { url, user, password, database, engine })
            }
            Self::Ducklake {
                catalog_url,
                data_path,
                pool_size,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            } => Ok(StoredDestinationConfig::Ducklake {
                catalog_url: SerializableSecretString::from(decrypt_text(
                    catalog_url,
                    encryption_key,
                )?),
                data_path,
                pool_size,
                s3_access_key_id: s3_access_key_id
                    .map(|value| {
                        decrypt_text(value, encryption_key).map(SerializableSecretString::from)
                    })
                    .transpose()?,
                s3_secret_access_key: s3_secret_access_key
                    .map(|value| {
                        decrypt_text(value, encryption_key).map(SerializableSecretString::from)
                    })
                    .transpose()?,
                s3_region,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
                metadata_schema,
                maintenance_target_file_size,
                expire_snapshots_older_than,
                maintenance_mode,
            }),
            Self::Snowflake {
                account_id,
                user,
                private_key,
                private_key_passphrase,
                database,
                schema,
                role,
            } => {
                let private_key =
                    SerializableSecretString::from(decrypt_text(private_key, encryption_key)?);
                let private_key_passphrase = private_key_passphrase
                    .map(|p| decrypt_text(p, encryption_key))
                    .transpose()?
                    .map(SerializableSecretString::from);

                Ok(StoredDestinationConfig::Snowflake {
                    account_id,
                    user,
                    private_key,
                    private_key_passphrase,
                    database,
                    schema,
                    role,
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum StoredIcebergConfig {
    Supabase {
        project_ref: String,
        warehouse_name: String,
        namespace: Option<String>,
        catalog_token: SerializableSecretString,
        s3_access_key_id: SerializableSecretString,
        s3_secret_access_key: SerializableSecretString,
        s3_region: String,
    },
    Rest {
        catalog_uri: String,
        warehouse_name: String,
        namespace: Option<String>,
        s3_access_key_id: SerializableSecretString,
        s3_secret_access_key: SerializableSecretString,
        s3_endpoint: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiIcebergConfig {
    Supabase {
        #[schema(example = "abcdefghijklmnopqrst")]
        #[serde(deserialize_with = "crate::utils::trim_supabase_project_ref")]
        project_ref: String,
        #[schema(example = "my-warehouse")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        warehouse_name: String,
        #[schema(example = "my-namespace")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        namespace: Option<String>,
        #[schema(
            example = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ"
        )]
        catalog_token: SerializableSecretString,
        #[schema(example = "9156667efc2c70d89af6588da86d2924")]
        s3_access_key_id: SerializableSecretString,
        #[schema(example = "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4")]
        s3_secret_access_key: SerializableSecretString,
        #[schema(example = "ap-southeast-1")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        s3_region: String,
    },
    Rest {
        #[schema(example = "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        catalog_uri: String,
        #[schema(example = "my-warehouse")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        warehouse_name: String,
        #[schema(example = "my-namespace")]
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::utils::trim_option_string"
        )]
        namespace: Option<String>,
        #[schema(example = "9156667efc2c70d89af6588da86d2924")]
        s3_access_key_id: SerializableSecretString,
        #[schema(example = "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4")]
        s3_secret_access_key: SerializableSecretString,
        #[schema(example = "https://s3.endpoint")]
        #[serde(deserialize_with = "crate::utils::trim_string")]
        s3_endpoint: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UpdateApiIcebergConfig {
    Supabase {
        #[schema(example = "abcdefghijklmnopqrst")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_supabase_project_ref"
        )]
        project_ref: UpdateField<String>,
        #[schema(example = "my-warehouse")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        warehouse_name: UpdateField<String>,
        #[schema(example = "my-namespace")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        namespace: UpdateField<String>,
        #[schema(
            example = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ"
        )]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        catalog_token: UpdateField<SerializableSecretString>,
        #[schema(example = "9156667efc2c70d89af6588da86d2924")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        s3_access_key_id: UpdateField<SerializableSecretString>,
        #[schema(example = "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        s3_secret_access_key: UpdateField<SerializableSecretString>,
        #[schema(example = "ap-southeast-1")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        s3_region: UpdateField<String>,
    },
    Rest {
        #[schema(example = "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        catalog_uri: UpdateField<String>,
        #[schema(example = "my-warehouse")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        warehouse_name: UpdateField<String>,
        #[schema(example = "my-namespace")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        namespace: UpdateField<String>,
        #[schema(example = "9156667efc2c70d89af6588da86d2924")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        s3_access_key_id: UpdateField<SerializableSecretString>,
        #[schema(example = "ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_secret_string"
        )]
        s3_secret_access_key: UpdateField<SerializableSecretString>,
        #[schema(example = "https://s3.endpoint")]
        #[serde(
            default,
            skip_serializing_if = "UpdateField::is_preserve",
            deserialize_with = "deserialize_update_trimmed_string"
        )]
        s3_endpoint: UpdateField<String>,
    },
}

impl UpdateApiIcebergConfig {
    /// Builds a replacement update from an API Iceberg configuration.
    ///
    /// Optional fields that are absent in the API config are cleared.
    fn from_api_config(value: ApiIcebergConfig) -> Self {
        match value {
            ApiIcebergConfig::Supabase {
                project_ref,
                warehouse_name,
                namespace,
                catalog_token,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
            } => Self::Supabase {
                project_ref: UpdateField::Set(project_ref),
                warehouse_name: UpdateField::Set(warehouse_name),
                namespace: UpdateField::from_option(namespace),
                catalog_token: UpdateField::Set(catalog_token),
                s3_access_key_id: UpdateField::Set(s3_access_key_id),
                s3_secret_access_key: UpdateField::Set(s3_secret_access_key),
                s3_region: UpdateField::Set(s3_region),
            },
            ApiIcebergConfig::Rest {
                catalog_uri,
                warehouse_name,
                namespace,
                s3_access_key_id,
                s3_secret_access_key,
                s3_endpoint,
            } => Self::Rest {
                catalog_uri: UpdateField::Set(catalog_uri),
                warehouse_name: UpdateField::Set(warehouse_name),
                namespace: UpdateField::from_option(namespace),
                s3_access_key_id: UpdateField::Set(s3_access_key_id),
                s3_secret_access_key: UpdateField::Set(s3_secret_access_key),
                s3_endpoint: UpdateField::Set(s3_endpoint),
            },
        }
    }

    /// Restores fields that were preserved by this update from raw storage.
    fn restore_preserved_fields(
        &self,
        stored_config: &serde_json::Value,
        updated_config: &mut serde_json::Value,
    ) {
        match self {
            Self::Supabase {
                project_ref,
                warehouse_name,
                namespace,
                catalog_token,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
            } => {
                restore_preserved_iceberg_field(
                    project_ref,
                    stored_config,
                    updated_config,
                    "supabase",
                    "project_ref",
                );
                restore_preserved_iceberg_field(
                    warehouse_name,
                    stored_config,
                    updated_config,
                    "supabase",
                    "warehouse_name",
                );
                restore_preserved_iceberg_field(
                    namespace,
                    stored_config,
                    updated_config,
                    "supabase",
                    "namespace",
                );
                restore_preserved_iceberg_field(
                    catalog_token,
                    stored_config,
                    updated_config,
                    "supabase",
                    "catalog_token",
                );
                restore_preserved_iceberg_field(
                    s3_access_key_id,
                    stored_config,
                    updated_config,
                    "supabase",
                    "s3_access_key_id",
                );
                restore_preserved_iceberg_field(
                    s3_secret_access_key,
                    stored_config,
                    updated_config,
                    "supabase",
                    "s3_secret_access_key",
                );
                restore_preserved_iceberg_field(
                    s3_region,
                    stored_config,
                    updated_config,
                    "supabase",
                    "s3_region",
                );
            }
            Self::Rest {
                catalog_uri,
                warehouse_name,
                namespace,
                s3_access_key_id,
                s3_secret_access_key,
                s3_endpoint,
            } => {
                restore_preserved_iceberg_field(
                    catalog_uri,
                    stored_config,
                    updated_config,
                    "rest",
                    "catalog_uri",
                );
                restore_preserved_iceberg_field(
                    warehouse_name,
                    stored_config,
                    updated_config,
                    "rest",
                    "warehouse_name",
                );
                restore_preserved_iceberg_field(
                    namespace,
                    stored_config,
                    updated_config,
                    "rest",
                    "namespace",
                );
                restore_preserved_iceberg_field(
                    s3_access_key_id,
                    stored_config,
                    updated_config,
                    "rest",
                    "s3_access_key_id",
                );
                restore_preserved_iceberg_field(
                    s3_secret_access_key,
                    stored_config,
                    updated_config,
                    "rest",
                    "s3_secret_access_key",
                );
                restore_preserved_iceberg_field(
                    s3_endpoint,
                    stored_config,
                    updated_config,
                    "rest",
                    "s3_endpoint",
                );
            }
        }
    }

    fn merge_into_stored(
        self,
        stored: StoredIcebergConfig,
    ) -> Result<StoredIcebergConfig, DestinationConfigUpdateError> {
        match (self, stored) {
            (
                Self::Supabase {
                    project_ref,
                    warehouse_name,
                    namespace,
                    catalog_token,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_region,
                },
                StoredIcebergConfig::Supabase {
                    project_ref: stored_project_ref,
                    warehouse_name: stored_warehouse_name,
                    namespace: stored_namespace,
                    catalog_token: stored_catalog_token,
                    s3_access_key_id: stored_s3_access_key_id,
                    s3_secret_access_key: stored_s3_secret_access_key,
                    s3_region: stored_s3_region,
                },
            ) => Ok(StoredIcebergConfig::Supabase {
                project_ref: apply_required_field(
                    project_ref,
                    stored_project_ref,
                    "Iceberg",
                    "project_ref",
                )?,
                warehouse_name: apply_required_field(
                    warehouse_name,
                    stored_warehouse_name,
                    "Iceberg",
                    "warehouse_name",
                )?,
                namespace: namespace.apply_to_option(stored_namespace),
                catalog_token: apply_required_secret(
                    catalog_token,
                    stored_catalog_token,
                    "Iceberg",
                    "catalog_token",
                )?,
                s3_access_key_id: apply_required_secret(
                    s3_access_key_id,
                    stored_s3_access_key_id,
                    "Iceberg",
                    "s3_access_key_id",
                )?,
                s3_secret_access_key: apply_required_secret(
                    s3_secret_access_key,
                    stored_s3_secret_access_key,
                    "Iceberg",
                    "s3_secret_access_key",
                )?,
                s3_region: apply_required_field(
                    s3_region,
                    stored_s3_region,
                    "Iceberg",
                    "s3_region",
                )?,
            }),
            (
                Self::Rest {
                    catalog_uri,
                    warehouse_name,
                    namespace,
                    s3_access_key_id,
                    s3_secret_access_key,
                    s3_endpoint,
                },
                StoredIcebergConfig::Rest {
                    catalog_uri: stored_catalog_uri,
                    warehouse_name: stored_warehouse_name,
                    namespace: stored_namespace,
                    s3_access_key_id: stored_s3_access_key_id,
                    s3_secret_access_key: stored_s3_secret_access_key,
                    s3_endpoint: stored_s3_endpoint,
                },
            ) => Ok(StoredIcebergConfig::Rest {
                catalog_uri: apply_required_field(
                    catalog_uri,
                    stored_catalog_uri,
                    "Iceberg",
                    "catalog_uri",
                )?,
                warehouse_name: apply_required_field(
                    warehouse_name,
                    stored_warehouse_name,
                    "Iceberg",
                    "warehouse_name",
                )?,
                namespace: namespace.apply_to_option(stored_namespace),
                s3_access_key_id: apply_required_secret(
                    s3_access_key_id,
                    stored_s3_access_key_id,
                    "Iceberg",
                    "s3_access_key_id",
                )?,
                s3_secret_access_key: apply_required_secret(
                    s3_secret_access_key,
                    stored_s3_secret_access_key,
                    "Iceberg",
                    "s3_secret_access_key",
                )?,
                s3_endpoint: apply_required_field(
                    s3_endpoint,
                    stored_s3_endpoint,
                    "Iceberg",
                    "s3_endpoint",
                )?,
            }),
            (config, _) => config.into_stored_requiring_secrets(),
        }
    }

    fn into_stored_requiring_secrets(
        self,
    ) -> Result<StoredIcebergConfig, DestinationConfigUpdateError> {
        match self {
            Self::Supabase {
                project_ref,
                warehouse_name,
                namespace,
                catalog_token,
                s3_access_key_id,
                s3_secret_access_key,
                s3_region,
            } => Ok(StoredIcebergConfig::Supabase {
                project_ref: require_field(project_ref, "Iceberg", "project_ref")?,
                warehouse_name: require_field(warehouse_name, "Iceberg", "warehouse_name")?,
                namespace: namespace.into_option(),
                catalog_token: require_secret_update(catalog_token, "Iceberg", "catalog_token")?,
                s3_access_key_id: require_secret_update(
                    s3_access_key_id,
                    "Iceberg",
                    "s3_access_key_id",
                )?,
                s3_secret_access_key: require_secret_update(
                    s3_secret_access_key,
                    "Iceberg",
                    "s3_secret_access_key",
                )?,
                s3_region: require_field(s3_region, "Iceberg", "s3_region")?,
            }),
            Self::Rest {
                catalog_uri,
                warehouse_name,
                namespace,
                s3_access_key_id,
                s3_secret_access_key,
                s3_endpoint,
            } => Ok(StoredIcebergConfig::Rest {
                catalog_uri: require_field(catalog_uri, "Iceberg", "catalog_uri")?,
                warehouse_name: require_field(warehouse_name, "Iceberg", "warehouse_name")?,
                namespace: namespace.into_option(),
                s3_access_key_id: require_secret_update(
                    s3_access_key_id,
                    "Iceberg",
                    "s3_access_key_id",
                )?,
                s3_secret_access_key: require_secret_update(
                    s3_secret_access_key,
                    "Iceberg",
                    "s3_secret_access_key",
                )?,
                s3_endpoint: require_field(s3_endpoint, "Iceberg", "s3_endpoint")?,
            }),
        }
    }
}

/// Restores one preserved Iceberg field from raw storage.
fn restore_preserved_iceberg_field<T>(
    field: &UpdateField<T>,
    stored_config: &serde_json::Value,
    updated_config: &mut serde_json::Value,
    variant: &str,
    field_name: &str,
) {
    let Some(updated_variant) = updated_config
        .as_object_mut()
        .and_then(|object| object.get_mut("iceberg"))
        .and_then(|iceberg| iceberg.as_object_mut())
        .and_then(|iceberg| iceberg.get_mut(variant))
    else {
        return;
    };

    if let Some(stored_variant) = stored_config
        .as_object()
        .and_then(|object| object.get("iceberg"))
        .and_then(|iceberg| iceberg.as_object())
        .and_then(|iceberg| iceberg.get(variant))
    {
        field.restore_preserved_value(stored_variant, updated_variant, field_name);
    } else {
        field.restore_preserved_value(&serde_json::Value::Null, updated_variant, field_name);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedStoredIcebergConfig {
    Supabase {
        project_ref: String,
        warehouse_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
        catalog_token: EncryptedValue,
        s3_access_key_id: EncryptedValue,
        s3_secret_access_key: EncryptedValue,
        s3_region: String,
    },
    Rest {
        catalog_uri: String,
        warehouse_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        namespace: Option<String>,
        s3_access_key_id: EncryptedValue,
        s3_secret_access_key: EncryptedValue,
        s3_endpoint: String,
    },
}

#[cfg(test)]
mod tests {
    use insta::assert_json_snapshot;

    use super::*;
    use crate::configs::encryption::{
        EncryptionKey, EncryptionKeyring, encrypt_text, generate_random_key,
    };

    #[test]
    fn stored_destination_config_encryption_decryption_bigquery() {
        let service_account_key_plaintext = "{\"test\": \"key\"}";
        let config = StoredDestinationConfig::BigQuery {
            project_id: "test-project".to_owned(),
            dataset_id: "test_dataset".to_owned(),
            service_account_key: SerializableSecretString::from(
                service_account_key_plaintext.to_owned(),
            ),
            max_staleness_mins: Some(15),
            connection_pool_size: 8,
        };

        let key = EncryptionKeyring::from(EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        });

        let encrypted = config.clone().encrypt(&key).unwrap();
        let encrypted_json = serde_json::to_string(&encrypted).unwrap();
        assert!(!encrypted_json.contains(r#"{\"test\": \"key\"}"#));
        match &encrypted {
            EncryptedStoredDestinationConfig::BigQuery { service_account_key, .. } => {
                assert_ne!(service_account_key.value, service_account_key_plaintext);
            }
            _ => panic!("Config types don't match"),
        }
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::BigQuery {
                    project_id: p1,
                    dataset_id: d1,
                    service_account_key: key1,
                    max_staleness_mins: staleness1,
                    connection_pool_size: connection_pool_size1,
                },
                StoredDestinationConfig::BigQuery {
                    project_id: p2,
                    dataset_id: d2,
                    service_account_key: key2,
                    max_staleness_mins: staleness2,
                    connection_pool_size: connection_pool_size2,
                },
            ) => {
                assert_eq!(p1, p2);
                assert_eq!(d1, d2);
                assert_eq!(staleness1, staleness2);
                assert_eq!(connection_pool_size1, connection_pool_size2);
                // Assert that service account key was encrypted and decrypted correctly
                assert_eq!(key1.expose_secret(), key2.expose_secret());
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_encryption_decryption_iceberg_supabase() {
        let config = StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_owned(),
                warehouse_name: "my-warehouse".to_owned(),
                namespace: Some("my-namespace".to_owned()),
                catalog_token: SerializableSecretString::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ".to_owned()),
                s3_access_key_id: SerializableSecretString::from("9156667efc2c70d89af6588da86d2924".to_owned()),
                s3_secret_access_key: SerializableSecretString::from("ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4".to_owned()),
                s3_region: "ap-southeast-1".to_owned(),
            },
        };

        let key = EncryptionKeyring::from(EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        });

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::Iceberg {
                    config:
                        StoredIcebergConfig::Supabase {
                            project_ref: p1_project_ref,
                            warehouse_name: p1_warehouse_name,
                            namespace: p1_namespace,
                            catalog_token: p1_catalog_token,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_region: p1_s3_region,
                        },
                },
                StoredDestinationConfig::Iceberg {
                    config:
                        StoredIcebergConfig::Supabase {
                            project_ref: p2_project_ref,
                            warehouse_name: p2_warehouse_name,
                            namespace: p2_namespace,
                            catalog_token: p2_catalog_token,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_region: p2_s3_region,
                        },
                },
            ) => {
                assert_eq!(p1_project_ref, p2_project_ref);
                assert_eq!(p1_warehouse_name, p2_warehouse_name);
                assert_eq!(p1_namespace, p2_namespace);
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(p1_s3_region, p2_s3_region);
                // Assert that secret fields were encrypted and decrypted correctly
                assert_eq!(p1_catalog_token.expose_secret(), p2_catalog_token.expose_secret());
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_encryption_decryption_iceberg_rest() {
        let config = StoredDestinationConfig::Iceberg {
            config: StoredIcebergConfig::Rest {
                catalog_uri: "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg"
                    .to_owned(),
                warehouse_name: "my-warehouse".to_owned(),
                namespace: Some("my-namespace".to_owned()),
                s3_access_key_id: SerializableSecretString::from("id".to_owned()),
                s3_secret_access_key: SerializableSecretString::from("key".to_owned()),
                s3_endpoint: "http://localhost:8080".to_owned(),
            },
        };

        let key = EncryptionKeyring::from(EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        });

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::Iceberg {
                    config:
                        StoredIcebergConfig::Rest {
                            catalog_uri: p1_catalog_uri,
                            warehouse_name: p1_warehouse_name,
                            namespace: p1_namespace,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_endpoint: p1_s3_endpoint,
                        },
                },
                StoredDestinationConfig::Iceberg {
                    config:
                        StoredIcebergConfig::Rest {
                            catalog_uri: p2_catalog_uri,
                            warehouse_name: p2_warehouse_name,
                            namespace: p2_namespace,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_endpoint: p2_s3_endpoint,
                        },
                },
            ) => {
                assert_eq!(p1_catalog_uri, p2_catalog_uri);
                assert_eq!(p1_warehouse_name, p2_warehouse_name);
                assert_eq!(p1_namespace, p2_namespace);
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
                assert_eq!(p1_s3_endpoint, p2_s3_endpoint);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_encryption_decryption_clickhouse() {
        let config = StoredDestinationConfig::ClickHouse {
            url: Url::parse("https://example.com:8443").unwrap(),
            user: "etl".to_owned(),
            password: Some(SerializableSecretString::from("secret".to_owned())),
            database: "analytics".to_owned(),
            engine: ClickHouseEngine::MergeTree,
        };

        let key = EncryptionKeyring::from(EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        });

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::ClickHouse {
                    url: u1,
                    user: user1,
                    password: p1,
                    database: d1,
                    engine: e1,
                },
                StoredDestinationConfig::ClickHouse {
                    url: u2,
                    user: user2,
                    password: p2,
                    database: d2,
                    engine: e2,
                },
            ) => {
                assert_eq!(u1, u2);
                assert_eq!(user1, user2);
                assert_eq!(d1, d2);
                assert_eq!(e1, e2);
                assert_eq!(
                    p1.as_ref().map(|value| value.expose_secret()),
                    p2.as_ref().map(|value| value.expose_secret())
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn create_api_destination_config_conversion_clickhouse() {
        let create_config = ApiDestinationConfig::ClickHouse {
            url: Url::parse("https://example.com:8443").unwrap(),
            user: "etl".to_owned(),
            password: Some(SerializableSecretString::from("secret".to_owned())),
            database: "analytics".to_owned(),
            engine: ClickHouseEngine::MergeTree,
        };

        let stored: StoredDestinationConfig = create_config.clone().into();
        let back_to_create: ApiDestinationConfig = stored.into();

        match (create_config, back_to_create) {
            (
                ApiDestinationConfig::ClickHouse {
                    url: u1,
                    user: user1,
                    password: p1,
                    database: d1,
                    engine: e1,
                },
                ApiDestinationConfig::ClickHouse {
                    url: u2,
                    user: user2,
                    password: p2,
                    database: d2,
                    engine: e2,
                },
            ) => {
                assert_eq!(u1, u2);
                assert_eq!(user1, user2);
                assert_eq!(d1, d2);
                assert_eq!(e1, e2);
                assert_eq!(
                    p1.as_ref().map(|value| value.expose_secret()),
                    p2.as_ref().map(|value| value.expose_secret())
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_into_etl_config_preserves_clickhouse_engine() {
        let config = StoredDestinationConfig::ClickHouse {
            url: Url::parse("https://example.com:8443").unwrap(),
            user: "etl".to_owned(),
            password: Some(SerializableSecretString::from("secret".to_owned())),
            database: "analytics".to_owned(),
            engine: ClickHouseEngine::MergeTree,
        };

        let etl_config = config.into_etl_config();

        match etl_config {
            DestinationConfig::ClickHouse { engine, .. } => {
                assert_eq!(engine, ClickHouseEngine::MergeTree);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn create_api_destination_config_deserializes_clickhouse_url() {
        let json = r#"
        {
            "clickhouse": {
                "url": "  https://example.com:8443  ",
                "user": "etl",
                "database": "analytics"
            }
        }
        "#;

        let deserialized: ApiDestinationConfig = serde_json::from_str(json).unwrap();
        match deserialized {
            ApiDestinationConfig::ClickHouse { url, user, password, database, engine } => {
                assert_eq!(url.as_str(), "https://example.com:8443/");
                assert_eq!(user, "etl");
                assert!(password.is_none());
                assert_eq!(database, "analytics");
                assert_eq!(engine, ClickHouseEngine::default());
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }

    #[test]
    fn create_api_destination_config_deserializes_clickhouse_engine() {
        let json = r#"
        {
            "clickhouse": {
                "url": "https://example.com:8443",
                "user": "etl",
                "database": "analytics",
                "engine": "merge_tree"
            }
        }
        "#;

        let deserialized: ApiDestinationConfig = serde_json::from_str(json).unwrap();
        match deserialized {
            ApiDestinationConfig::ClickHouse { engine, .. } => {
                assert_eq!(engine, ClickHouseEngine::MergeTree);
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }

    #[test]
    fn encrypted_stored_destination_config_defaults_legacy_clickhouse_engine() {
        let json = r#"
        {
            "click_house": {
                "url": "https://example.com:8443",
                "user": "etl",
                "password": null,
                "database": "analytics"
            }
        }
        "#;

        let deserialized: EncryptedStoredDestinationConfig = serde_json::from_str(json).unwrap();
        match deserialized {
            EncryptedStoredDestinationConfig::ClickHouse { engine, .. } => {
                assert_eq!(engine, ClickHouseEngine::default());
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }

    #[test]
    fn create_api_destination_config_rejects_non_http_clickhouse_url() {
        let json = r#"
        {
            "clickhouse": {
                "url": "ftp://example.com/data",
                "user": "etl",
                "database": "analytics"
            }
        }
        "#;

        let error = serde_json::from_str::<ApiDestinationConfig>(json).unwrap_err();
        assert!(error.to_string().contains("url must use http or https scheme"));
    }

    #[test]
    fn create_api_destination_config_conversion_bigquery() {
        let create_config = ApiDestinationConfig::BigQuery {
            project_id: "test-project".to_owned(),
            dataset_id: "test_dataset".to_owned(),
            service_account_key: SerializableSecretString::from("{\"test\": \"key\"}".to_owned()),
            max_staleness_mins: Some(15),
            connection_pool_size: None,
        };

        let stored: StoredDestinationConfig = create_config.clone().into();
        let back_to_create: ApiDestinationConfig = stored.into();

        match (create_config, back_to_create) {
            (
                ApiDestinationConfig::BigQuery {
                    project_id: p1_project_id,
                    dataset_id: p1_dataset_id,
                    service_account_key: p1_service_account_key,
                    max_staleness_mins: p1_max_staleness_mins,
                    connection_pool_size: p1_connection_pool_size,
                },
                ApiDestinationConfig::BigQuery {
                    project_id: p2_project_id,
                    dataset_id: p2_dataset_id,
                    service_account_key: p2_service_account_key,
                    max_staleness_mins: p2_max_staleness_mins,
                    connection_pool_size: p2_connection_pool_size,
                },
            ) => {
                assert_eq!(p1_project_id, p2_project_id);
                assert_eq!(p1_dataset_id, p2_dataset_id);
                assert_eq!(
                    p1_service_account_key.expose_secret(),
                    p2_service_account_key.expose_secret()
                );
                assert_eq!(p1_max_staleness_mins, p2_max_staleness_mins);
                // Note: connection_pool_size should be set to DEFAULT_POOL_SIZE when None
                assert_eq!(p1_connection_pool_size, None);
                assert_eq!(
                    p2_connection_pool_size,
                    Some(DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE)
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn update_api_destination_config_preserves_omitted_bigquery_secret() {
        let stored_config = StoredDestinationConfig::BigQuery {
            project_id: "test-project".to_owned(),
            dataset_id: "test_dataset".to_owned(),
            service_account_key: SerializableSecretString::from("existing-key".to_owned()),
            max_staleness_mins: Some(15),
            connection_pool_size: 8,
        };
        let update_config = UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Set("updated-project".to_owned()),
            dataset_id: UpdateField::Set("updated_dataset".to_owned()),
            service_account_key: UpdateField::Preserve,
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Preserve,
        };

        let updated_config = update_config.merge_into_stored(stored_config).unwrap();

        match updated_config {
            StoredDestinationConfig::BigQuery {
                project_id,
                dataset_id,
                service_account_key,
                max_staleness_mins,
                connection_pool_size,
            } => {
                assert_eq!(project_id, "updated-project");
                assert_eq!(dataset_id, "updated_dataset");
                assert_eq!(service_account_key.expose_secret(), "existing-key");
                assert_eq!(max_staleness_mins, Some(15));
                assert_eq!(connection_pool_size, 8);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn update_api_destination_config_resets_cleared_defaulted_field() {
        let stored_config = StoredDestinationConfig::BigQuery {
            project_id: "test-project".to_owned(),
            dataset_id: "test_dataset".to_owned(),
            service_account_key: SerializableSecretString::from("existing-key".to_owned()),
            max_staleness_mins: Some(15),
            connection_pool_size: 8,
        };
        let update_config = UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Preserve,
            dataset_id: UpdateField::Preserve,
            service_account_key: UpdateField::Preserve,
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Clear,
        };

        let updated_config = update_config.merge_into_stored(stored_config).unwrap();

        match updated_config {
            StoredDestinationConfig::BigQuery { connection_pool_size, .. } => {
                assert_eq!(connection_pool_size, DestinationConfig::DEFAULT_CONNECTION_POOL_SIZE);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn update_api_destination_config_replaces_provided_bigquery_secret() {
        let stored_config = StoredDestinationConfig::BigQuery {
            project_id: "test-project".to_owned(),
            dataset_id: "test_dataset".to_owned(),
            service_account_key: SerializableSecretString::from("existing-key".to_owned()),
            max_staleness_mins: Some(15),
            connection_pool_size: 8,
        };
        let update_config = UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Set("updated-project".to_owned()),
            dataset_id: UpdateField::Set("updated_dataset".to_owned()),
            service_account_key: UpdateField::Set(SerializableSecretString::from(
                "new-key".to_owned(),
            )),
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Preserve,
        };

        let updated_config = update_config.merge_into_stored(stored_config).unwrap();

        match updated_config {
            StoredDestinationConfig::BigQuery { service_account_key, .. } => {
                assert_eq!(service_account_key.expose_secret(), "new-key");
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn update_api_destination_config_preserves_omitted_snowflake_passphrase() {
        let stored_config = StoredDestinationConfig::Snowflake {
            account_id: "myorg-myaccount".to_owned(),
            user: "etl_user".to_owned(),
            private_key: SerializableSecretString::from("existing-key".to_owned()),
            private_key_passphrase: Some(SerializableSecretString::from(
                "existing-passphrase".to_owned(),
            )),
            database: "analytics".to_owned(),
            schema: "public".to_owned(),
            role: None,
        };
        let update_config: UpdateApiDestinationConfig = serde_json::from_value(serde_json::json!({
            "snowflake": {
                "account_id": "myorg-myaccount",
                "user": "etl_user",
                "private_key": "new-key",
                "database": "analytics",
                "schema": "public"
            }
        }))
        .unwrap();

        let updated_config = update_config.merge_into_stored(stored_config).unwrap();

        match updated_config {
            StoredDestinationConfig::Snowflake { private_key, private_key_passphrase, .. } => {
                assert_eq!(private_key.expose_secret(), "new-key");
                assert_eq!(
                    private_key_passphrase.expect("passphrase should be preserved").expose_secret(),
                    "existing-passphrase"
                );
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn update_api_destination_config_clears_null_snowflake_passphrase() {
        let stored_config = StoredDestinationConfig::Snowflake {
            account_id: "myorg-myaccount".to_owned(),
            user: "etl_user".to_owned(),
            private_key: SerializableSecretString::from("existing-key".to_owned()),
            private_key_passphrase: Some(SerializableSecretString::from(
                "existing-passphrase".to_owned(),
            )),
            database: "analytics".to_owned(),
            schema: "public".to_owned(),
            role: None,
        };
        let update_config: UpdateApiDestinationConfig = serde_json::from_value(serde_json::json!({
            "snowflake": {
                "account_id": "myorg-myaccount",
                "user": "etl_user",
                "private_key": "new-key",
                "private_key_passphrase": null,
                "database": "analytics",
                "schema": "public"
            }
        }))
        .unwrap();

        let updated_config = update_config.merge_into_stored(stored_config).unwrap();

        match updated_config {
            StoredDestinationConfig::Snowflake { private_key, private_key_passphrase, .. } => {
                assert_eq!(private_key.expose_secret(), "new-key");
                assert!(private_key_passphrase.is_none());
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn update_api_destination_config_preserves_omitted_clickhouse_and_ducklake_secrets() {
        let clickhouse: UpdateApiDestinationConfig = serde_json::from_value(serde_json::json!({
            "clickhouse": {
                "url": "https://example.com:8443",
                "user": "etl",
                "database": "analytics"
            }
        }))
        .unwrap();
        let ducklake: UpdateApiDestinationConfig = serde_json::from_value(serde_json::json!({
            "ducklake": {
                "data_path": "s3://bucket/path",
                "s3_region": "us-east-1",
                "s3_endpoint": "storage.example.com"
            }
        }))
        .unwrap();

        match clickhouse {
            UpdateApiDestinationConfig::ClickHouse { password, .. } => {
                assert!(password.is_preserve());
            }
            _ => panic!("Config types don't match"),
        }
        match ducklake {
            UpdateApiDestinationConfig::Ducklake {
                s3_access_key_id, s3_secret_access_key, ..
            } => {
                assert!(s3_access_key_id.is_preserve());
                assert!(s3_secret_access_key.is_preserve());
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn update_api_destination_config_clears_null_clickhouse_and_ducklake_secrets() {
        let clickhouse: UpdateApiDestinationConfig = serde_json::from_value(serde_json::json!({
            "clickhouse": {
                "url": "https://example.com:8443",
                "user": "etl",
                "password": null,
                "database": "analytics"
            }
        }))
        .unwrap();
        let ducklake: UpdateApiDestinationConfig = serde_json::from_value(serde_json::json!({
            "ducklake": {
                "data_path": "s3://bucket/path",
                "s3_access_key_id": null,
                "s3_secret_access_key": "secret",
                "s3_region": "us-east-1",
                "s3_endpoint": "storage.example.com"
            }
        }))
        .unwrap();

        match clickhouse {
            UpdateApiDestinationConfig::ClickHouse { password, .. } => {
                assert!(matches!(password, UpdateField::Clear));
            }
            _ => panic!("Config types don't match"),
        }
        match ducklake {
            UpdateApiDestinationConfig::Ducklake {
                s3_access_key_id, s3_secret_access_key, ..
            } => {
                assert!(matches!(s3_access_key_id, UpdateField::Clear));
                assert!(matches!(s3_secret_access_key, UpdateField::Set(_)));
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn update_api_destination_config_requires_secrets_when_changing_destination_type() {
        let stored_config = StoredDestinationConfig::ClickHouse {
            url: Url::parse("https://example.com:8443").unwrap(),
            user: "etl".to_owned(),
            password: Some(SerializableSecretString::from("secret".to_owned())),
            database: "analytics".to_owned(),
            engine: ClickHouseEngine::MergeTree,
        };
        let update_config = UpdateApiDestinationConfig::BigQuery {
            project_id: UpdateField::Set("test-project".to_owned()),
            dataset_id: UpdateField::Set("test_dataset".to_owned()),
            service_account_key: UpdateField::Preserve,
            max_staleness_mins: UpdateField::Preserve,
            connection_pool_size: UpdateField::Preserve,
        };

        let error = update_config.merge_into_stored(stored_config).unwrap_err();

        assert!(matches!(
            error,
            DestinationConfigUpdateError::MissingRequiredSecret {
                destination: "BigQuery",
                field: "service_account_key"
            }
        ));
    }

    #[test]
    fn create_api_destination_config_conversion_iceberg_supabase() {
        let create_config = ApiDestinationConfig::Iceberg {
            config: ApiIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_owned(),
                warehouse_name: "my-warehouse".to_owned(),
                namespace: Some("my-namespace".to_owned()),
                catalog_token: SerializableSecretString::from("eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6IjFkNzFjMGEyNmIxMDFjODQ5ZTkxZmQ1NjdjYjA5NTJmIn0.eyJleHAiOjIwNzA3MTcxNjAsImlhdCI6MTc1NjE0NTE1MCwiaXNzIjoic3VwYWJhc2UiLCJyZWYiOiJhYmNkZWZnaGlqbGttbm9wcXJzdCIsInJvbGUiOiJzZXJ2aWNlX3JvbGUifQ.YdTWkkIvwjSkXot3NC07xyjPjGWQMNzLq5EPzumzrdLzuHrj-zuzI-nlyQtQ5V7gZauysm-wGwmpztRXfPc3AQ".to_owned()),
                s3_access_key_id: SerializableSecretString::from("9156667efc2c70d89af6588da86d2924".to_owned()),
                s3_secret_access_key: SerializableSecretString::from("ca833e890916d848c69135924bcd75e5909184814a0ebc6c988937ee094120d4".to_owned()),
                s3_region: "ap-southeast-1".to_owned(),
            },
        };

        let stored: StoredDestinationConfig = create_config.clone().into();
        let back_to_create: ApiDestinationConfig = stored.into();

        match (create_config, back_to_create) {
            (
                ApiDestinationConfig::Iceberg {
                    config:
                        ApiIcebergConfig::Supabase {
                            project_ref: p1_project_ref,
                            warehouse_name: p1_warehouse_name,
                            namespace: p1_namespace,
                            catalog_token: p1_catalog_token,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_region: p1_s3_region,
                        },
                },
                ApiDestinationConfig::Iceberg {
                    config:
                        ApiIcebergConfig::Supabase {
                            project_ref: p2_project_ref,
                            warehouse_name: p2_warehouse_name,
                            namespace: p2_namespace,
                            catalog_token: p2_catalog_token,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_region: p2_s3_region,
                        },
                },
            ) => {
                assert_eq!(p1_project_ref, p2_project_ref);
                assert_eq!(p1_warehouse_name, p2_warehouse_name);
                assert_eq!(p1_namespace, p2_namespace);
                assert_eq!(p1_catalog_token.expose_secret(), p2_catalog_token.expose_secret());
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
                assert_eq!(p1_s3_region, p2_s3_region);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn create_api_destination_config_conversion_iceberg_rest() {
        let create_config = ApiDestinationConfig::Iceberg {
            config: ApiIcebergConfig::Rest {
                catalog_uri: "https://abcdefghijklmnopqrst.storage.supabase.com/storage/v1/iceberg"
                    .to_owned(),
                warehouse_name: "my-warehouse".to_owned(),
                namespace: Some("my-namespace".to_owned()),
                s3_access_key_id: SerializableSecretString::from("id".to_owned()),
                s3_secret_access_key: SerializableSecretString::from("key".to_owned()),
                s3_endpoint: "http://localhost:8080".to_owned(),
            },
        };

        let stored: StoredDestinationConfig = create_config.clone().into();
        let back_to_create: ApiDestinationConfig = stored.into();

        match (create_config, back_to_create) {
            (
                ApiDestinationConfig::Iceberg {
                    config:
                        ApiIcebergConfig::Rest {
                            catalog_uri: p1_catalog_uri,
                            warehouse_name: p1_warehouse_name,
                            namespace: p1_namespace,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_endpoint: p1_s3_endpoint,
                        },
                },
                ApiDestinationConfig::Iceberg {
                    config:
                        ApiIcebergConfig::Rest {
                            catalog_uri: p2_catalog_uri,
                            warehouse_name: p2_warehouse_name,
                            namespace: p2_namespace,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_endpoint: p2_s3_endpoint,
                        },
                },
            ) => {
                assert_eq!(p1_catalog_uri, p2_catalog_uri);
                assert_eq!(p1_warehouse_name, p2_warehouse_name);
                assert_eq!(p1_namespace, p2_namespace);
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
                assert_eq!(p1_s3_endpoint, p2_s3_endpoint);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn stored_destination_config_encryption_decryption_ducklake() {
        let config = StoredDestinationConfig::Ducklake {
            catalog_url: SerializableSecretString::from(
                "postgres://user:pass@localhost:5432/ducklake_catalog".to_owned(),
            ),
            data_path: "s3://bucket/path".to_owned(),
            pool_size: 8,
            s3_access_key_id: Some(SerializableSecretString::from("access".to_owned())),
            s3_secret_access_key: Some(SerializableSecretString::from("secret".to_owned())),
            s3_region: Some("us-east-1".to_owned()),
            s3_endpoint: Some("127.0.0.1:5000/s3".to_owned()),
            s3_url_style: Some("path".to_owned()),
            s3_use_ssl: Some(false),
            metadata_schema: Some("ducklake".to_owned()),
            maintenance_target_file_size: Some("10MB".to_owned()),
            expire_snapshots_older_than: Some("7 days".to_owned()),
            maintenance_mode: DuckLakeMaintenanceMode::Kubernetes,
        };

        let key = EncryptionKeyring::from(EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        });

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        match (config, decrypted) {
            (
                StoredDestinationConfig::Ducklake {
                    catalog_url: c1,
                    data_path: d1,
                    pool_size: p1,
                    s3_access_key_id: a1,
                    s3_secret_access_key: s1,
                    s3_region: r1,
                    s3_endpoint: e1,
                    s3_url_style: u1,
                    s3_use_ssl: ssl1,
                    metadata_schema: m1,
                    maintenance_target_file_size: target1,
                    expire_snapshots_older_than: expire1,
                    maintenance_mode: mode1,
                },
                StoredDestinationConfig::Ducklake {
                    catalog_url: c2,
                    data_path: d2,
                    pool_size: p2,
                    s3_access_key_id: a2,
                    s3_secret_access_key: s2,
                    s3_region: r2,
                    s3_endpoint: e2,
                    s3_url_style: u2,
                    s3_use_ssl: ssl2,
                    metadata_schema: m2,
                    maintenance_target_file_size: target2,
                    expire_snapshots_older_than: expire2,
                    maintenance_mode: mode2,
                },
            ) => {
                assert_eq!(c1.expose_secret(), c2.expose_secret());
                assert_eq!(d1, d2);
                assert_eq!(p1, p2);
                assert_eq!(
                    a1.as_ref().map(|value| value.expose_secret()),
                    a2.as_ref().map(|value| value.expose_secret())
                );
                assert_eq!(
                    s1.as_ref().map(|value| value.expose_secret()),
                    s2.as_ref().map(|value| value.expose_secret())
                );
                assert_eq!(r1, r2);
                assert_eq!(e1, e2);
                assert_eq!(u1, u2);
                assert_eq!(ssl1, ssl2);
                assert_eq!(m1, m2);
                assert_eq!(target1, target2);
                assert_eq!(expire1, expire2);
                assert_eq!(mode1, mode2);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn encrypted_stored_destination_config_ducklake_defaults_maintenance_mode() {
        let key = EncryptionKeyring::from(EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        });
        let encrypted_catalog_url =
            encrypt_text("postgres://user:pass@localhost:5432/ducklake_catalog".to_owned(), &key)
                .unwrap();
        let config: EncryptedStoredDestinationConfig = serde_json::from_value(serde_json::json!({
            "ducklake": {
                "catalog_url": encrypted_catalog_url,
                "data_path": "s3://bucket/path",
                "pool_size": 8
            }
        }))
        .unwrap();

        match config {
            EncryptedStoredDestinationConfig::Ducklake { maintenance_mode, .. } => {
                assert_eq!(maintenance_mode, DuckLakeMaintenanceMode::Disabled);
            }
            _ => panic!("Config type doesn't match"),
        }
    }

    #[test]
    fn encrypted_stored_destination_config_ducklake_rejects_plaintext_catalog_url() {
        let error = serde_json::from_value::<EncryptedStoredDestinationConfig>(serde_json::json!({
            "ducklake": {
                "catalog_url": "postgres://user:pass@localhost:5432/ducklake_catalog",
                "data_path": "s3://bucket/path",
                "pool_size": 8
            }
        }))
        .unwrap_err();

        assert!(error.to_string().contains("invalid type"));
    }

    #[test]
    fn create_api_destination_config_ducklake_defaults_maintenance_mode() {
        let config: ApiDestinationConfig = serde_json::from_value(serde_json::json!({
            "ducklake": {
                "catalog_url": "postgres://user:pass@localhost:5432/ducklake_catalog",
                "data_path": "s3://bucket/path"
            }
        }))
        .unwrap();

        match config {
            ApiDestinationConfig::Ducklake { maintenance_mode, .. } => {
                assert_eq!(maintenance_mode, DuckLakeMaintenanceMode::Disabled);
            }
            _ => panic!("Config type doesn't match"),
        }
    }

    #[test]
    fn create_api_destination_config_conversion_ducklake() {
        let create_config = ApiDestinationConfig::Ducklake {
            catalog_url: SerializableSecretString::from(
                "postgres://user:pass@localhost:5432/ducklake_catalog".to_owned(),
            ),
            data_path: "s3://bucket/path".to_owned(),
            pool_size: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_region: None,
            s3_endpoint: None,
            s3_url_style: None,
            s3_use_ssl: None,
            metadata_schema: Some("ducklake".to_owned()),
            maintenance_target_file_size: None,
            expire_snapshots_older_than: None,
            maintenance_mode: DuckLakeMaintenanceMode::Kubernetes,
        };

        let stored: StoredDestinationConfig = create_config.clone().into();
        let back_to_create: ApiDestinationConfig = stored.into();

        match (create_config, back_to_create) {
            (
                ApiDestinationConfig::Ducklake {
                    catalog_url: c1,
                    data_path: d1,
                    pool_size: p1,
                    metadata_schema: m1,
                    maintenance_target_file_size: target1,
                    expire_snapshots_older_than: expire1,
                    ..
                },
                ApiDestinationConfig::Ducklake {
                    catalog_url: c2,
                    data_path: d2,
                    pool_size: p2,
                    metadata_schema: m2,
                    maintenance_target_file_size: target2,
                    expire_snapshots_older_than: expire2,
                    ..
                },
            ) => {
                assert_eq!(c1.expose_secret(), c2.expose_secret());
                assert_eq!(d1, d2);
                assert_eq!(p1, None);
                assert_eq!(p2, Some(DestinationConfig::DEFAULT_DUCKLAKE_POOL_SIZE));
                assert_eq!(m1, m2);
                assert_eq!(target1, target2);
                assert_eq!(expire1, expire2);
            }
            _ => panic!("Config types don't match"),
        }
    }

    #[test]
    fn create_api_destination_config_serialization_ducklake() {
        let create_config = ApiDestinationConfig::Ducklake {
            catalog_url: SerializableSecretString::from(
                "postgres://user:pass@localhost:5432/ducklake_catalog".to_owned(),
            ),
            data_path: "s3://bucket/path".to_owned(),
            pool_size: Some(4),
            s3_access_key_id: Some(SerializableSecretString::from("access".to_owned())),
            s3_secret_access_key: Some(SerializableSecretString::from("secret".to_owned())),
            s3_region: Some("us-east-1".to_owned()),
            s3_endpoint: Some("127.0.0.1:5000/s3".to_owned()),
            s3_url_style: Some("path".to_owned()),
            s3_use_ssl: Some(false),
            metadata_schema: Some("ducklake".to_owned()),
            maintenance_target_file_size: Some("10MB".to_owned()),
            expire_snapshots_older_than: Some("7 days".to_owned()),
            maintenance_mode: DuckLakeMaintenanceMode::Kubernetes,
        };

        assert_json_snapshot!(create_config);

        let json = serde_json::to_string_pretty(&create_config).unwrap();
        let deserialized: ApiDestinationConfig = serde_json::from_str(&json).unwrap();
        match (&create_config, deserialized) {
            (
                ApiDestinationConfig::Ducklake {
                    catalog_url: c1,
                    data_path: d1,
                    pool_size: p1,
                    s3_access_key_id: a1,
                    s3_secret_access_key: s1,
                    s3_region: r1,
                    s3_endpoint: e1,
                    s3_url_style: u1,
                    s3_use_ssl: ssl1,
                    metadata_schema: m1,
                    maintenance_target_file_size: target1,
                    expire_snapshots_older_than: expire1,
                    maintenance_mode: mode1,
                },
                ApiDestinationConfig::Ducklake {
                    catalog_url: c2,
                    data_path: d2,
                    pool_size: p2,
                    s3_access_key_id: a2,
                    s3_secret_access_key: s2,
                    s3_region: r2,
                    s3_endpoint: e2,
                    s3_url_style: u2,
                    s3_use_ssl: ssl2,
                    metadata_schema: m2,
                    maintenance_target_file_size: target2,
                    expire_snapshots_older_than: expire2,
                    maintenance_mode: mode2,
                },
            ) => {
                assert_eq!(c1.expose_secret(), c2.expose_secret());
                assert_eq!(d1, &d2);
                assert_eq!(p1, &p2);
                assert_eq!(
                    a1.as_ref().map(|value| value.expose_secret()),
                    a2.as_ref().map(|value| value.expose_secret())
                );
                assert_eq!(
                    s1.as_ref().map(|value| value.expose_secret()),
                    s2.as_ref().map(|value| value.expose_secret())
                );
                assert_eq!(r1, &r2);
                assert_eq!(e1, &e2);
                assert_eq!(u1, &u2);
                assert_eq!(ssl1, &ssl2);
                assert_eq!(m1, &m2);
                assert_eq!(target1, &target2);
                assert_eq!(expire1, &expire2);
                assert_eq!(mode1, &mode2);
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }

    #[test]
    fn create_api_destination_config_serialization_iceberg_supabase() {
        let create_config = ApiDestinationConfig::Iceberg {
            config: ApiIcebergConfig::Supabase {
                project_ref: "abcdefghijklmnopqrst".to_owned(),
                warehouse_name: "my-warehouse".to_owned(),
                namespace: Some("my-namespace".to_owned()),
                catalog_token: SerializableSecretString::from("token123".to_owned()),
                s3_access_key_id: SerializableSecretString::from("access_key_123".to_owned()),
                s3_secret_access_key: SerializableSecretString::from("secret123".to_owned()),
                s3_region: "us-west-2".to_owned(),
            },
        };

        // Use snapshot testing to verify the exact JSON structure
        assert_json_snapshot!(create_config);

        // Test that we can deserialize it back and all fields match
        let json = serde_json::to_string_pretty(&create_config).unwrap();
        let deserialized: ApiDestinationConfig = serde_json::from_str(&json).unwrap();
        match (&create_config, deserialized) {
            (
                ApiDestinationConfig::Iceberg {
                    config:
                        ApiIcebergConfig::Supabase {
                            project_ref: orig_project_ref,
                            warehouse_name: orig_warehouse_name,
                            namespace: orig_namespace,
                            catalog_token: orig_catalog_token,
                            s3_access_key_id: orig_s3_access_key_id,
                            s3_secret_access_key: orig_s3_secret_access_key,
                            s3_region: orig_s3_region,
                        },
                },
                ApiDestinationConfig::Iceberg {
                    config:
                        ApiIcebergConfig::Supabase {
                            project_ref: deser_project_ref,
                            warehouse_name: deser_warehouse_name,
                            namespace: deser_namespace,
                            catalog_token: deser_catalog_token,
                            s3_access_key_id: deser_s3_access_key_id,
                            s3_secret_access_key: deser_s3_secret_access_key,
                            s3_region: deser_s3_region,
                        },
                },
            ) => {
                assert_eq!(orig_project_ref, &deser_project_ref);
                assert_eq!(orig_warehouse_name, &deser_warehouse_name);
                assert_eq!(orig_namespace, &deser_namespace);
                assert_eq!(orig_catalog_token.expose_secret(), deser_catalog_token.expose_secret());
                assert_eq!(
                    orig_s3_access_key_id.expose_secret(),
                    deser_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    orig_s3_secret_access_key.expose_secret(),
                    deser_s3_secret_access_key.expose_secret()
                );
                assert_eq!(orig_s3_region, &deser_s3_region);
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }

    #[test]
    fn create_api_destination_config_serialization_iceberg_rest() {
        let create_config = ApiDestinationConfig::Iceberg {
            config: ApiIcebergConfig::Rest {
                catalog_uri: "https://catalog.example.com/iceberg".to_owned(),
                warehouse_name: "my-warehouse".to_owned(),
                namespace: Some("my-namespace".to_owned()),
                s3_access_key_id: SerializableSecretString::from("id".to_owned()),
                s3_secret_access_key: SerializableSecretString::from("key".to_owned()),
                s3_endpoint: "http://localhost:8080".to_owned(),
            },
        };

        // Use snapshot testing to verify the exact JSON structure
        assert_json_snapshot!(create_config);

        // Test that we can deserialize it back and all fields match
        let json = serde_json::to_string_pretty(&create_config).unwrap();
        let deserialized: ApiDestinationConfig = serde_json::from_str(&json).unwrap();
        match (&create_config, deserialized) {
            (
                ApiDestinationConfig::Iceberg {
                    config:
                        ApiIcebergConfig::Rest {
                            catalog_uri: orig_catalog_uri,
                            warehouse_name: orig_warehouse_name,
                            namespace: orig_namespace,
                            s3_access_key_id: p1_s3_access_key_id,
                            s3_secret_access_key: p1_s3_secret_access_key,
                            s3_endpoint: p1_s3_endpoint,
                        },
                },
                ApiDestinationConfig::Iceberg {
                    config:
                        ApiIcebergConfig::Rest {
                            catalog_uri: deser_catalog_uri,
                            warehouse_name: deser_warehouse_name,
                            namespace: deser_namespace,
                            s3_access_key_id: p2_s3_access_key_id,
                            s3_secret_access_key: p2_s3_secret_access_key,
                            s3_endpoint: p2_s3_endpoint,
                        },
                },
            ) => {
                assert_eq!(orig_catalog_uri, &deser_catalog_uri);
                assert_eq!(orig_warehouse_name, &deser_warehouse_name);
                assert_eq!(orig_namespace, &deser_namespace);
                assert_eq!(
                    p1_s3_access_key_id.expose_secret(),
                    p2_s3_access_key_id.expose_secret()
                );
                assert_eq!(
                    p1_s3_secret_access_key.expose_secret(),
                    p2_s3_secret_access_key.expose_secret()
                );
                assert_eq!(p1_s3_endpoint, &p2_s3_endpoint);
            }
            _ => panic!("Deserialization failed or variant mismatch"),
        }
    }
}
