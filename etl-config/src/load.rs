use serde::de::DeserializeOwned;

use crate::environment::Environment;

/// Directory containing configuration files relative to application root.
const CONFIGURATION_DIR: &str = "configuration";

/// Base configuration file loaded for all environments.
const BASE_CONFIG_FILE: &str = "base.yaml";

/// Prefix for environment variable configuration overrides.
const ENV_PREFIX: &str = "APP";

/// Separator between environment variable prefix and key segments.
const ENV_PREFIX_SEPARATOR: &str = "_";

/// Separator for nested configuration keys in environment variables.
///
/// Example: `APP_DATABASE__URL` sets the `database.url` field.
const ENV_SEPARATOR: &str = "__";

/// Loads hierarchical configuration from YAML files and environment variables.
///
/// Loads configuration in this order:
/// 1. Base configuration from `configuration/base.yaml`
/// 2. Environment-specific file from `configuration/{environment}.yaml`
/// 3. Environment variable overrides prefixed with `APP`
///
/// Nested keys use double underscores: `APP_DATABASE__URL` → `database.url`
///
/// # Panics
/// Panics if current directory cannot be determined or if `APP_ENVIRONMENT`
/// cannot be parsed.
pub fn load_config<T>() -> Result<T, config::ConfigError>
where
    T: DeserializeOwned,
{
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join(CONFIGURATION_DIR);

    // Detect the running environment.
    // Default to `dev` if unspecified.
    let environment = Environment::load().expect("Failed to parse APP_ENVIRONMENT.");

    let environment_filename = format!("{environment}.yaml");
    let settings = config::Config::builder()
        .add_source(config::File::from(
            configuration_directory.join(BASE_CONFIG_FILE),
        ))
        .add_source(config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and '__' as separator)
        // E.g. `APP_DESTINATION__BIG_QUERY__PROJECT_ID=my-project-id` sets
        // `Settings { destination: BigQuery { project_id } }` to `my-project-id`.
        .add_source(
            config::Environment::with_prefix(ENV_PREFIX)
                .prefix_separator(ENV_PREFIX_SEPARATOR)
                .separator(ENV_SEPARATOR),
        )
        .build()?;

    settings.try_deserialize::<T>()
}
