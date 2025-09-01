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

/// Separator for list elements in environment variables.
///
/// Example: `APP_API_KEYS=abc,def` sets the `api_keys` array field.
const LIST_SEPARATOR: &str = ",";

/// Trait defining the list of keys that should be parsed as lists in a given [`Config`]
/// implementation.
pub trait Config {
    /// Slice containing all the keys that should be parsed as lists when loading the configuration.
    const LIST_PARSE_KEYS: &'static [&'static str];
}

/// Loads hierarchical configuration from YAML files and environment variables.
///
/// Loads configuration in this order:
/// 1. Base configuration from `configuration/base.yaml`
/// 2. Environment-specific file from `configuration/{environment}.yaml`
/// 3. Environment variable overrides prefixed with `APP`
///
/// Nested keys use double underscores: `APP_DATABASE__URL` → `database.url` and lists are separated
/// by `,`.
///
/// # Panics
/// Panics if current directory cannot be determined or if `APP_ENVIRONMENT`
/// cannot be parsed.
pub fn load_config<T>() -> Result<T, config::ConfigError>
where
    T: Config + DeserializeOwned,
{
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join(CONFIGURATION_DIR);

    // Detect the running environment.
    // Default to `dev` if unspecified.
    let environment = Environment::load().expect("Failed to parse APP_ENVIRONMENT.");

    let environment_filename = format!("{environment}.yaml");

    // We build the environment configuration source.
    let mut environment_source = config::Environment::with_prefix(ENV_PREFIX)
        .prefix_separator(ENV_PREFIX_SEPARATOR)
        .separator(ENV_SEPARATOR);

    // If there is a list of keys to parse, we add them to the source and enable parsing with the
    // separator.
    if !<T as Config>::LIST_PARSE_KEYS.is_empty() {
        environment_source = environment_source
            .try_parsing(true)
            .list_separator(LIST_SEPARATOR);

        for key in <T as Config>::LIST_PARSE_KEYS {
            environment_source = environment_source.with_list_parse_key(key);
        }
    }

    let settings = config::Config::builder()
        // Add in settings from the base configuration file.
        .add_source(config::File::from(
            configuration_directory.join(BASE_CONFIG_FILE),
        ))
        // Add in settings from the environment-specific file.
        .add_source(config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and '__' as separator)
        // E.g. `APP_DESTINATION__BIG_QUERY__PROJECT_ID=my-project-id` sets
        // `Settings { destination: BigQuery { project_id } }` to `my-project-id`.
        .add_source(environment_source)
        .build()?;

    settings.try_deserialize::<T>()
}
