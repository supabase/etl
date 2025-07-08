use serde::de::DeserializeOwned;

use crate::environment::Environment;

/// Directory containing configuration files relative to the application root.
const CONFIGURATION_DIR: &str = "configuration";

/// Name of the base configuration file loaded for all environments.
const BASE_CONFIG_FILE: &str = "base.yaml";

/// Prefix for environment variable overrides in configuration.
///
/// Environment variables with this prefix are used to override configuration values.
const ENV_PREFIX: &str = "APP";

/// Separator between the environment variable prefix and the first key segment.
const ENV_PREFIX_SEPARATOR: &str = "_";

/// Separator for nested configuration keys in environment variables.
///
/// For example, `APP_DATABASE__URL` sets the `database.url` field.
const ENV_SEPARATOR: &str = "__";

/// Loads the application configuration from YAML files and environment variables.
///
/// This function loads the base configuration file, then an environment-specific file
/// (determined by the `APP_ENVIRONMENT` variable, defaulting to `dev`), and finally
/// applies overrides from environment variables prefixed with `APP`. Nested keys are
/// separated by double underscores (`__`).
///
/// The resulting configuration is deserialized into the type `T`.
///
/// # Panics
///
/// Panics if the current working directory cannot be determined or if the value of
/// `APP_ENVIRONMENT` cannot be parsed into an [`Environment`].
///
/// Returns an error if configuration loading or deserialization fails.
pub fn load_config<T>() -> Result<T, rust_cli_config::ConfigError>
where
    T: DeserializeOwned,
{
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join(CONFIGURATION_DIR);

    // Detect the running environment.
    // Default to `dev` if unspecified.
    let environment = Environment::load().expect("Failed to parse APP_ENVIRONMENT.");

    let environment_filename = format!("{environment}.yaml");
    let settings = rust_cli_config::Config::builder()
        .add_source(rust_cli_config::File::from(
            configuration_directory.join(BASE_CONFIG_FILE),
        ))
        .add_source(rust_cli_config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and '__' as separator)
        // E.g. `APP_DESTINATION__BIG_QUERY__PROJECT_ID=my-project-id` sets
        // `Settings { destination: BigQuery { project_id } }` to `my-project-id`.
        .add_source(
            rust_cli_config::Environment::with_prefix(ENV_PREFIX)
                .prefix_separator(ENV_PREFIX_SEPARATOR)
                .separator(ENV_SEPARATOR),
        )
        .build()?;

    settings.try_deserialize::<T>()
}
