use std::{
    borrow::Cow,
    io,
    path::{Path, PathBuf},
};

use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::environment::Environment;

/// Directory containing configuration files relative to application root.
const CONFIGURATION_DIR: &str = "configuration";

/// Supported extensions for base and environment configuration files.
const CONFIG_FILE_EXTENSIONS: &[&str] = &["yaml", "yml", "json"];

/// Prefix for environment variable configuration overrides.
const ENV_PREFIX: &str = "APP";

/// Separator between environment variable prefix and key segments.
const ENV_PREFIX_SEPARATOR: &str = "_";

/// Separator for nested configuration keys in environment variables.
const ENV_SEPARATOR: &str = "__";

/// Separator for list elements in environment variables.
const LIST_SEPARATOR: &str = ",";

/// Trait implemented by configuration structures that require list parsing help.
pub trait Config {
    /// Keys whose values should be parsed as lists when loading the configuration.
    const LIST_PARSE_KEYS: &'static [&'static str];
}

/// Identifies which configuration file is currently being loaded.
#[derive(Debug, Clone, Copy)]
enum ConfigFileKind {
    /// Always-present base configuration that every service loads.
    Base,
    /// Environment-specific overrides (dev/staging/prod).
    Environment(Environment),
}

impl ConfigFileKind {
    fn stem(&self) -> Cow<'static, str> {
        match self {
            ConfigFileKind::Base => Cow::Borrowed("base"),
            ConfigFileKind::Environment(env) => Cow::Owned(env.to_string()),
        }
    }

    /// Returns a static string describing this configuration file kind for error messages.
    fn as_str(&self) -> &'static str {
        match self {
            ConfigFileKind::Base => "base",
            ConfigFileKind::Environment(Environment::Dev) => "dev",
            ConfigFileKind::Environment(Environment::Staging) => "staging",
            ConfigFileKind::Environment(Environment::Prod) => "prod",
        }
    }
}

/// Errors that can occur while loading configuration files and overrides.
#[derive(Debug, Error)]
pub enum LoadConfigError {
    /// Failed to determine the current working directory.
    #[error("failed to determine the current directory")]
    CurrentDir(#[source] io::Error),

    /// The configured `configuration` directory does not exist.
    #[error("configuration directory `{0}` does not exist")]
    MissingConfigurationDirectory(PathBuf),

    /// Could not locate one of the required configuration files.
    #[error("could not locate {kind} configuration in `{directory}`; attempted: {attempted}")]
    ConfigurationFileMissing {
        kind: &'static str,
        directory: PathBuf,
        attempted: String,
    },

    /// Environment variable overrides failed to merge into the configuration.
    #[error("failed to load configuration from environment variables")]
    EnvironmentVariables(#[source] config::ConfigError),

    /// The configuration files were parsed but deserialization failed.
    #[error("failed to deserialize configuration")]
    Deserialization(#[source] config::ConfigError),

    /// Failed to determine the runtime environment (`APP_ENVIRONMENT`).
    #[error("failed to determine runtime environment")]
    Environment(#[source] io::Error),

    /// Failed to initialize the configuration builder.
    #[error("failed to initialize configuration builder")]
    Builder(#[source] config::ConfigError),
}

/// Loads hierarchical configuration from base, environment, and environment-variable sources.
///
/// Loads files from `configuration/base.(yaml|yml|json)` and `configuration/{environment}.{yaml|yml|json}`
/// before applying overrides from `APP_`-prefixed environment variables.
///
/// Nested keys use double underscores (`APP_SERVICE__HOST`), and list values are comma-separated.
pub fn load_config<T>() -> Result<T, LoadConfigError>
where
    T: Config + DeserializeOwned,
{
    let base_path = std::env::current_dir().map_err(LoadConfigError::CurrentDir)?;
    let configuration_directory = base_path.join(CONFIGURATION_DIR);

    if !configuration_directory.is_dir() {
        return Err(LoadConfigError::MissingConfigurationDirectory(
            configuration_directory,
        ));
    }

    let environment = Environment::load().map_err(LoadConfigError::Environment)?;

    let base_file = find_configuration_file(&configuration_directory, ConfigFileKind::Base)?;
    let environment_file = find_configuration_file(
        &configuration_directory,
        ConfigFileKind::Environment(environment),
    )?;

    let mut environment_source = config::Environment::with_prefix(ENV_PREFIX)
        .prefix_separator(ENV_PREFIX_SEPARATOR)
        .separator(ENV_SEPARATOR);

    if !T::LIST_PARSE_KEYS.is_empty() {
        environment_source = environment_source
            .try_parsing(true)
            .list_separator(LIST_SEPARATOR);

        for key in <T as Config>::LIST_PARSE_KEYS {
            environment_source = environment_source.with_list_parse_key(key);
        }
    }

    let base_file_source = config::File::from(base_file.clone());
    let environment_file_source = config::File::from(environment_file.clone());

    let builder = config::Config::builder()
        .add_source(base_file_source)
        .add_source(environment_file_source)
        .add_source(environment_source);

    let settings = builder.build().map_err(LoadConfigError::Builder)?;

    settings
        .try_deserialize::<T>()
        .map_err(LoadConfigError::Deserialization)
}

/// Finds the configuration file that matches the requested kind and supported extensions.
fn find_configuration_file(
    directory: &Path,
    kind: ConfigFileKind,
) -> Result<PathBuf, LoadConfigError> {
    let stem = kind.stem();
    let mut attempted_paths = Vec::with_capacity(CONFIG_FILE_EXTENSIONS.len());

    for extension in CONFIG_FILE_EXTENSIONS {
        let path = directory.join(format!("{stem}.{extension}"));
        attempted_paths.push(path.clone());

        if path.is_file() {
            return Ok(path);
        }
    }

    let attempted = attempted_paths
        .iter()
        .map(|path| format!("`{}`", path.display()))
        .collect::<Vec<_>>()
        .join(", ");

    Err(LoadConfigError::ConfigurationFileMissing {
        kind: kind.as_str(),
        directory: directory.to_path_buf(),
        attempted,
    })
}
