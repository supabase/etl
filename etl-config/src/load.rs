use std::{
    borrow::Cow,
    io,
    path::{Path, PathBuf},
};

use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::environment::Environment;

/// Directory containing configuration files relative to the application root.
const CONFIGURATION_DIR: &str = "configuration";

/// Environment variable for specifying an absolute path to the configuration directory.
const CONFIG_DIR_ENV_VAR: &str = "APP_CONFIG_DIR";

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
/// The configuration directory is determined by:
/// - First checking the `APP_CONFIG_DIR` environment variable for an absolute path
/// - If not set, using `<current_dir>/configuration`
///
/// Loads files from `base.(yaml|yml|json)` and `{environment}.(yaml|yml|json)`
/// before applying overrides from `APP_`-prefixed environment variables.
///
/// Nested keys use double underscores (`APP_SERVICE__HOST`), and list values are comma-separated.
pub fn load_config<T>() -> Result<T, LoadConfigError>
where
    T: Config + DeserializeOwned,
{
    let configuration_directory = if let Ok(config_dir) = std::env::var(CONFIG_DIR_ENV_VAR) {
        // Use the absolute path provided by APP_CONFIG_DIR
        PathBuf::from(config_dir)
    } else {
        // Fallback to <current_dir>/configuration
        let base_path = std::env::current_dir().map_err(LoadConfigError::CurrentDir)?;
        base_path.join(CONFIGURATION_DIR)
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::fs;
    use std::sync::{Mutex, OnceLock};
    use tempfile::TempDir;

    /// Mutex to serialize tests that modify environment variables or current directory.
    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct ApplicationConfig {
        /// Application name.
        name: String,
        /// Storage mode configuration.
        mode: StorageMode,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum StorageMode {
        Memory,
        Disk { path: String, max_size: u64 },
    }

    impl Config for ApplicationConfig {
        const LIST_PARSE_KEYS: &'static [&'static str] = &[];
    }

    fn create_mock_config() -> ApplicationConfig {
        ApplicationConfig {
            name: "test-app".to_string(),
            mode: StorageMode::Disk {
                path: "/tmp/data".to_string(),
                max_size: 1024,
            },
        }
    }

    fn test_roundtrip_with_extension(extension: &str) {
        let _guard = env_lock().lock().unwrap();

        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        let original_config = create_mock_config();

        // Write base file (empty)
        let base_file = config_dir.join(format!("base.{extension}"));
        let base_content = match extension {
            "json" => "{}",
            "yaml" | "yml" => "",
            _ => panic!("Unsupported extension: {extension}"),
        };
        fs::write(&base_file, base_content).unwrap();

        // Write environment file with actual config
        let env_file = config_dir.join(format!("prod.{extension}"));
        let env_content = match extension {
            "json" => serde_json::to_string_pretty(&original_config).unwrap(),
            "yaml" | "yml" => {
                // YAML serialization for externally tagged enums doesn't match what config crate expects
                // For now, manually construct YAML that config crate can deserialize
                format!(
                    "name: \"{}\"\nmode:\n  disk:\n    path: \"{}\"\n    max_size: {}\n",
                    original_config.name,
                    match &original_config.mode {
                        StorageMode::Disk { path, .. } => path.as_str(),
                        _ => panic!("Expected Disk variant"),
                    },
                    match &original_config.mode {
                        StorageMode::Disk { max_size, .. } => max_size,
                        _ => panic!("Expected Disk variant"),
                    }
                )
            }
            _ => panic!("Unsupported extension: {extension}"),
        };
        fs::write(&env_file, env_content).unwrap();

        // Set environment and working directory
        unsafe {
            std::env::remove_var("APP_CONFIG_DIR");
            std::env::set_var("APP_ENVIRONMENT", "prod");
        }
        std::env::set_current_dir(temp_dir.path()).unwrap();

        // Load config
        let loaded_config: ApplicationConfig = load_config().unwrap();

        // Verify the loaded config matches the original exactly
        assert_eq!(loaded_config, original_config);
    }

    #[test]
    fn test_roundtrip_json() {
        test_roundtrip_with_extension("json");
    }

    #[test]
    fn test_roundtrip_yaml() {
        test_roundtrip_with_extension("yaml");
    }

    #[test]
    fn test_roundtrip_yml() {
        test_roundtrip_with_extension("yml");
    }

    #[test]
    fn test_all_supported_extensions_detected() {
        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        // Test each supported extension is correctly detected
        for extension in CONFIG_FILE_EXTENSIONS {
            let test_file = config_dir.join(format!("base.{extension}"));
            fs::write(&test_file, "{}").unwrap();

            let result = find_configuration_file(&config_dir, ConfigFileKind::Base);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), test_file);

            // Clean up for next iteration
            fs::remove_file(&test_file).unwrap();
        }
    }

    #[test]
    fn test_app_config_dir_env_var() {
        let _guard = env_lock().lock().unwrap();

        let temp_dir = TempDir::new().unwrap();
        // Note: NOT using "configuration" subdirectory, using a custom path
        let custom_config_dir = temp_dir.path().join("my-custom-config");
        fs::create_dir(&custom_config_dir).unwrap();

        let original_config = create_mock_config();

        // Write base and environment files
        let base_file = custom_config_dir.join("base.json");
        fs::write(&base_file, "{}").unwrap();

        let env_file = custom_config_dir.join("prod.json");
        let env_content = serde_json::to_string_pretty(&original_config).unwrap();
        fs::write(&env_file, env_content).unwrap();

        // Set APP_CONFIG_DIR to the custom path (absolute path)
        unsafe {
            std::env::set_var("APP_CONFIG_DIR", custom_config_dir.to_str().unwrap());
            std::env::set_var("APP_ENVIRONMENT", "prod");
        }

        // Load config - should use APP_CONFIG_DIR, not current_dir/configuration
        let loaded_config: ApplicationConfig = load_config().unwrap();

        // Verify the loaded config matches the original
        assert_eq!(loaded_config, original_config);

        // Clean up
        unsafe {
            std::env::remove_var("APP_CONFIG_DIR");
        }
    }

    #[test]
    fn test_fallback_to_current_dir_when_app_config_dir_not_set() {
        let _guard = env_lock().lock().unwrap();

        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        let original_config = create_mock_config();

        // Write base and environment files
        let base_file = config_dir.join("base.json");
        fs::write(&base_file, "{}").unwrap();

        let env_file = config_dir.join("prod.json");
        let env_content = serde_json::to_string_pretty(&original_config).unwrap();
        fs::write(&env_file, env_content).unwrap();

        // Ensure APP_CONFIG_DIR is not set
        unsafe {
            std::env::remove_var("APP_CONFIG_DIR");
            std::env::set_var("APP_ENVIRONMENT", "prod");
        }
        std::env::set_current_dir(temp_dir.path()).unwrap();

        // Load config - should fallback to current_dir/configuration
        let loaded_config: ApplicationConfig = load_config().unwrap();

        // Verify the loaded config matches the original
        assert_eq!(loaded_config, original_config);
    }
}
