use std::fmt;
use std::io::Error;

/// Environment variable name containing the environment identifier.
const APP_ENVIRONMENT_ENV_NAME: &str = "APP_ENVIRONMENT";

/// Production environment identifier.
const PROD_ENV_NAME: &str = "prod";

/// Staging environment identifier.
const STAGING_ENV_NAME: &str = "staging";

/// Development environment identifier.
const DEV_ENV_NAME: &str = "dev";

/// Runtime environment for the application.
///
/// Used to distinguish between development, staging, and production modes
/// for configuration loading and feature toggles.
#[derive(Debug, Clone)]
pub enum Environment {
    /// Production environment.
    Prod,
    /// Staging environment.
    Staging,
    /// Development environment.
    Dev,
}

impl Environment {
    /// Loads the environment from the `APP_ENVIRONMENT` environment variable.
    ///
    /// Defaults to [`Environment::Prod`] if the variable is not set.
    pub fn load() -> Result<Environment, Error> {
        std::env::var(APP_ENVIRONMENT_ENV_NAME)
            .unwrap_or_else(|_| PROD_ENV_NAME.into())
            .try_into()
    }

    /// Sets the `APP_ENVIRONMENT` environment variable to this environment's value.
    pub fn set(&self) {
        unsafe { std::env::set_var(APP_ENVIRONMENT_ENV_NAME, self.to_string()) }
    }

    /// Returns whether this is a production-like environment.
    ///
    /// Returns `true` for both [`Environment::Prod`] and [`Environment::Staging`].
    pub fn is_prod(&self) -> bool {
        matches!(self, Self::Prod | Self::Staging)
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Environment::Prod => write!(f, "{PROD_ENV_NAME}"),
            Environment::Staging => write!(f, "{STAGING_ENV_NAME}"),
            Environment::Dev => write!(f, "{DEV_ENV_NAME}"),
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = Error;

    /// Creates an [`Environment`] from a string, case-insensitively.
    ///
    /// Accepts "dev", "staging", or "prod". Returns an error for unsupported values.
    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            PROD_ENV_NAME => Ok(Self::Prod),
            STAGING_ENV_NAME => Ok(Self::Staging),
            DEV_ENV_NAME => Ok(Self::Dev),
            other => Err(Error::other(format!(
                "{other} is not a supported environment. Use either `{PROD_ENV_NAME}`/`{STAGING_ENV_NAME}`/`{DEV_ENV_NAME}`.",
            ))),
        }
    }
}
