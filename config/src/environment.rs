use std::fmt;
use std::io::Error;

/// Names of the environment variable which contains the environment name.
const APP_ENVIRONMENT_ENV_NAME: &str = "APP_ENVIRONMENT";

/// The name of the production environment.
const PROD_ENV_NAME: &str = "prod";

/// The name of the staging environment.
const STAGING_ENV_NAME: &str = "staging";

/// The name of the development environment.
const DEV_ENV_NAME: &str = "dev";

/// Represents the runtime environment for the application.
///
/// Use [`Environment`] to distinguish between development and production modes.
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
    /// Loads the environment from the `APP_ENVIRONMENT` env variable.
    ///
    /// In case no environment is specified, we default to [`Environment::Prod`].
    pub fn load() -> Result<Environment, Error> {
        std::env::var(APP_ENVIRONMENT_ENV_NAME)
            .unwrap_or_else(|_| PROD_ENV_NAME.into())
            .try_into()
    }

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

    /// Attempts to create an [`Environment`] from a string, case-insensitively.
    ///
    /// Accepts "dev" or "prod". Returns an error if the input does not match a supported environment.
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
