use std::fmt;
use std::io::Error;

/// Names of the environment variable which contains the environment name.
const APP_ENVIRONMENT_ENV_NAME: &str = "APP_ENVIRONMENT";

/// The name of the development environment.
const DEV_ENV_NAME: &str = "dev";

/// The name of the production environment.
const PROD_ENV_NAME: &str = "prod";

/// Represents the runtime environment for the application.
///
/// Use [`Environment`] to distinguish between development and production modes.
#[derive(Debug, Clone)]
pub enum Environment {
    /// Development environment.
    Dev,
    /// Production environment.
    Prod,
}

impl Environment {
    /// Loads the environment from the `APP_ENVIRONMENT` env variable.
    pub fn load() -> Result<Environment, Error> {
        std::env::var(APP_ENVIRONMENT_ENV_NAME)
            .unwrap_or_else(|_| DEV_ENV_NAME.into())
            .try_into()
    }

    /// Returns the string name of the environment.
    ///
    /// The returned value matches the corresponding constant, such as [`DEV_ENV_NAME`] or [`PROD_ENV_NAME`].
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Dev => DEV_ENV_NAME,
            Environment::Prod => PROD_ENV_NAME,
        }
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Environment::Dev => write!(f, "{DEV_ENV_NAME}"),
            Environment::Prod => write!(f, "{PROD_ENV_NAME}"),
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
            DEV_ENV_NAME => Ok(Self::Dev),
            PROD_ENV_NAME => Ok(Self::Prod),
            other => Err(Error::other(format!(
                "{other} is not a supported environment. Use either `{DEV_ENV_NAME}` or `{PROD_ENV_NAME}`.",
            ))),
        }
    }
}
