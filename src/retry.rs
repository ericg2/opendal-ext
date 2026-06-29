use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
use opendal::{Error, ErrorKind};

#[serde_as]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// The retry policy for all calls.
pub enum RetrySetting {
    /// No retry policy set.
    Disabled,
    #[default]
    /// Use the default retry policy.
    Default,
    /// Use a custom policy of # times.
    Count(usize),
}

impl RetrySetting {
    /// Retrieves the # of times to retry. Based on `def` default.
    pub fn get_setting(&self, def: usize) -> usize {
        match self {
            RetrySetting::Disabled => 0,
            RetrySetting::Default => def,
            RetrySetting::Count(x) => *x,
        }
    }
}

impl Display for RetrySetting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => write!(f, "off"),
            Self::Default => write!(f, "default"),
            Self::Count(n) => write!(f, "{n}"),
        }
    }
}

impl FromStr for RetrySetting {
    type Err = Error;

    fn from_str(s: &str) -> opendal::Result<Self> {
        match s.to_lowercase().as_str() {
            "false" | "off" => Ok(Self::Disabled),
            "default" => Ok(Self::Default),
            value => {
                let count = value.parse::<usize>().map_err(|err| {
                    Error::new(
                        ErrorKind::ConfigInvalid,
                        format!(
                            "Parsing retry value `{value}` failed, the value must be a valid integer."
                        ),
                    )
                        .with_context("value", value.to_string())
                        .set_source(err)
                        .set_permanent()
                })?;

                Ok(Self::Count(count))
            }
        }
    }
}