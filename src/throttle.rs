use anyhow::anyhow;
use bytesize::ByteSize;
use derive_setters::Setters;
use opendal::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// Throttling parameters
///
/// Note: Throttle implements [`FromStr`] to read it from something like "10kiB,10MB"
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Setters)]
#[setters(into)]
#[non_exhaustive]
pub struct Throttle {
    /// The maximum bandwidth to use, in bits per second.
    pub bandwidth: u32,
    /// The maximum "burst" to use, in bits per second.
    pub burst: u32,
}

impl Default for Throttle {
    fn default() -> Self {
        Self {
            bandwidth: u32::MAX,
            burst: u32::MAX,
        }
    }
}

impl Display for Throttle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{}",
            ByteSize::b(u64::from(self.bandwidth)),
            ByteSize::b(u64::from(self.burst)),
        )
    }
}

impl FromStr for Throttle {
    type Err = Error;

    fn from_str(s: &str) -> opendal::Result<Self> {
        let mut values = s
            .split(',')
            .map(|s| {
                ByteSize::from_str(s.trim()).map_err(|err| {
                    Error::new(ErrorKind::ConfigInvalid, "Parsing ByteSize failed")
                        .with_context("input", s)
                        .set_source(anyhow!(err))
                        .set_permanent()
                })
            })
            .map(|b| -> opendal::Result<u32> {
                b?.as_u64().try_into().map_err(|err| {
                    Error::new(ErrorKind::ConfigInvalid, "Parsing ByteSize failed").set_source(err)
                })
            });

        let bandwidth = values
            .next()
            .transpose()?
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "No bandwidth given."))?;

        let burst = values
            .next()
            .transpose()?
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "No burst given."))?;

        Ok(Self { bandwidth, burst })
    }
}
