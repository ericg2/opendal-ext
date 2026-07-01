#![allow(unused_qualifications)]

use crate::retry::RetrySetting;
use crate::{Throttle, macros::opendal_add};
use derive_setters::Setters;
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, RetryLayer, ThrottleLayer};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[cfg(all(feature = "sftp", windows))]
compile_error!("sftp service is not supported on windows");

opendal_add!(
    B2 => "b2",
    Ftp => "ftp",
    Swift => "swift",
    Azblob => "azblob",
    Azdls => "azdls",
    Azfile => "azfile",
    Cos => "cos",
    Fs => "fs",
    Dropbox => "dropbox",
    Gdrive => "gdrive",
    Gcs => "gcs",
    Ghac => "ghac",
    Http => "http",
    Ipmfs => "ipmfs",
    Memory => "memory",
    Obs => "obs",
    Onedrive => "onedrive",
    Oss => "oss",
    Pcloud => "pcloud",
    S3 => "s3",
    Webdav => "webdav",
    Webhdfs => "webhdfs",
    YandexDisk => "yandex-disk",
    Sftp => "sftp",
);

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Setters, Default)]
#[serde(rename_all = "kebab-case")]
#[setters(into)]
#[non_exhaustive]
/// Represents a openDAL repository.
pub struct OpenDALConfig {
    /// The maximum connections.
    #[serde(alias = "connections", alias = "max_connections")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub connections: Option<u32>,

    /// The [`crate::opendal::throttle::Throttle`] settings.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub throttle: Option<Throttle>,

    /// The [`RetrySetting`] config.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default)]
    pub retry: RetrySetting,

    /// The serialized config.
    #[setters(skip)]
    #[serde(flatten)]
    pub config: Scheme,
}

impl From<Scheme> for OpenDALConfig {
    fn from(value: Scheme) -> Self {
        OpenDALConfig::new(&value)
    }
}

impl OpenDALConfig {
    /// Creates an [`OpenDALConfig`] from an iterator.
    ///
    /// # Important
    /// This does not guarantee the [`OpenDALConfig`] is initialized correctly. Due to the
    /// nature of dynamic types - this feature is only a convenience. All invalid fields will
    /// be skipped, and will not return an error during this process.
    pub fn from_iter<K, V, I>(scheme: impl AsRef<str>, dict: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut map: HashMap<String, String> = dict
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        let connections = map
            .remove("connections")
            .or_else(|| map.remove("max_connections"))
            .and_then(|v| v.parse::<u32>().ok());

        let throttle = map
            .remove("throttle")
            .and_then(|v| v.parse::<Throttle>().ok());

        let retry = map
            .remove("retry")
            .and_then(|v| v.parse::<RetrySetting>().ok())
            .unwrap_or_default();

        Self {
            connections,
            throttle,
            retry,
            config: Scheme::dynamic(scheme.as_ref(), map.clone())
        }
    }

    /// Creates a new openDAL backend via a [`Scheme`].
    ///
    ///
    /// # Arguments
    ///
    /// * `be` - The [`Scheme`] to use.
    pub fn new(be: &Scheme) -> Self {
        Self {
            config: be.clone(),
            retry: RetrySetting::Default,
            connections: None,
            throttle: None,
        }
    }

    /// # Returns
    ///
    /// The associated [`Scheme`] with this [`OpenDALConfig`].
    pub fn scheme(&self) -> &Scheme {
        &self.config
    }

    /// Creates an [`Operator`] from the current config.
    pub fn operator(&self) -> opendal::Result<Operator> {
        let mut op = self.scheme().operator()?;
        let retry = self.retry.get_setting(5);
        op = op.layer(RetryLayer::new().with_max_times(retry).with_jitter());

        if let Some(x) = self.connections {
            op = op.layer(ConcurrentLimitLayer::new(x as usize));
        }

        if let Some(ref x) = self.throttle {
            op = op.layer(ThrottleLayer::new(x.bandwidth, x.burst));
        }

        Ok(op)
    }
}

impl TryFrom<OpenDALConfig> for Operator {
    type Error = opendal::Error;

    fn try_from(config: OpenDALConfig) -> Result<Self, Self::Error> {
        config.operator()
    }
}
