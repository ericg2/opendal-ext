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

opendal_add!(
    B2 => opendal::services::B2Config: "services-b2",
    Ftp => opendal::services::FtpConfig: "services-ftp",
    Swift => opendal::services::SwiftConfig: "services-swift",
    Azblob => opendal::services::AzblobConfig: "services-azblob",
    Azdls => opendal::services::AzdlsConfig: "services-azdls",
    Azfile => opendal::services::AzfileConfig: "services-azfile",
    Cos => opendal::services::CosConfig: "services-cos",
    Fs => opendal::services::FsConfig: "services-fs",
    Dropbox => opendal::services::DropboxConfig: "services-dropbox",
    Gdrive => opendal::services::GdriveConfig: "services-gdrive",
    Gcs => opendal::services::GcsConfig: "services-gcs",
    Ghac => opendal::services::GhacConfig: "services-ghac",
    Http => opendal::services::HttpConfig: "services-http",
    Ipmfs => opendal::services::IpmfsConfig: "services-ipmfs",
    Memory => opendal::services::MemoryConfig: "services-memory",
    Obs => opendal::services::ObsConfig: "services-obs",
    Onedrive => opendal::services::OnedriveConfig: "services-onedrive",
    Oss => opendal::services::OssConfig: "services-oss",
    Pcloud => opendal::services::PcloudConfig: "services-pcloud",
    S3 => opendal::services::S3Config: "services-s3",
    Webdav => opendal::services::WebdavConfig: "services-webdav",
    Webhdfs => opendal::services::WebhdfsConfig: "services-webhdfs",
    YandexDisk => opendal::services::YandexDiskConfig: "services-yandex-disk",
    Sftp => opendal::services::SftpConfig: "services-sftp",
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
            config: Scheme::dynamic(scheme, map),
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
