// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::time::Duration;
use opendal::{Builder, Configurator};
use opendal::raw::Access;
use rustic_core::{Credentials, RepositoryOptions, BackendOptions};
use serde::{Deserialize, Serialize};
use crate::rustic_be::VfsBackend;

/// Configuration for the Rustic VFS OpenDAL backend.
///
/// `RusticVfsConfig` holds all settings required to construct a
/// [`RusticVfsBuilder`] and ultimately a [`VfsBackend`] backed by a
/// [rustic](https://github.com/rustic-rs/rustic) repository. It implements
/// [`Configurator`], so it can be handed directly to OpenDAL's service-builder
/// machinery via [`into_builder`](Configurator::into_builder).
///
/// This config is **rustic-specific** — it is not a general-purpose VFS
/// abstraction. All fields map directly to rustic concepts and are forwarded
/// as-is to the underlying rustic storage layer.
///
/// # Field requirements
///
/// All fields are wrapped in `Option` so that [`Default`] can be derived and
/// configs can be partially constructed (e.g. loaded from a file and then
/// patched). However, some fields are **logically required** and will cause
/// [`RusticVfsBuilder::build`] to return a
/// [`ConfigInvalid`](opendal_core::ErrorKind::ConfigInvalid) error if left
/// as `None`:
///
/// | Field              | Required at build time | Notes |
/// |--------------------|------------------------|-------|
/// | `options`          | ✓                      | Must describe a valid repository |
/// | `backend`          | ✓                      | Must point to reachable storage |
/// | `credentials`      | ✓                      | Repository will not open without these |
/// | `refresh_interval` | –                      | Defaults to 5 minutes when `None` |
///
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
//#[doc = include_str!("docs.md")]
pub struct RusticVfsConfig {
    /// Options that describe the rustic repository to open.
    ///
    /// Controls how the repository is located (path, URL) and how its password
    /// is sourced (env var, file, command). Forwarded verbatim to
    /// [`Repository::new`](rustic_core::Repository::new).
    pub options: RepositoryOptions,

    /// Low-level backend options forwarded to the rustic storage layer.
    ///
    /// Governs how rustic physically accesses its storage (local disk, S3,
    /// SFTP, rclone, etc.), including repository and hot-cache paths.
    pub backend: BackendOptions,

    /// Credentials used to authenticate against the rustic repository.
    ///
    /// Passed to [`Repository::open`](rustic_core::Repository::open). There
    /// is no ambient/fallback credential lookup — these must be supplied
    /// explicitly. If authentication fails a
    /// [`ConfigInvalid`](opendal_core::ErrorKind::ConfigInvalid) error is
    /// returned at build time.
    ///
    /// **Logically required** — [`build`](Builder::build) returns
    /// [`ConfigInvalid`](opendal_core::ErrorKind::ConfigInvalid) if `None`.
    /// Wrapped in `Option` solely to satisfy [`Default`].
    pub credentials: Option<Credentials>,

    /// How often the VFS layer should re-read the rustic repository index.
    ///
    /// When set, a background task wakes at this cadence, rebuilds the
    /// in-memory VFS from the latest snapshots, and swaps it in atomically so
    /// that snapshots written by other writers become visible without restarting
    /// the process.
    ///
    /// Serialized / deserialized as a human-readable duration string
    /// (e.g. `"30s"`, `"5m"`) via [`humantime_serde`]. Defaults to **5
    /// minutes** when `None`.
    #[serde(default, with = "humantime_serde")]
    pub refresh_interval: Option<Duration>,
}

impl Configurator for RusticVfsConfig {
    type Builder = RusticVfsBuilder;

    /// Consumes the config and returns a [`RusticVfsBuilder`] ready for
    /// further customization or an immediate call to [`build`](Builder::build).
    fn into_builder(self) -> Self::Builder {
        RusticVfsBuilder { config: self }
    }
}

/// Builder for the Rustic VFS OpenDAL backend.
///
/// `RusticVfsBuilder` wraps a [`RusticVfsConfig`] and exposes a fluent setter
/// API so that individual fields can be overridden after a config has been
/// loaded (e.g. from a file) but before the backend is constructed.
///
/// This builder is **rustic-specific** — it targets rustic repositories
/// exclusively and is not a general-purpose VFS builder. All setters map
/// directly to rustic concepts.
///
/// Obtain a builder either via [`RusticVfsConfig::into_builder`] or from the
/// [`Default`] impl when starting from a blank slate.
///
/// # Required setters
///
/// [`with_options`](RusticVfsBuilder::with_options),
/// [`with_backend`](RusticVfsBuilder::with_backend), and
/// [`with_credentials`](RusticVfsBuilder::with_credentials) **must** be called
/// before [`build`](Builder::build); omitting any of them will return a
/// [`ConfigInvalid`](opendal_core::ErrorKind::ConfigInvalid) error.
///
#[derive(Debug, Default, Clone)]
pub struct RusticVfsBuilder {
    pub(super) config: RusticVfsConfig,
}

impl RusticVfsBuilder {
    /// Sets the [`RepositoryOptions`] that describe the rustic repository.
    ///
    /// Controls how the repository is located and opened (path, URL, password
    /// source, etc.). Forwarded verbatim to the rustic layer.
    ///
    /// **Required before [`build`](Builder::build).**
    ///
    /// # Arguments
    ///
    /// * `options` – Repository options describing the target rustic repository.
    pub fn with_options(mut self, options: RepositoryOptions) -> Self {
        self.config.options = options;
        self
    }

    /// Sets the [`BackendOptions`] used by the rustic storage layer.
    ///
    /// Governs how rustic physically accesses its storage (local disk, S3,
    /// SFTP, rclone, etc.), including repository and hot-cache paths.
    ///
    /// **Required before [`build`](Builder::build).**
    ///
    /// # Arguments
    ///
    /// * `backend` – Low-level storage backend options for the rustic backend.
    pub fn with_backend(mut self, backend: BackendOptions) -> Self {
        self.config.backend = backend;
        self
    }

    /// Sets the [`Credentials`] used to authenticate against the rustic
    /// repository.
    ///
    /// There is no ambient/fallback credential lookup — credentials must be
    /// supplied explicitly here or via [`RusticVfsConfig`].
    ///
    /// **Required before [`build`](Builder::build).**
    ///
    /// # Arguments
    ///
    /// * `credentials` – Credentials for rustic repository authentication.
    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        self.config.credentials = Some(credentials);
        self
    }

    /// Sets the periodic refresh interval for the rustic index cache.
    ///
    /// When set, a background task rebuilds the in-memory VFS at this cadence
    /// so that snapshots written by other writers become visible without
    /// restarting the process. Pass `None` to fall back to the default of
    /// **5 minutes**.
    ///
    /// # Arguments
    ///
    /// * `interval` – How often to re-read the rustic repository index.
    pub fn with_refresh_interval(mut self, interval: impl Into<Option<Duration>>) -> Self {
        self.config.refresh_interval = interval.into();
        self
    }
}

impl Builder for RusticVfsBuilder {
    type Config = RusticVfsConfig;

    /// Consumes the builder and constructs a [`VfsBackend`] from the current
    /// rustic configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigInvalid`](opendal_core::ErrorKind::ConfigInvalid) if
    /// any of the logically required fields (`options`, `backend`,
    /// `credentials`) were not set, or if the rustic repository cannot be
    /// opened with the supplied configuration. Returns
    /// [`Unexpected`](opendal_core::ErrorKind::Unexpected) for transient
    /// failures (e.g. the storage backend is temporarily unreachable).
    fn build(self) -> opendal::Result<impl Access> {
        VfsBackend::from_config(self.config)
    }
}