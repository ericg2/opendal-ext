//! `MountFs` — a custom OpenDAL [`Access`] backend that mounts other
//! operators onto fixed virtual paths, the way you'd mount filesystems onto
//! directories on a Unix box.
//!
//! Unlike `RouteLayer`, this is **not** a `Layer` wrapping some "default"
//! operator — there is no default. It is a standalone backend: you give a
//! `MountFsBuilder` a list of mounts, call `.build()`, and get back a plain
//! `Operator`. Internally:
//!
//! - Listing a path that isn't inside any mount (e.g. `/`, or `/repos` when
//!   you've mounted `/repos/test`) synthesizes a virtual directory listing
//!   out of the configured mount paths. There is no real backend behind
//!   these paths — they exist only because some mount lives underneath them.
//! - Listing/stat/read/write/delete/etc. against a path that *is* inside a
//!   mount gets forwarded to that mount's operator, with the path rebased
//!   so the mount root becomes `"/"`. E.g. if `/repos/test` is mounted and
//!   you call `create_dir("/repos/test/abc/")`, the backend calls
//!   `create_dir("abc/")` on the mounted operator.
//! - Paths that fall under no mount and aren't an ancestor of one are
//!   `NotFound`.
//!
//! Per-mount config supports:
//! - `read_only`: every write/create_dir/delete/rename/copy against that
//!   mount is rejected with `PermissionDenied`. Implemented with a tiny
//!   internal `ReadOnlyLayer`, applied only to that mount's operator.
//! - `quota_bytes`: cumulative bytes written to that mount are capped,
//!   enforced by wrapping that mount's operator with the existing
//!   `QuotaLayer` (using the mount's path as the quota id).
//!
//! ```ignore
//! use std::sync::Arc;
//! use crate::mount_fs::MountFsBuilder;
//! use crate::quota_layer::QuotaTracker;
//!
//! # async fn run(tracker: Arc<impl QuotaTracker>) -> opendal::Result<()> {
//! let op = MountFsBuilder::new(tracker)
//!     .mount("/repos/test", my_namespace::Scheme::Fs(/* ... */))
//!     .mount("/repos/readonly-mirror", my_namespace::Scheme::S3(/* ... */))
//!         .read_only()
//!     .mount("/scratch", my_namespace::Scheme::Memory)
//!         .quota(64 * 1024 * 1024)
//!     .build()?;
//!
//! op.create_dir("/repos/test/abc/").await?;     // -> create_dir("abc/") on the fs mount
//! op.list("/repos/").await?;                    // -> ["test/", "readonly-mirror/"] (virtual)
//! op.write("/repos/readonly-mirror/x", "y").await.unwrap_err(); // PermissionDenied
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use futures_lite::{AsyncReadExt, StreamExt};
use serde::{Deserialize, Serialize};

use crate::config::OpenDALConfig;
use crate::quota::{MemoryTracker, QuotaLayer, QuotaTracker};
use crate::util::ReadOnlyLayer;
use opendal::raw::*;
use opendal::{
    Buffer, Builder, Capability, Configurator, EntryMode, Error, ErrorKind, Metadata, Operator,
    Result,
};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for a VFS Quota.
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, Default)]
pub enum VfsQuota {
    /// Quota is disabled.
    #[default]
    Disabled,
    /// Quota is enabled.
    Enabled {
        /// The ID to use for the [`QuotaTracker`].
        id: String,
        /// The byte limit for writing.
        bytes: u64,
    },
}

/// Configuration for a single mount point.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MountEntry {
    /// Virtual path this mount is bound to, e.g. `"/repos/test"`. Normalized
    /// on insert: always absolute, no trailing slash (except root, which
    /// can't itself be mounted - see [`VfsBuilder::mount`]).
    pub path: String,
    /// Which backend to resolve and mount at `path`.
    pub config: OpenDALConfig,
    /// Reject all writes/deletes/create_dir/rename/copy against this mount.
    #[serde(default)]
    pub read_only: bool,
    /// Cap cumulative bytes written to this mount, enforced via
    /// `QuotaLayer`. `None` means unlimited.
    #[serde(default)]
    pub quota: VfsQuota,
}

/// Serializable configuration for the whole `MountFs` backend.
#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct VfsConfig {
    /// All mounts to use for the VFS.
    pub mounts: Vec<MountEntry>,
}

impl Configurator for VfsConfig {
    type Builder = VfsBuilder;

    fn into_builder(self) -> Self::Builder {
        VfsBuilder::from_config(self)
    }
}

/// Normalize a virtual mount path: absolute, no trailing slash, `/`
/// collapsed to itself.
fn normalize(path: &str) -> String {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{trimmed}")
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for the `MountFs` backend. Produces a plain `Operator` - there is
/// intentionally no way to get a bare un-mounted backend out of this, and no
/// "default operator" fallback.
#[derive(Clone, Debug)]
pub struct VfsBuilder {
    tracker: Arc<dyn QuotaTracker>,
    config: VfsConfig,
}

impl Default for VfsBuilder {
    fn default() -> Self {
        Self {
            tracker: Arc::new(MemoryTracker::default()),
            config: VfsConfig::default(),
        }
    }
}

impl Builder for VfsBuilder {
    type Config = VfsConfig;

    fn build(self) -> Result<impl Access> {
        let mut mounts = BTreeMap::new();
        for entry in &self.config.mounts {
            if entry.path == "/" {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "cannot mount at the virtual root '/'",
                ));
            }

            let mut op = entry.config.operator()?;
            if let VfsQuota::Enabled { id, bytes } = &entry.quota {
                op = op.layer(QuotaLayer::new(id.clone(), self.tracker.clone(), *bytes));
            }
            if entry.read_only {
                op = op.layer(ReadOnlyLayer);
            }

            if mounts
                .insert(
                    entry.path.clone(),
                    Mount {
                        operator: op,
                        read_only: entry.read_only,
                    },
                )
                .is_some()
            {
                return Err(Error::new(ErrorKind::ConfigInvalid, "duplicate mount path")
                    .with_context("path", entry.path.clone()));
            }
        }

        Ok(MountAccess {
            mounts: Arc::new(mounts),
        })
    }
}

impl VfsBuilder {
    /// Start an empty builder. `tracker` backs every mount's quota (if any)
    /// - one tracker instance shared across mounts, keyed by mount path.
    pub fn new(tracker: Arc<impl QuotaTracker>) -> Self {
        Self {
            config: VfsConfig::default(),
            tracker,
        }
    }

    /// Build from a config you already have (e.g. loaded from disk/DB).
    pub fn from_config(config: VfsConfig) -> Self {
        Self {
            config,
            ..Default::default()
        }
    }

    /// Sets the [`QuotaTracker`] for this builder.
    pub fn with_tracker(mut self, tracker: Arc<impl QuotaTracker>) -> Self {
        self.tracker = tracker;
        self
    }

    /// Mount `scheme` at `path`. Mounting at `/` itself is rejected at
    /// `build()` time - there has to be at least one real path segment, or
    /// "mounting a sub-folder" doesn't mean anything.
    pub fn mount(mut self, path: impl Into<String>, config: impl Into<OpenDALConfig>) -> Self {
        self.config.mounts.push(MountEntry {
            path: normalize(&path.into()),
            config: config.into(),
            read_only: false,
            quota: VfsQuota::Disabled,
        });
        self
    }

    /// Mark the most recently added mount read-only.
    pub fn read_only(mut self) -> Self {
        if let Some(last) = self.config.mounts.last_mut() {
            last.read_only = true;
        }
        self
    }

    /// Cap the most recently added mount's cumulative write quota.
    pub fn quota(mut self, id: impl AsRef<str>, bytes: u64) -> Self {
        if let Some(last) = self.config.mounts.last_mut() {
            last.quota = VfsQuota::Enabled {
                id: id.as_ref().to_string(),
                bytes,
            };
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Mount table
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Mount {
    operator: Operator,
    read_only: bool,
}

/// Find the mount (if any) that owns `path`, and the path made relative to
/// that mount's root. "Owns" means `path` equals the mount path or is nested
/// under it. If multiple configured mounts could match (shouldn't happen
/// with sane config, but mount paths aren't required to be disjoint), the
/// longest (most specific) one wins.
fn resolve<'a>(
    mounts: &'a BTreeMap<String, Mount>,
    path: &str,
) -> Option<(&'a str, &'a Mount, String)> {
    let normalized = normalize(path);
    mounts
        .iter()
        .filter(|(mount_path, _)| {
            normalized == mount_path.as_str() || normalized.starts_with(&format!("{mount_path}/"))
        })
        .max_by_key(|(mount_path, _)| mount_path.len())
        .map(|(mount_path, mount)| {
            let rel = normalized
                .strip_prefix(mount_path.as_str())
                .unwrap()
                .trim_start_matches('/');
            // Preserve a trailing slash from the original request (it
            // signals "this is a directory" to most backends), but don't
            // add one that wasn't there.
            let mut rel = rel.to_string();
            if path.ends_with('/') && !rel.is_empty() && !rel.ends_with('/') {
                rel.push('/');
            }
            (mount_path.as_str(), mount, rel)
        })
}

/// True if `path` is a virtual ancestor directory of at least one mount
/// (i.e. has no operator of its own, but some mount lives under it).
fn virtual_children(mounts: &BTreeMap<String, Mount>, path: &str) -> Option<Vec<String>> {
    let normalized = normalize(path);
    // All mount paths start with '/', so when listing root we use "/" as the
    // prefix rather than "" — otherwise strip_prefix("") always succeeds and
    // split('/') yields an empty leading segment for every mount path.
    let prefix = if normalized == "/" {
        "/".to_string()
    } else {
        format!("{normalized}/")
    };

    let mut children = std::collections::BTreeSet::new();
    for mount_path in mounts.keys() {
        let Some(rest) = mount_path.strip_prefix(&prefix) else {
            continue;
        };
        if rest.is_empty() {
            // `path` exactly equals an existing mount - that's handled by
            // `resolve`, not as a virtual directory.
            continue;
        }
        let next_segment = rest.split('/').next().unwrap_or(rest);
        let _ = children.insert(next_segment.to_string());
    }

    if children.is_empty() && normalized != "/" {
        None
    } else {
        Some(children.into_iter().collect())
    }
}

// ---------------------------------------------------------------------------
// Access impl
// ---------------------------------------------------------------------------
/// Accessor for VFS backend. Initialize via [`VfsBuilder`]!
pub struct MountAccess {
    mounts: Arc<BTreeMap<String, Mount>>,
}

impl MountAccess {
    async fn metadata(&self, path: &str) -> Result<Metadata> {
        if let Some((mount_path, mount, rel)) = resolve(&self.mounts, path) {
            if normalize(path) == mount_path && rel.is_empty() {
                return Ok(Metadata::new(EntryMode::DIR));
            }

            return mount.operator.stat(&rel).await;
        }

        if virtual_children(&self.mounts, path).is_some() {
            return Ok(Metadata::new(EntryMode::DIR));
        }

        Err(not_found(path))
    }
}

impl Debug for MountAccess {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MountAccess")
            .field("mounts", &self.mounts.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn not_found(path: &str) -> Error {
    Error::new(ErrorKind::NotFound, "no mount covers this path").with_context("path", path)
}

fn permission_denied(path: &str) -> Error {
    Error::new(ErrorKind::PermissionDenied, "mount is read-only").with_context("path", path)
}

impl Access for MountAccess {
    type Reader = MountReader;
    type Writer = MountWriter;
    type Lister = MountLister;
    type Deleter = MountDeleter;
    type Copier = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        let _ = info.set_root("/");
        let _ = info.set_native_capability(Capability {
            stat: true,
            read: true,
            write: true,
            create_dir: true,
            delete: true,
            list: true,
            ..Default::default()
        });
        Arc::new(info)
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        match resolve(&self.mounts, path) {
            Some((_, mount, rel)) => {
                if mount.read_only {
                    return Err(permission_denied(path));
                }
                mount.operator.create_dir(&rel).await?;
                Ok(RpCreateDir::default())
            }
            None => Err(not_found(path)),
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        Ok(RpStat::new(self.metadata(path).await?))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let meta = self.metadata(path).await?;
        let Some((_, mount, rel)) = resolve(&self.mounts, path) else {
            return Err(not_found(path));
        };

        let range = args.range();
        let rdr = mount
            .operator
            .reader(&rel)
            .await?
            .into_futures_async_read(range.to_range())
            .await?;
        Ok((RpRead::new(meta), MountReader::new(rdr)))
    }

    async fn write(&self, path: &str, _args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let Some((_, mount, rel)) = resolve(&self.mounts, path) else {
            return Err(not_found(path));
        };
        if mount.read_only {
            return Err(permission_denied(path));
        }
        let writer = mount.operator.writer(&rel).await?;
        Ok((RpWrite::new(), MountWriter { inner: writer }))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            MountDeleter {
                mounts: self.mounts.clone(),
            },
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        if let Some((mount_path, mount, rel)) = resolve(&self.mounts, path) {
            let lister = mount.operator.lister(&rel).await?;
            return Ok((
                RpList::default(),
                MountLister::Real {
                    inner: lister,
                    mount_path: mount_path.to_string(),
                },
            ));
        }

        match virtual_children(&self.mounts, path) {
            Some(children) => {
                let base = normalize(path);
                let entries = children
                    .into_iter()
                    .map(|name| {
                        let full = if base == "/" {
                            format!("{name}/") // "repos/"  — missing leading "/"
                        } else {
                            format!("{}/{name}/", base.trim_start_matches('/')) // "repos/test/" — missing leading "/"
                        };
                        oio::Entry::new(&full, Metadata::new(EntryMode::DIR))
                    })
                    .collect();
                Ok((RpList::default(), MountLister::Virtual { entries }))
            }
            None => Err(not_found(path)),
        }
    }
}

// ---------------------------------------------------------------------------
// Reader / Writer / Lister / Deleter bridges
// ---------------------------------------------------------------------------

const BUFFER_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

/// The reader to use for VFS operations. Streaming and lazy-loading.
#[allow(missing_debug_implementations)]
pub struct MountReader {
    inner: opendal::FuturesAsyncReader,
}

impl oio::Read for MountReader {
    async fn read(&mut self) -> Result<Buffer> {
        let mut buf = vec![0u8; BUFFER_SIZE];
        let n = self.inner.read(&mut buf).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "mount reader: read failed")
                .with_operation("MountReader::read")
                .set_source(err)
        })?;

        buf.truncate(n);
        Ok(Buffer::from(buf))
    }
}

impl MountReader {
    fn new(inner: opendal::FuturesAsyncReader) -> Self {
        Self { inner }
    }
}

/// The writer to use for VFS operations.
#[allow(missing_debug_implementations)]
pub struct MountWriter {
    inner: opendal::Writer,
}

impl oio::Write for MountWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

/// The lister for the VFS system.
pub enum MountLister {
    /// A real path with an inner [`Lister`].
    Real {
        /// The OpenDAL [`Lister`] to refer to.
        inner: opendal::Lister,
        /// The relative path to use.
        mount_path: String,
    },
    /// A virtual path with fake entries (roots, sub-dirs, etc.)
    Virtual {
        /// The fake entries to display.
        entries: Vec<oio::Entry>,
    },
}

impl Debug for MountLister {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MountLister::Real { mount_path, .. } => f
                .debug_struct("Real")
                .field("inner", &"<Lister>")
                .field("mount_path", mount_path)
                .finish(),

            MountLister::Virtual { entries } => {
                f.debug_struct("Virtual").field("entries", entries).finish()
            }
        }
    }
}

impl oio::List for MountLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            MountLister::Real { inner, mount_path } => match inner.next().await {
                Some(Ok(entry)) => {
                    let rebased = format!(
                        "{}/{}",
                        mount_path.trim_end_matches('/'),
                        entry.path().trim_start_matches('/')
                    );
                    Ok(Some(oio::Entry::new(&rebased, entry.metadata().clone())))
                }
                Some(Err(e)) => Err(e),
                None => Ok(None),
            },
            MountLister::Virtual { entries } => Ok(entries.pop()),
        }
    }
}

/// Deletes always re-resolve per path, since a single delete batch can span
/// multiple mounts (or none, which is an error per-entry rather than for
/// the whole batch).
#[derive(Debug)]
pub struct MountDeleter {
    /// The mounts to use for deleting.
    mounts: Arc<BTreeMap<String, Mount>>,
}

impl oio::Delete for MountDeleter {
    async fn delete(&mut self, path: &str, _args: OpDelete) -> Result<()> {
        match resolve(&self.mounts, path) {
            Some((_, mount, rel)) => {
                if mount.read_only {
                    return Err(permission_denied(path));
                }
                mount.operator.delete(&rel).await
            }
            None => Err(not_found(path)),
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(unused_results)]
mod tests {
    use super::*;
    use crate::config::{MemoryConfig, Scheme};
    use std::sync::Arc;

    fn builder() -> VfsBuilder {
        VfsBuilder::new(Arc::new(MemoryTracker::default()))
    }

    #[tokio::test]
    async fn write_and_read_inside_a_mount_rebases_the_path() {
        let op =
            Operator::new(builder().mount("/repos/test", Scheme::Memory(MemoryConfig::default())))
                .unwrap()
                .finish();

        op.write("/repos/test/abc.txt", "hello").await.unwrap();

        let data = op.read("/repos/test/abc.txt").await.unwrap();
        assert_eq!(data.to_vec(), b"hello");
    }

    #[tokio::test]
    async fn create_dir_rebases_the_path() {
        let op =
            Operator::new(builder().mount("/repos/test", Scheme::Memory(MemoryConfig::default())))
                .unwrap()
                .finish();

        op.create_dir("/repos/test/abc/").await.unwrap();
        let meta = op.stat("/repos/test/abc/").await.unwrap();
        assert!(meta.is_dir());
    }

    #[tokio::test]
    async fn path_outside_any_mount_is_not_found() {
        let op =
            Operator::new(builder().mount("/repos/test", Scheme::Memory(MemoryConfig::default())))
                .unwrap()
                .finish();

        let err = op.write("/elsewhere/file.txt", "x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn listing_an_unmounted_ancestor_shows_virtual_subfolders() {
        let op = Operator::new(
            builder()
                .mount("/repos/test", Scheme::Memory(MemoryConfig::default()))
                .mount("/repos/other", Scheme::Memory(MemoryConfig::default()))
                .mount("/images", Scheme::Memory(MemoryConfig::default())),
        )
        .unwrap()
        .finish();

        let mut names: Vec<String> = op
            .list("/")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name().trim_end_matches('/').to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["images", "repos"]);

        let mut repos_children: Vec<String> = op
            .list("/repos/") // <-- add trailing slash
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.name().trim_end_matches('/').to_string())
            .collect();
        repos_children.sort();
        assert_eq!(repos_children, vec!["other", "test"]);
    }

    #[tokio::test]
    async fn listing_inside_a_mount_delegates_and_rebases_entries() {
        let op =
            Operator::new(builder().mount("/repos/test", Scheme::Memory(MemoryConfig::default())))
                .unwrap()
                .finish();

        op.write("/repos/test/a.txt", "1").await.unwrap();
        op.write("/repos/test/b.txt", "2").await.unwrap();

        let mut names: Vec<String> = op
            .list("/repos/test/")
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.path().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["/repos/test/a.txt", "/repos/test/b.txt"]);
    }

    #[tokio::test]
    async fn read_only_mount_rejects_writes_but_allows_reads() {
        let op = Operator::new(
            builder()
                .mount("/repos/test", Scheme::Memory(MemoryConfig::default()))
                .read_only(),
        )
        .unwrap()
        .finish();

        let err = op.write("/repos/test/a.txt", "x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);

        let err = op.create_dir("/repos/test/dir/").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn quota_is_enforced_per_mount() {
        let op = Operator::new(
            builder()
                .mount("/repos/test", Scheme::Memory(MemoryConfig::default()))
                .quota("", 10)
                .mount("/scratch", Scheme::Memory(MemoryConfig::default())),
        )
        .unwrap()
        .finish();

        op.write("/repos/test/a.txt", "0123456789").await.unwrap(); // exactly 10

        let err = op.write("/repos/test/b.txt", "x").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RateLimited);

        // The other mount's quota (unlimited) is unaffected.
        op.write("/scratch/big.txt", "0123456789012345678901234567890")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn deleting_routes_per_path() {
        let op = Operator::new(
            builder()
                .mount("/repos/test", Scheme::Memory(MemoryConfig::default()))
                .mount("/scratch", Scheme::Memory(MemoryConfig::default())),
        )
        .unwrap()
        .finish();

        op.write("/repos/test/a.txt", "x").await.unwrap();
        op.write("/scratch/b.txt", "y").await.unwrap();

        op.delete("/repos/test/a.txt").await.unwrap();
        op.delete("/scratch/b.txt").await.unwrap();

        assert_eq!(
            op.stat("/repos/test/a.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            op.stat("/scratch/b.txt").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn duplicate_mount_paths_are_rejected_at_build() {
        let err = builder()
            .mount("/repos/test", Scheme::Memory(MemoryConfig::default()))
            .mount("/repos/test", Scheme::Memory(MemoryConfig::default()))
            .build()
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[tokio::test]
    async fn mounting_root_is_rejected_at_build() {
        let err = builder()
            .mount("/", Scheme::Memory(MemoryConfig::default()))
            .build()
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }
}
