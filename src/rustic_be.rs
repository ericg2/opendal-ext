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

use log::warn;
use opendal::raw::oio::Entry;
use opendal::raw::*;
use opendal::{Buffer, Capability, EntryMode, Error, ErrorKind, Metadata};
use rustic_core::vfs::{IdenticalSnapshot, Latest, OpenFile, Vfs};
use rustic_core::{IndexedFullStatus, Node, Repository};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::vec;
use rustic_backend::BackendBuilder;
use tokio::sync::RwLock;
use tokio::time;
use crate::rustic_config::RusticVfsConfig;
// ── type aliases ─────────────────────────────────────────────────────────────

/// A fully-opened, index-loaded rustic repository.
///
/// The [`IndexedFullStatus`] type parameter signals that rustic has read the
/// repository index into memory, making pack lookups and node resolution
/// available without further I/O.
type IndexedRepo = Repository<IndexedFullStatus>;

// ── constants ─────────────────────────────────────────────────────────────────

/// Fallback refresh cadence used when [`RusticVfsConfig::refresh_interval`] is
/// `None`. Set to 5 minutes as a balance between freshness and repository I/O.
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// Default snapshot path template passed to [`Vfs::from_snapshots`].
///
/// Tokens like `{hostname}` and `{label}` are expanded by rustic at VFS
/// build time; `{time}` is formatted with [`DEFAULT_TIME`].
const DEFAULT_PATH: &str = "[{hostname}]/[{label}]/{time}";

/// [`strftime`](https://docs.rs/chrono/latest/chrono/format/strftime)-style
/// format string used to render the `{time}` token in [`DEFAULT_PATH`].
const DEFAULT_TIME: &str = "%Y-%m-%d_%H-%M-%S";

/// Number of bytes read from the rustic blob store in a single
/// [`oio::Read::read`] call. 4 MiB keeps individual allocations modest while
/// amortizing per-call overhead over a reasonable chunk of data.
const BUFFER_SIZE: usize = 4_000_000;

// ── VfsBackend ────────────────────────────────────────────────────────────────

/// OpenDAL [`Access`] implementation backed by a rustic repository.
///
/// `VfsBackend` presents the snapshots in a rustic repository as a read-only
/// virtual filesystem through OpenDAL's [`Access`] trait. The in-memory VFS
/// is kept up-to-date by a background refresh task that periodically re-reads
/// the repository index and atomically swaps in a new [`Vfs`] instance.
///
/// # Capabilities
///
/// | Capability        | Supported |
/// |-------------------|-----------|
/// | `stat`            | ✓         |
/// | `read`            | ✓         |
/// | `list`            | ✓         |
/// | `copy`            | ✓         |
/// | `list_recursive`  | –         |
/// | writes / deletes  | –         |
///
/// # Construction
///
/// Prefer [`VfsBackend::from_config`] when building from a
/// [`RusticVfsConfig`], or [`VfsBackend::from_repo`] when you already hold an
/// open [`IndexedRepo`].
#[derive(Debug)]
pub struct VfsBackend {
    /// Shared, refresh-able VFS.
    ///
    /// Writers hold the lock only long enough to swap in a freshly-built
    /// instance; readers (`stat` / `read` / `list`) hold a short-lived read
    /// guard and release it before returning.
    vfs: Arc<RwLock<Vfs>>,

    /// The open rustic repository used for blob and node lookups.
    ///
    /// Wrapped in `Arc` so it can be shared with the background refresh task
    /// without cloning the repository itself.
    repo: Arc<IndexedRepo>,

    /// Cached OpenDAL accessor metadata (scheme, name, capabilities).
    ///
    /// Built once at construction and cheaply cloned on every [`Access::info`]
    /// call.
    info: Arc<AccessorInfo>,
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a fresh [`Vfs`] from all snapshots currently in `repo`.
///
/// Snapshots are arranged into a directory tree using [`DEFAULT_PATH`] and
/// [`DEFAULT_TIME`]. Duplicate snapshots with identical content are presented
/// as directories ([`IdenticalSnapshot::AsDir`]) and the `latest` symlink
/// entry is also a directory ([`Latest::AsDir`]).
///
/// # Errors
///
/// Propagates any rustic error (e.g. index read failure, corrupt pack) as an
/// [`opendal_core::ErrorKind::Unexpected`] temporary error so that the caller
/// can retry without discarding the existing VFS.
fn build_vfs(repo: &IndexedRepo) -> opendal::Result<Vfs> {
    repo.get_all_snapshots()
        .and_then(|snapshots| {
            Vfs::from_snapshots(
                snapshots,
                DEFAULT_PATH,
                DEFAULT_TIME,
                Latest::AsDir,
                IdenticalSnapshot::AsDir,
            )
        })
        .map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to build VFS.")
                .set_source(e)
                .set_temporary()
        })
}

// ── background refresh task ───────────────────────────────────────────────────

/// Spawns a Tokio task that atomically rebuilds the shared [`Vfs`] on
/// `interval`.
///
/// The task holds only a [`Weak`] reference to the shared `RwLock<Vfs>`; when
/// the owning [`VfsBackend`] is dropped, the `Weak` upgrade fails and the task
/// exits cleanly on the next tick — no explicit cancellation is needed.
///
/// On a successful rebuild the write lock is held only for the duration of the
/// pointer swap, keeping read-side latency impact minimal. On failure the
/// existing VFS is left intact and a warning is logged so that reads continue
/// serving stale-but-valid data.
///
/// # Arguments
///
/// * `vfs`      – Shared reference to the VFS being managed.
/// * `repo`     – Open rustic repository used to rebuild the VFS.
/// * `interval` – How often to attempt a rebuild.
fn spawn_refresh_task(vfs: &Arc<RwLock<Vfs>>, repo: Arc<IndexedRepo>, interval: Duration) {
    let weak_vfs: Weak<RwLock<Vfs>> = Arc::downgrade(vfs);
    let mut ticker = time::interval(interval);
    tokio::spawn(async move {
        loop {
            ticker.tick().await;
            let Some(arc_vfs) = weak_vfs.upgrade() else {
                break;
            };
            let repo_clone = repo.clone();
            // Move blocking work off the executor thread
            match tokio::task::spawn_blocking(move || build_vfs(&repo_clone)).await {
                Ok(Ok(new_vfs)) => {
                    *arc_vfs.write().await = new_vfs;
                }
                Ok(Err(e)) => warn!("VFS refresh failed: {e}"),
                Err(e) => warn!("VFS refresh panicked: {e}"),
            }
        }
    });
}

// ── VfsBackend impl ───────────────────────────────────────────────────────────

impl VfsBackend {
    /// Construct a [`VfsBackend`] from an already-opened, indexed rustic
    /// repository.
    ///
    /// Builds the initial in-memory VFS synchronously, then spawns a
    /// background task to refresh it every `refresh_interval`. Use
    /// [`DEFAULT_REFRESH_INTERVAL`] if you have no specific cadence in mind.
    ///
    /// Prefer [`from_config`](VfsBackend::from_config) when you are starting
    /// from a [`RusticVfsConfig`] rather than a pre-opened repository.
    ///
    /// # Arguments
    ///
    /// * `repo`             – An open, indexed rustic repository.
    /// * `refresh_interval` – How often the background task rebuilds the VFS.
    ///
    /// # Errors
    ///
    /// Returns [`Unexpected`](opendal_core::ErrorKind::Unexpected) if the
    /// initial VFS build fails (e.g. the repository index is unreadable).
    pub fn from_repo(
        repo: Arc<IndexedRepo>,
        refresh_interval: Duration,
    ) -> opendal::Result<Self> {
        let vfs = Arc::new(RwLock::new(build_vfs(&repo)?));
        let info = AccessorInfo::default();
        info.set_scheme("rustic")
            .set_name("Rustic")
            .set_native_capability(Capability {
                stat: true,
                read: true,
                copy: true,
                list: true,
                shared: true,
                list_with_recursive: false,
                ..Default::default()
            });

        let backend = Self {
            vfs,
            repo,
            info: Arc::new(info),
        };

        spawn_refresh_task(&backend.vfs, backend.repo.clone(), refresh_interval);

        Ok(backend)
    }

    /// Construct a [`VfsBackend`] from a [`RusticVfsConfig`].
    ///
    /// Opens the rustic repository described by the config (authenticating with
    /// the supplied credentials), loads its index, and then delegates to
    /// [`from_repo`](VfsBackend::from_repo).
    ///
    /// This is the primary entry point used by [`RusticVfsBuilder::build`].
    ///
    /// # Errors
    ///
    /// | Condition | Error kind |
    /// |-----------|-----------|
    /// | `credentials` is `None` | [`ConfigInvalid`](opendal_core::ErrorKind::ConfigInvalid) |
    /// | `backend` options cannot be parsed | [`ConfigInvalid`](opendal_core::ErrorKind::ConfigInvalid) |
    /// | Repository cannot be opened / indexed | [`Unexpected`](opendal_core::ErrorKind::Unexpected) (temporary) |
    pub fn from_config(config: RusticVfsConfig) -> opendal::Result<Self> {
        let creds = config.credentials.ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "Credentials must be supplied via `RusticVfsConfig::credentials`.",
            )
        })?;

        let be = config.backend.to_backends().map_err(|e| {
            Error::new(ErrorKind::ConfigInvalid, "Failed to parse backend config.")
                .set_source(e)
                .with_context("repo", config.backend.repository.unwrap_or_default())
                .with_context("repo_hot", config.backend.repo_hot.unwrap_or_default())
                .set_permanent()
        })?;

        let repo = Repository::new(&config.options, &be)
            .and_then(|r| r.open(&creds))
            .and_then(|r| r.to_indexed())
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to open rustic repository. Check that options and credentials are correct.",
                )
                    .set_source(e)
                    .set_temporary()
            })?;

        Self::from_repo(
            Arc::new(repo),
            config.refresh_interval.unwrap_or(DEFAULT_REFRESH_INTERVAL),
        )
    }

    /// Resolve a VFS path string to a rustic [`Node`].
    ///
    /// Normalises the path (ensuring a leading `/` and stripping trailing
    /// slashes) before handing it to the VFS. Holds the VFS read lock only for
    /// the duration of the lookup.
    ///
    /// # Errors
    ///
    /// Returns [`NotFound`](opendal_core::ErrorKind::NotFound) if the path
    /// does not exist in the current VFS snapshot.
    async fn node_from_path(&self, path: &str) -> opendal::Result<Node> {
        let path = normalize_path(path);
        self.vfs
            .read()
            .await
            .node_from_path(&self.repo, Path::new(&path))
            .map_err(|e| Error::new(ErrorKind::NotFound, "Path not found in VFS.").set_source(e))
    }
}

// ── Access impl ───────────────────────────────────────────────────────────────

impl Access for VfsBackend {
    type Reader = VfsReader;
    type Writer = ();
    type Lister = VfsLister;
    type Deleter = ();
    type Copier = ();

    /// Returns the cached [`AccessorInfo`] describing this backend's scheme,
    /// name, and capabilities.
    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    /// Stat a path, returning its [`Metadata`] (type and last-modified time).
    ///
    /// # Errors
    ///
    /// Returns [`NotFound`](opendal_core::ErrorKind::NotFound) if the path
    /// does not exist in the current VFS snapshot.
    async fn stat(&self, path: &str, _args: OpStat) -> opendal::Result<RpStat> {
        let node = self.node_from_path(path).await?;
        Ok(RpStat::new(meta_from_node(&node)))
    }

    /// Open a file for reading, returning its metadata and a [`VfsReader`].
    ///
    /// The reader honours the byte range specified in `args`; if no range is
    /// given the full file is readable. Reads are served in
    /// [`BUFFER_SIZE`]-byte chunks from the rustic blob store.
    ///
    /// # Errors
    ///
    /// Returns [`NotFound`](opendal_core::ErrorKind::NotFound) if the path
    /// does not exist, or [`Unexpected`](opendal_core::ErrorKind::Unexpected)
    /// if the file cannot be opened in the rustic layer.
    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        let node = self.node_from_path(path).await?;
        let meta = meta_from_node(&node);
        let file = self.repo.open_file(&node).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                "Failed to open file in rustic backend.",
            )
                .set_source(e)
                .set_temporary()
        })?;
        let reader = VfsReader::new(
            file,
            self.repo.clone(),
            args.range().offset() as usize,
            args.range().size().map(|s| s as usize),
        );
        Ok((RpRead::new(meta), reader))
    }

    /// List the direct children of a directory path.
    ///
    /// Returns a [`VfsLister`] that yields one [`Entry`] per child node. Only
    /// the immediate children are returned; recursive listing is not supported
    /// (see [`Capability`] flags).
    ///
    /// # Errors
    ///
    /// Returns [`NotFound`](ErrorKind::NotFound) if the path
    /// does not exist or is not a directory in the current VFS snapshot.
    async fn list(
        &self,
        path: &str,
        _args: OpList,
    ) -> opendal::Result<(RpList, Self::Lister)> {
        let normalized = normalize_path(path);
        let path_buf = Path::new(&normalized).to_path_buf();
        let entries = self
            .vfs
            .read()
            .await
            .dir_entries_from_path(&self.repo, &path_buf)
            .map_err(|e| {
                Error::new(ErrorKind::NotFound, "Directory not found in VFS.").set_source(e)
            })?;

        Ok((RpList::default(), VfsLister::new(path_buf, entries)))
    }
}

// ── metadata / path helpers ───────────────────────────────────────────────────

/// Convert a rustic [`Node`] to OpenDAL [`Metadata`].
///
/// Maps the node's directory/file status, last-modified time, and content
/// length into the fields OpenDAL consumers expect.
fn meta_from_node(n: &Node) -> Metadata {
    Metadata::default()
        .with_mode(if n.is_dir() {
            EntryMode::DIR
        } else {
            EntryMode::FILE
        })
        .with_last_modified(n.meta.mtime.unwrap_or_default().into())
        .with_content_length(n.meta.size)
}

/// Normalise a path string for VFS lookup.
///
/// Ensures the path starts with `/` and strips any trailing `/` unless the
/// path is the root (`/`) itself. OpenDAL may hand us paths in either form;
/// rustic's VFS expects the leading slash.
fn normalize_path(path: &str) -> String {
    let p = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if p.len() > 1 {
        p.trim_end_matches('/').to_string()
    } else {
        p
    }
}

// ── VfsReader ─────────────────────────────────────────────────────────────────

/// [`oio::Read`] implementation that streams blob data out of a rustic
/// repository file.
///
/// Reads are issued in [`BUFFER_SIZE`]-byte chunks starting at `pos`. If a
/// byte-range was requested via [`OpRead`], `len` tracks how many bytes remain
/// in the window; once exhausted, `read` returns an empty [`Buffer`].
pub struct VfsReader {
    /// The open rustic file handle (contains chunk metadata, not raw bytes).
    file: Arc<OpenFile>,
    /// Repository used to fetch blob data for each chunk.
    repo: Arc<IndexedRepo>,
    /// Current read position as a byte offset into the file.
    pos: usize,
    /// Remaining bytes to serve, or `None` if reading to end-of-file.
    len: Option<usize>,
}

impl VfsReader {
    /// Create a new [`VfsReader`].
    ///
    /// # Arguments
    ///
    /// * `file` – Open rustic file handle.
    /// * `repo` – Repository used to resolve blob data.
    /// * `pos`  – Starting byte offset (from [`OpRead`] range).
    /// * `len`  – Maximum bytes to serve, or `None` for the full remainder.
    pub(crate) fn new(
        file: OpenFile,
        repo: Arc<IndexedRepo>,
        pos: usize,
        len: Option<usize>,
    ) -> Self {
        Self {
            file: Arc::new(file),
            repo,
            pos,
            len,
        }
    }
}

impl oio::Read for VfsReader {
    /// Read the next chunk of data from the rustic file.
    ///
    /// Issues a single [`Repository::read_file_at`] call of up to
    /// [`BUFFER_SIZE`] bytes (or the remaining window, whichever is smaller)
    /// and advances the internal position. Returns an empty [`Buffer`] when
    /// the requested range has been fully consumed.
    ///
    /// # Errors
    ///
    /// Returns [`Unexpected`](opendal_core::ErrorKind::Unexpected) (temporary)
    /// if the underlying rustic blob read fails.
    async fn read(&mut self) -> opendal::Result<Buffer> {
        let read_size = match self.len {
            Some(0) => return Ok(Buffer::new()),
            Some(remaining) => BUFFER_SIZE.min(remaining),
            None => BUFFER_SIZE,
        };

        let file = self.file.clone();
        let repo = self.repo.clone();
        let pos = self.pos;
        let data = tokio::task::spawn_blocking(move || repo.read_file_at(&file, pos, read_size))
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, "join error").set_source(e))?
            .map_err(|e| Error::new(ErrorKind::Unexpected, "read failed").set_source(e))?;

        self.pos += data.len();
        if let Some(remaining) = self.len.as_mut() {
            *remaining -= data.len().min(*remaining);
        }

        Ok(data.into())
    }
}

// ── VfsLister ─────────────────────────────────────────────────────────────────

/// [`oio::List`] implementation that iterates over the direct children of a
/// rustic VFS directory.
///
/// Each call to [`next`](oio::List::next) pops one [`Node`] from the
/// pre-fetched child list, computes its OpenDAL path relative to `root`, and
/// returns an [`Entry`] with the node's metadata.
pub struct VfsLister {
    /// The directory path being listed (used to build child entry paths).
    root: PathBuf,
    /// Pre-fetched child nodes, consumed one per [`next`](oio::List::next) call.
    nodes: vec::IntoIter<Node>,
}

impl VfsLister {
    /// Create a new [`VfsLister`].
    ///
    /// # Arguments
    ///
    /// * `root`  – Normalised path of the directory being listed.
    /// * `nodes` – Direct child nodes returned by the rustic VFS.
    pub(crate) fn new(root: PathBuf, nodes: Vec<Node>) -> Self {
        Self {
            root,
            nodes: nodes.into_iter(),
        }
    }
}

impl oio::List for VfsLister {
    /// Return the next directory entry, or `None` when the listing is
    /// exhausted.
    ///
    /// Entry paths are constructed as `{root}/{node.name}`, with a trailing
    /// `/` appended for directories (as required by OpenDAL's path convention).
    async fn next(&mut self) -> opendal::Result<Option<Entry>> {
        let entry = self.nodes.next().map(|n| {
            let base = self.root.to_string_lossy().replace('\\', "/");
            let base = base.trim_matches('/');

            let mut path = if base.is_empty() {
                n.name.clone()
            } else {
                format!("{base}/{}", n.name)
            };

            if n.is_dir() {
                if !path.ends_with('/') {
                    path.push('/');
                }
            } else {
                path = path.trim_end_matches('/').to_string();
            }

            Entry::new(&path, meta_from_node(&n))
        });

        Ok(entry)
    }
}
