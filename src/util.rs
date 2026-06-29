use opendal::raw::*;
use opendal::{Error, ErrorKind};
use std::fmt;
use std::fmt::Debug;

/// An OpenDAL layer that rejects all writes and deletes with
/// [`ErrorKind::PermissionDenied`].
///
/// Applied to every repo mount (the rustic VFS is inherently R/O, but this
/// makes the constraint visible at the opendal layer level) and to any data
/// point with [`VfsPoint::readonly`] set to `true`.
pub struct ReadOnlyLayer;

impl<A: Access> Layer<A> for ReadOnlyLayer {
    type LayeredAccess = ReadOnlyAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        ReadOnlyAccessor { inner }
    }
}

pub struct ReadOnlyAccessor<A: Access> {
    inner: A,
}

impl<A: Access> Debug for ReadOnlyAccessor<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadonlyAccessor").finish_non_exhaustive()
    }
}

impl<A: Access> LayeredAccess for ReadOnlyAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = oio::Writer;
    type Lister = A::Lister;
    type Deleter = oio::Deleter;
    type Copier = A::Copier;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, _path: &str, _args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        Err(
            Error::new(ErrorKind::PermissionDenied, "read-only mount point")
                .with_context("layer", "ReadonlyLayer"),
        )
    }

    async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
        Err(
            Error::new(ErrorKind::PermissionDenied, "read-only mount point")
                .with_context("layer", "ReadonlyLayer"),
        )
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }
}
