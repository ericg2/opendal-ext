//! A global write-quota [`Layer`] for OpenDAL.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;

use opendal::raw::*;
use opendal::{Buffer, Error, ErrorKind, Metadata, Result};

/// Persistence for how many bytes have been written under a given quota id.
#[async_trait]
pub trait QuotaTracker: Send + Sync + 'static {
    async fn get_bytes_written(&self, id: &str) -> Result<u64>;
    async fn set_bytes_written(&self, id: &str, bytes: u64) -> Result<()>;
}

/// Simple in-memory tracker for tests.
#[derive(Default)]
pub struct MemoryTracker(AsyncMutex<HashMap<String, u64>>);

#[async_trait]
impl QuotaTracker for MemoryTracker {
    async fn get_bytes_written(&self, id: &str) -> Result<u64> {
        Ok(*self.0.lock().await.get(id).unwrap_or(&0))
    }

    async fn set_bytes_written(&self, id: &str, bytes: u64) -> Result<()> {
        self.0.lock().await.insert(id.to_string(), bytes);
        Ok(())
    }
}

/// Shared quota state.
struct QuotaState {
    id: String,
    tracker: Arc<dyn QuotaTracker>,
    limit: u64,
    cache: AsyncMutex<Option<u64>>,
}

impl QuotaState {
    async fn reserve(&self, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }

        let mut cache = self.cache.lock().await;

        let current = match *cache {
            Some(v) => v,
            None => self.tracker.get_bytes_written(&self.id).await?,
        };

        let new_total = current.saturating_add(len);

        if new_total > self.limit {
            *cache = Some(current);

            return Err(Error::new(
                ErrorKind::RateLimited,
                format!(
                    "write quota exceeded for '{}': {} used, {} requested, {} limit",
                    self.id, current, len, self.limit
                ),
            )
            .with_context("quota_id", self.id.clone())
            .with_context("quota_limit", self.limit.to_string())
            .with_context("quota_used", current.to_string())
            .with_context("quota_requested", len.to_string()));
        }

        self.tracker.set_bytes_written(&self.id, new_total).await?;
        *cache = Some(new_total);

        Ok(())
    }

    async fn release(&self, len: u64) {
        if len == 0 {
            return;
        }

        let mut cache = self.cache.lock().await;
        let current = cache.unwrap_or(0);
        let new_total = current.saturating_sub(len);

        let _ = self.tracker.set_bytes_written(&self.id, new_total).await;

        *cache = Some(new_total);
    }
}

/// Global quota layer (dyn-based).
pub struct QuotaLayer {
    state: Arc<QuotaState>,
}

impl Clone for QuotaLayer {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl QuotaLayer {
    pub fn new(id: impl Into<String>, tracker: Arc<dyn QuotaTracker>, limit_bytes: u64) -> Self {
        Self {
            state: Arc::new(QuotaState {
                id: id.into(),
                tracker,
                limit: limit_bytes,
                cache: AsyncMutex::new(None),
            }),
        }
    }
}

impl<A: Access> Layer<A> for QuotaLayer {
    type LayeredAccess = QuotaAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        QuotaAccessor {
            inner,
            state: self.state.clone(),
        }
    }
}

pub struct QuotaAccessor<A: Access> {
    inner: A,
    state: Arc<QuotaState>,
}

impl<A: Access> fmt::Debug for QuotaAccessor<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuotaAccessor")
            .field("id", &self.state.id)
            .field("limit", &self.state.limit)
            .finish_non_exhaustive()
    }
}

impl<A: Access> LayeredAccess for QuotaAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = QuotaWriter<A::Writer>;
    type Lister = A::Lister;
    type Deleter = A::Deleter;
    type Copier = A::Copier;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let (rp, w) = self.inner.write(path, args).await?;
        Ok((rp, QuotaWriter::new(w, self.state.clone())))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }
}

pub struct QuotaWriter<W> {
    inner: W,
    state: Arc<QuotaState>,
    reserved: u64,
}

impl<W> QuotaWriter<W> {
    fn new(inner: W, state: Arc<QuotaState>) -> Self {
        Self {
            inner,
            state,
            reserved: 0,
        }
    }
}

impl<W: oio::Write> oio::Write for QuotaWriter<W> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let len = bs.len() as u64;

        self.state.reserve(len).await?;
        self.reserved += len;

        if let Err(e) = self.inner.write(bs).await {
            self.reserved -= len;
            self.state.release(len).await;
            return Err(e);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let meta = self.inner.close().await?;
        self.reserved = 0;
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await?;
        let to_release = self.reserved;
        self.reserved = 0;
        self.state.release(to_release).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use opendal::{Operator, services};
    use super::*;

    fn build_op(id: &str, tracker: Arc<MemoryTracker>, limit: u64) -> Operator {
        Operator::new(services::Memory::default())
            .unwrap()
            .layer(QuotaLayer::new(id, tracker, limit))
            .finish()
    }

    #[tokio::test]
    async fn writes_within_quota_succeed_and_are_tracked() {
        let tracker = Arc::new(MemoryTracker::default());
        let op = build_op("tenant-a", Arc::clone(&tracker), 1024);

        op.write("a.txt", "hello world").await.unwrap();

        assert_eq!(
            tracker.get_bytes_written("tenant-a").await.unwrap(),
            "hello world".len() as u64
        );
    }

    #[tokio::test]
    async fn write_exceeding_quota_is_rejected() {
        let tracker = Arc::new(MemoryTracker::default());
        let op = build_op("tenant-b", Arc::clone(&tracker), 10);

        let err = op
            .write("big.txt", "this is way too large")
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RateLimited);
        assert_eq!(tracker.get_bytes_written("tenant-b").await.unwrap(), 0);
    }
}
