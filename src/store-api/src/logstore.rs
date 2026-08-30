// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! LogStore APIs.

pub mod entry;
pub mod provider;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use common_error::ext::{BoxedError, ErrorExt};
use common_wal::options::WalOptions;
use entry::Entry;
use futures::{Stream, TryStreamExt};

pub type SendableEntryStream<'a, I, E> = Pin<Box<dyn Stream<Item = Result<Vec<I>, E>> + Send + 'a>>;

pub use crate::logstore::entry::Id as EntryId;
use crate::logstore::provider::{ExternalProvider, Provider};
use crate::storage::RegionId;

// The information used to locate WAL index for the specified region.
#[derive(Debug, Clone, Copy)]
pub struct WalIndex {
    pub region_id: RegionId,
    pub location_id: u64,
}

impl WalIndex {
    pub fn new(region_id: RegionId, location_id: u64) -> Self {
        Self {
            region_id,
            location_id,
        }
    }
}

/// `LogStore` serves as a Write-Ahead-Log for storage engine.
#[async_trait::async_trait]
pub trait LogStore: Send + Sync + 'static + std::fmt::Debug {
    type Error: ErrorExt + Send + Sync + 'static;

    /// Stops components of the logstore.
    async fn stop(&self) -> Result<(), Self::Error>;

    /// Resolves the provider used by Mito for a region owned by an external log
    /// store.
    ///
    /// Mito uses the returned provider when creating or reopening a region,
    /// replaying its WAL, and performing offline cleanup. The provider identity
    /// and its local or remote WAL semantics must remain stable while the region's
    /// WAL data exists; otherwise replay and obsolete operations may target
    /// different namespaces or use inconsistent recovery semantics.
    ///
    /// Resolution order:
    /// - Mito does not call this method for [`WalOptions::Noop`].
    /// - `Ok(Some(_))` overrides the built-in Raft Engine or Kafka provider.
    /// - `Ok(None)` delegates to the built-in provider resolution.
    /// - `Err(_)` aborts the operation. In particular, `create_or_open` does not
    ///   fall back to creating a new region after provider resolution fails.
    ///
    /// This synchronous method is called from async worker paths. It must be
    /// inexpensive and must not perform blocking I/O.
    fn resolve_provider(
        &self,
        _region_id: RegionId,
        _wal_options: &WalOptions,
    ) -> Result<Option<ExternalProvider>, Self::Error> {
        Ok(None)
    }

    /// Appends a batch of entries and returns a response containing a map where the key is a region id
    /// while the value is the id of the last successfully written entry of the region.
    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse, Self::Error>;

    /// Creates a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`.
    async fn read(
        &self,
        provider: &Provider,
        id: EntryId,
        index: Option<WalIndex>,
    ) -> Result<SendableEntryStream<'static, Entry, Self::Error>, Self::Error>;

    /// Creates a new `Namespace` from the given ref.
    async fn create_namespace(&self, ns: &Provider) -> Result<(), Self::Error>;

    /// Deletes an existing `Namespace` specified by the given ref.
    async fn delete_namespace(&self, ns: &Provider) -> Result<(), Self::Error>;

    /// Lists all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Provider>, Self::Error>;

    /// Marks all entries with ids `<=entry_id` of the given `namespace` as obsolete,
    /// so that the log store can safely delete those entries. This method does not guarantee
    /// that the obsolete entries are deleted immediately.
    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<(), Self::Error>;

    /// Marks all entries of a region as obsolete and removes its dedicated namespace when
    /// supported by the backend.
    async fn obsolete_all(
        &self,
        provider: &Provider,
        region_id: RegionId,
    ) -> Result<(), Self::Error>;

    /// Makes an entry instance of the associated Entry type
    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry, Self::Error>;

    /// Returns the latest entry id in the log store.
    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId, Self::Error>;
}

/// A sized, type-erased reference to a [`LogStore`].
#[derive(Clone, Debug)]
pub struct BoxedLogStore {
    inner: Arc<dyn LogStore<Error = BoxedError>>,
}

impl BoxedLogStore {
    /// Wraps a shared log store reference and erases its implementation and error types.
    pub fn new<S: LogStore>(store: Arc<S>) -> Self {
        Self {
            inner: Arc::new(LogStoreRefAdapter(store)),
        }
    }
}

#[derive(Debug)]
struct LogStoreRefAdapter<S>(Arc<S>);

#[async_trait::async_trait]
impl<S: LogStore> LogStore for LogStoreRefAdapter<S> {
    type Error = BoxedError;

    async fn stop(&self) -> Result<(), Self::Error> {
        self.0.stop().await.map_err(BoxedError::new)
    }

    fn resolve_provider(
        &self,
        region_id: RegionId,
        wal_options: &WalOptions,
    ) -> Result<Option<ExternalProvider>, Self::Error> {
        self.0
            .resolve_provider(region_id, wal_options)
            .map_err(BoxedError::new)
    }

    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse, Self::Error> {
        self.0.append_batch(entries).await.map_err(BoxedError::new)
    }

    async fn read(
        &self,
        provider: &Provider,
        id: EntryId,
        index: Option<WalIndex>,
    ) -> Result<SendableEntryStream<'static, Entry, Self::Error>, Self::Error> {
        let stream = self
            .0
            .read(provider, id, index)
            .await
            .map_err(BoxedError::new)?;
        Ok(Box::pin(stream.map_err(BoxedError::new)))
    }

    async fn create_namespace(&self, ns: &Provider) -> Result<(), Self::Error> {
        self.0.create_namespace(ns).await.map_err(BoxedError::new)
    }

    async fn delete_namespace(&self, ns: &Provider) -> Result<(), Self::Error> {
        self.0.delete_namespace(ns).await.map_err(BoxedError::new)
    }

    async fn list_namespaces(&self) -> Result<Vec<Provider>, Self::Error> {
        self.0.list_namespaces().await.map_err(BoxedError::new)
    }

    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<(), Self::Error> {
        self.0
            .obsolete(provider, region_id, entry_id)
            .await
            .map_err(BoxedError::new)
    }

    async fn obsolete_all(
        &self,
        provider: &Provider,
        region_id: RegionId,
    ) -> Result<(), Self::Error> {
        self.0
            .obsolete_all(provider, region_id)
            .await
            .map_err(BoxedError::new)
    }

    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry, Self::Error> {
        self.0
            .entry(data, entry_id, region_id, provider)
            .map_err(BoxedError::new)
    }

    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId, Self::Error> {
        self.0.latest_entry_id(provider).map_err(BoxedError::new)
    }
}

#[async_trait::async_trait]
impl LogStore for BoxedLogStore {
    type Error = BoxedError;

    async fn stop(&self) -> Result<(), Self::Error> {
        self.inner.stop().await
    }

    fn resolve_provider(
        &self,
        region_id: RegionId,
        wal_options: &WalOptions,
    ) -> Result<Option<ExternalProvider>, Self::Error> {
        self.inner.resolve_provider(region_id, wal_options)
    }

    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse, Self::Error> {
        self.inner.append_batch(entries).await
    }

    async fn read(
        &self,
        provider: &Provider,
        id: EntryId,
        index: Option<WalIndex>,
    ) -> Result<SendableEntryStream<'static, Entry, Self::Error>, Self::Error> {
        self.inner.read(provider, id, index).await
    }

    async fn create_namespace(&self, ns: &Provider) -> Result<(), Self::Error> {
        self.inner.create_namespace(ns).await
    }

    async fn delete_namespace(&self, ns: &Provider) -> Result<(), Self::Error> {
        self.inner.delete_namespace(ns).await
    }

    async fn list_namespaces(&self) -> Result<Vec<Provider>, Self::Error> {
        self.inner.list_namespaces().await
    }

    async fn obsolete(
        &self,
        provider: &Provider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<(), Self::Error> {
        self.inner.obsolete(provider, region_id, entry_id).await
    }

    async fn obsolete_all(
        &self,
        provider: &Provider,
        region_id: RegionId,
    ) -> Result<(), Self::Error> {
        self.inner.obsolete_all(provider, region_id).await
    }

    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry, Self::Error> {
        self.inner.entry(data, entry_id, region_id, provider)
    }

    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId, Self::Error> {
        self.inner.latest_entry_id(provider)
    }
}

/// The response of an `append` operation.
#[derive(Debug, Default)]
pub struct AppendResponse {
    /// The id of the entry appended to the log store.
    pub last_entry_id: EntryId,
}

/// The response of an `append_batch` operation.
#[derive(Debug, Default)]
pub struct AppendBatchResponse {
    /// Key: region id (as u64). Value: the id of the last successfully written entry of the region.
    pub last_entry_ids: HashMap<RegionId, EntryId>,
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use common_error::ext::{RetryHint, StackError};
    use common_error::status_code::StatusCode;
    use futures::{StreamExt, stream};

    use super::*;
    use crate::logstore::entry::NaiveEntry;

    #[derive(Debug)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    impl ErrorExt for TestError {
        fn status_code(&self) -> StatusCode {
            StatusCode::StorageUnavailable
        }

        fn retry_hint(&self) -> RetryHint {
            RetryHint::Retryable
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl StackError for TestError {
        fn debug_fmt(&self, _layer: usize, _buf: &mut Vec<String>) {}

        fn next(&self) -> Option<&dyn StackError> {
            None
        }
    }

    #[derive(Debug, Default)]
    struct TestLogStore {
        fail_append: AtomicBool,
        fail_mutation: AtomicBool,
        fail_read: AtomicBool,
        fail_read_item: AtomicBool,
        mutation_calls: AtomicUsize,
        stop_calls: AtomicUsize,
    }

    impl TestLogStore {
        fn mutation(&self, call: usize) -> Result<(), TestError> {
            self.mutation_calls.fetch_or(call, Ordering::Relaxed);
            (!self.fail_mutation.load(Ordering::Relaxed))
                .then_some(())
                .ok_or(TestError)
        }
    }

    #[async_trait::async_trait]
    impl LogStore for TestLogStore {
        type Error = TestError;

        async fn stop(&self) -> Result<(), Self::Error> {
            self.stop_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn resolve_provider(
            &self,
            region_id: RegionId,
            _wal_options: &WalOptions,
        ) -> Result<Option<ExternalProvider>, Self::Error> {
            Ok(Some(ExternalProvider::local("test", region_id.to_string())))
        }

        async fn append_batch(
            &self,
            entries: Vec<Entry>,
        ) -> Result<AppendBatchResponse, Self::Error> {
            if self.fail_append.load(Ordering::Relaxed) {
                return Err(TestError);
            }
            Ok(AppendBatchResponse {
                last_entry_ids: entries
                    .iter()
                    .map(|entry| (entry.region_id(), entry.entry_id()))
                    .collect(),
            })
        }

        async fn read(
            &self,
            provider: &Provider,
            id: EntryId,
            _index: Option<WalIndex>,
        ) -> Result<SendableEntryStream<'static, Entry, Self::Error>, Self::Error> {
            if self.fail_read.load(Ordering::Relaxed) {
                return Err(TestError);
            }
            if self.fail_read_item.load(Ordering::Relaxed) {
                return Ok(Box::pin(stream::iter([Err(TestError)])));
            }
            let entry = self.entry(vec![1, 2, 3], id, RegionId::new(1, 2), provider)?;
            Ok(Box::pin(stream::iter([Ok(vec![entry])])))
        }

        async fn create_namespace(&self, ns: &Provider) -> Result<(), Self::Error> {
            assert_eq!(&Provider::noop_provider(), ns);
            self.mutation(1)
        }

        async fn delete_namespace(&self, ns: &Provider) -> Result<(), Self::Error> {
            assert_eq!(&Provider::noop_provider(), ns);
            self.mutation(2)
        }

        async fn list_namespaces(&self) -> Result<Vec<Provider>, Self::Error> {
            Ok(vec![Provider::noop_provider()])
        }

        async fn obsolete(
            &self,
            provider: &Provider,
            region_id: RegionId,
            entry_id: EntryId,
        ) -> Result<(), Self::Error> {
            assert_eq!(
                (&Provider::noop_provider(), RegionId::new(1, 2), 7),
                (provider, region_id, entry_id)
            );
            self.mutation(4)
        }

        async fn obsolete_all(
            &self,
            provider: &Provider,
            region_id: RegionId,
        ) -> Result<(), Self::Error> {
            assert_eq!(
                (&Provider::noop_provider(), RegionId::new(1, 2)),
                (provider, region_id)
            );
            self.mutation(8)
        }

        fn entry(
            &self,
            data: Vec<u8>,
            entry_id: EntryId,
            region_id: RegionId,
            provider: &Provider,
        ) -> Result<Entry, Self::Error> {
            Ok(Entry::Naive(NaiveEntry {
                provider: provider.clone(),
                region_id,
                entry_id,
                data,
            }))
        }

        fn latest_entry_id(&self, _provider: &Provider) -> Result<EntryId, Self::Error> {
            Ok(42)
        }
    }

    #[tokio::test]
    async fn test_boxed_log_store_forwards_calls() {
        let inner = Arc::new(TestLogStore::default());
        let store = BoxedLogStore::new(inner.clone());
        let region_id = RegionId::new(1, 2);
        let provider = store
            .resolve_provider(region_id, &WalOptions::RaftEngine)
            .unwrap()
            .unwrap();
        assert_eq!(region_id.to_string(), provider.namespace());

        let provider = Provider::external(provider);
        let entry = store.entry(vec![1, 2, 3], 7, region_id, &provider).unwrap();
        let response = store.append_batch(vec![entry.clone()]).await.unwrap();
        assert_eq!(Some(&7), response.last_entry_ids.get(&region_id));

        let batch = store
            .read(&provider, 7, None)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(vec![entry], batch);
        assert_eq!(42, store.latest_entry_id(&provider).unwrap());
        assert_eq!(
            vec![Provider::noop_provider()],
            store.list_namespaces().await.unwrap()
        );
        let noop = Provider::noop_provider();
        store.create_namespace(&noop).await.unwrap();
        store.delete_namespace(&noop).await.unwrap();
        store.obsolete(&noop, region_id, 7).await.unwrap();
        store.obsolete_all(&noop, region_id).await.unwrap();
        assert_eq!(15, inner.mutation_calls.load(Ordering::Relaxed));

        store.stop().await.unwrap();
        assert_eq!(1, inner.stop_calls.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_boxed_log_store_preserves_error_metadata() {
        fn assert_metadata(error: BoxedError) {
            assert_eq!(StatusCode::StorageUnavailable, error.status_code());
            assert_eq!(RetryHint::Retryable, error.retry_hint());
        }

        let inner = Arc::new(TestLogStore::default());
        inner.fail_append.store(true, Ordering::Relaxed);
        let error = BoxedLogStore::new(inner)
            .append_batch(vec![])
            .await
            .unwrap_err();
        assert_metadata(error);

        let inner = Arc::new(TestLogStore::default());
        inner.fail_mutation.store(true, Ordering::Relaxed);
        let store = BoxedLogStore::new(inner);
        let noop = Provider::noop_provider();
        assert_metadata(store.create_namespace(&noop).await.unwrap_err());
        assert_metadata(store.delete_namespace(&noop).await.unwrap_err());
        assert_metadata(
            store
                .obsolete(&noop, RegionId::new(1, 2), 7)
                .await
                .unwrap_err(),
        );
        assert_metadata(
            store
                .obsolete_all(&noop, RegionId::new(1, 2))
                .await
                .unwrap_err(),
        );

        let inner = Arc::new(TestLogStore::default());
        inner.fail_read.store(true, Ordering::Relaxed);
        let Err(error) = BoxedLogStore::new(inner).read(&noop, 0, None).await else {
            panic!("expected read to fail")
        };
        assert_metadata(error);

        let inner = Arc::new(TestLogStore::default());
        inner.fail_read_item.store(true, Ordering::Relaxed);
        let error = BoxedLogStore::new(inner)
            .read(&Provider::noop_provider(), 0, None)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert_metadata(error);
    }
}
