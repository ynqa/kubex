use std::future::Future;

use futures::{
    future::BoxFuture,
    stream::{BoxStream, StreamExt},
};
use kube::{
    Api, Error as KubeError,
    api::{
        GetParams, ListParams, ObjectList, Patch, PatchParams, PostParams, WatchEvent, WatchParams,
    },
    core::PartialObjectMeta,
};
use serde::{Serialize, de::DeserializeOwned};

use super::{RetryPolicy, retry_with_policy};

type WatchRetryResult<K> = Result<WatchEvent<K>, KubeError>;
type WatchRetryStream<'a, K> = BoxStream<'a, WatchRetryResult<K>>;
type WatchMetadataRetryResult<K> = Result<WatchEvent<PartialObjectMeta<K>>, KubeError>;
type WatchMetadataRetryStream<'a, K> = BoxStream<'a, WatchMetadataRetryResult<K>>;

/// Retry extension methods for `Api<T>`.
pub trait ApiRetryExt<K> {
    fn retry<'a, T: 'a, F>(
        &'a self,
        policy: RetryPolicy,
        operation: F,
    ) -> impl Future<Output = Result<T, KubeError>> + 'a
    where
        F: for<'b> FnMut(&'b Api<K>) -> BoxFuture<'b, Result<T, KubeError>> + 'a;

    fn list_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        lp: &'a ListParams,
    ) -> impl Future<Output = Result<ObjectList<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn list_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        lp: &'a ListParams,
    ) -> impl Future<Output = Result<ObjectList<PartialObjectMeta<K>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn get_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn get_with_params_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        gp: &'a GetParams,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn get_opt_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<Option<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn get_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<PartialObjectMeta<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn get_metadata_with_params_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        gp: &'a GetParams,
    ) -> impl Future<Output = Result<PartialObjectMeta<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn get_metadata_opt_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<Option<PartialObjectMeta<K>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn get_metadata_opt_with_params_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        gp: &'a GetParams,
    ) -> impl Future<Output = Result<Option<PartialObjectMeta<K>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn create_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        pp: &'a PostParams,
        data: &'a K,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug + Serialize;

    fn patch_with_retry<'a, P>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        pp: &'a PatchParams,
        patch: &'a Patch<P>,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
        P: Serialize + std::fmt::Debug;

    fn patch_metadata_with_retry<'a, P>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        pp: &'a PatchParams,
        patch: &'a Patch<P>,
    ) -> impl Future<Output = Result<PartialObjectMeta<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
        P: Serialize + std::fmt::Debug;

    fn replace_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        pp: &'a PostParams,
        data: &'a K,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug + Serialize;

    fn watch_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        wp: &'a WatchParams,
        version: &'a str,
    ) -> impl Future<Output = Result<WatchRetryStream<'a, K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn watch_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        wp: &'a WatchParams,
        version: &'a str,
    ) -> impl Future<Output = Result<WatchMetadataRetryStream<'a, K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;
}

impl<K> ApiRetryExt<K> for Api<K> {
    fn retry<'a, T: 'a, F>(
        &'a self,
        policy: RetryPolicy,
        operation: F,
    ) -> impl Future<Output = Result<T, KubeError>> + 'a
    where
        F: for<'b> FnMut(&'b Api<K>) -> BoxFuture<'b, Result<T, KubeError>> + 'a,
    {
        let mut operation = operation;
        retry_with_policy(policy, move || operation(self))
    }

    fn list_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        lp: &'a ListParams,
    ) -> impl Future<Output = Result<ObjectList<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.list(lp))
    }

    fn list_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        lp: &'a ListParams,
    ) -> impl Future<Output = Result<ObjectList<PartialObjectMeta<K>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.list_metadata(lp))
    }

    fn get_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.get(name))
    }

    fn get_with_params_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        gp: &'a GetParams,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.get_with(name, gp))
    }

    fn get_opt_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<Option<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.get_opt(name))
    }

    fn get_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<PartialObjectMeta<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.get_metadata(name))
    }

    fn get_metadata_with_params_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        gp: &'a GetParams,
    ) -> impl Future<Output = Result<PartialObjectMeta<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.get_metadata_with(name, gp))
    }

    fn get_metadata_opt_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<Option<PartialObjectMeta<K>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.get_metadata_opt(name))
    }

    fn get_metadata_opt_with_params_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        gp: &'a GetParams,
    ) -> impl Future<Output = Result<Option<PartialObjectMeta<K>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.get_metadata_opt_with(name, gp))
    }

    fn create_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        pp: &'a PostParams,
        data: &'a K,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug + Serialize,
    {
        retry_with_policy(policy, || self.create(pp, data))
    }

    fn patch_with_retry<'a, P>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        pp: &'a PatchParams,
        patch: &'a Patch<P>,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
        P: Serialize + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.patch(name, pp, patch))
    }

    fn patch_metadata_with_retry<'a, P>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        pp: &'a PatchParams,
        patch: &'a Patch<P>,
    ) -> impl Future<Output = Result<PartialObjectMeta<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
        P: Serialize + std::fmt::Debug,
    {
        retry_with_policy(policy, || self.patch_metadata(name, pp, patch))
    }

    fn replace_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
        pp: &'a PostParams,
        data: &'a K,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug + Serialize,
    {
        retry_with_policy(policy, || self.replace(name, pp, data))
    }

    fn watch_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        wp: &'a WatchParams,
        version: &'a str,
    ) -> impl Future<Output = Result<WatchRetryStream<'a, K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || async {
            let stream = self.watch(wp, version).await?;
            Ok::<_, KubeError>(stream.boxed())
        })
    }

    fn watch_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        wp: &'a WatchParams,
        version: &'a str,
    ) -> impl Future<Output = Result<WatchMetadataRetryStream<'a, K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        retry_with_policy(policy, || async {
            let stream = self.watch_metadata(wp, version).await?;
            Ok::<_, KubeError>(stream.boxed())
        })
    }
}
