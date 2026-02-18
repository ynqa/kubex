use std::future::Future;

use futures::{
    future::BoxFuture,
    stream::{LocalBoxStream, StreamExt},
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

/// Retry extension methods for `Api<T>`.
pub trait ApiRetryExt<K> {
    fn retry<T, F>(
        &self,
        policy: RetryPolicy,
        operation: F,
    ) -> impl Future<Output = Result<T, KubeError>>
    where
        F: for<'a> FnMut(&'a Api<K>) -> BoxFuture<'a, Result<T, KubeError>>;

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
    ) -> impl Future<Output = Result<LocalBoxStream<'a, Result<WatchEvent<K>, KubeError>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;

    fn watch_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        wp: &'a WatchParams,
        version: &'a str,
    ) -> impl Future<
        Output = Result<
            LocalBoxStream<'a, Result<WatchEvent<PartialObjectMeta<K>>, KubeError>>,
            KubeError,
        >,
    > + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug;
}

impl<K> ApiRetryExt<K> for Api<K> {
    fn retry<T, F>(
        &self,
        policy: RetryPolicy,
        operation: F,
    ) -> impl Future<Output = Result<T, KubeError>>
    where
        F: for<'a> FnMut(&'a Api<K>) -> BoxFuture<'a, Result<T, KubeError>>,
    {
        async move {
            let mut operation = operation;
            retry_with_policy(policy, || operation(self)).await
        }
    }

    fn list_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        lp: &'a ListParams,
    ) -> impl Future<Output = Result<ObjectList<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        async move { retry_with_policy(policy, || self.list(lp)).await }
    }

    fn list_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        lp: &'a ListParams,
    ) -> impl Future<Output = Result<ObjectList<PartialObjectMeta<K>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        async move { retry_with_policy(policy, || self.list_metadata(lp)).await }
    }

    fn get_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<K, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        async move { retry_with_policy(policy, || self.get(name)).await }
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
        async move { retry_with_policy(policy, || self.get_with(name, gp)).await }
    }

    fn get_opt_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<Option<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        async move { retry_with_policy(policy, || self.get_opt(name)).await }
    }

    fn get_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<PartialObjectMeta<K>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        async move { retry_with_policy(policy, || self.get_metadata(name)).await }
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
        async move { retry_with_policy(policy, || self.get_metadata_with(name, gp)).await }
    }

    fn get_metadata_opt_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        name: &'a str,
    ) -> impl Future<Output = Result<Option<PartialObjectMeta<K>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        async move { retry_with_policy(policy, || self.get_metadata_opt(name)).await }
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
        async move { retry_with_policy(policy, || self.get_metadata_opt_with(name, gp)).await }
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
        async move { retry_with_policy(policy, || self.create(pp, data)).await }
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
        async move { retry_with_policy(policy, || self.patch(name, pp, patch)).await }
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
        async move { retry_with_policy(policy, || self.patch_metadata(name, pp, patch)).await }
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
        async move { retry_with_policy(policy, || self.replace(name, pp, data)).await }
    }

    fn watch_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        wp: &'a WatchParams,
        version: &'a str,
    ) -> impl Future<Output = Result<LocalBoxStream<'a, Result<WatchEvent<K>, KubeError>>, KubeError>> + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        async move {
            retry_with_policy(policy, || async {
                let stream = self.watch(wp, version).await?;
                Ok::<_, KubeError>(stream.boxed_local())
            })
            .await
        }
    }

    fn watch_metadata_with_retry<'a>(
        &'a self,
        policy: RetryPolicy,
        wp: &'a WatchParams,
        version: &'a str,
    ) -> impl Future<
        Output = Result<
            LocalBoxStream<'a, Result<WatchEvent<PartialObjectMeta<K>>, KubeError>>,
            KubeError,
        >,
    > + 'a
    where
        K: Clone + DeserializeOwned + std::fmt::Debug,
    {
        async move {
            retry_with_policy(policy, || async {
                let stream = self.watch_metadata(wp, version).await?;
                Ok::<_, KubeError>(stream.boxed_local())
            })
            .await
        }
    }
}
