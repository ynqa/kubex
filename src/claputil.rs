use std::{collections::HashSet, ffi::OsStr, path::PathBuf, time::Duration};

use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};
use k8s_openapi::api::core::v1::Namespace;
use kube::{Config, config::Kubeconfig};
use tokio::{runtime::Handle, task};

use crate::ResourceTargetSpec;

/// Create an `ArgValueCompleter` that lists contexts from the active kubeconfig.
pub fn context_value_completer() -> ArgValueCompleter {
    ArgValueCompleter::new(|input: &OsStr| -> Vec<CompletionCandidate> {
        let kubeconfig = match Kubeconfig::read() {
            Ok(config) => config,
            Err(_) => return Vec::new(),
        };

        // Convert OsStr to &str with trimmed whitespace
        let input = input.to_string_lossy();
        let input = input.trim();

        kubeconfig
            .contexts
            .iter()
            .filter(|named_context| named_context.name.starts_with(input))
            .map(|named_context| CompletionCandidate::new(named_context.name.as_str()))
            .collect()
    })
}

/// Create an `ArgValueCompleter` that lists namespaces from the active kubeconfig.
///
/// This function makes a network call to the Kubernetes cluster to retrieve the list of namespaces.
/// As a result, it may be slow or fail silently (returning an empty list) in case of network issues,
/// authentication failures, or missing permissions.
///
/// When called within an existing Tokio runtime, it uses `block_in_place` to avoid panicking and
/// blocks on the current runtime handle. If no runtime exists, it creates a new Tokio runtime to
/// perform the network call.
///
/// Limitation: The context specified by --context is not considered.
/// See https://github.com/clap-rs/clap/issues/1910 for more details.
pub fn namespace_value_completer() -> ArgValueCompleter {
    ArgValueCompleter::new(|input: &OsStr| -> Vec<CompletionCandidate> {
        let kubeconfig = match Kubeconfig::read() {
            Ok(config) => config,
            Err(_) => return Vec::new(),
        };

        let current_ctx = match &kubeconfig.current_context {
            Some(name) => name,
            None => return Vec::new(),
        };

        let options = kube::config::KubeConfigOptions {
            context: Some(current_ctx.clone()),
            ..Default::default()
        };

        let input_str = input.to_string_lossy();
        let input_str = input_str.trim();

        let namespaces_future = async {
            let config = match Config::from_custom_kubeconfig(kubeconfig, &options).await {
                Ok(cfg) => cfg,
                Err(_) => return Vec::new(),
            };

            let client = match kube::Client::try_from(config) {
                Ok(c) => c,
                Err(_) => return Vec::new(),
            };

            let namespaces: kube::Api<Namespace> = kube::Api::all(client);

            let ns_list = match namespaces.list(&Default::default()).await {
                Ok(list) => list,
                Err(_) => return Vec::new(),
            };

            ns_list
                .items
                .iter()
                .filter_map(|ns| ns.metadata.name.as_ref())
                .filter(|name| name.starts_with(input_str))
                .map(CompletionCandidate::new)
                .collect()
        };

        // If called on an existing Tokio runtime, `Runtime::block_on` will panic.
        // Therefore, if a runtime exists, we use `block_in_place` to escape to a blocking thread,
        // and from there we call `block_on` with the current handle.
        match Handle::try_current() {
            Ok(handle) => task::block_in_place(move || handle.block_on(namespaces_future)),
            Err(_) => tokio::runtime::Runtime::new()
                .map(|rt| rt.block_on(namespaces_future))
                .unwrap_or_default(),
        }
    })
}

/// Create an `ArgValueCompleter` that lists Kubernetes resources.
///
/// Completion candidates are generated from `singular_name` only.
/// This completer works with comma-delimited values and completes the current token.
/// When `cache_path` is `Some`, `cache_path` and `cache_ttl` are forwarded to
/// `resolve_requested_resources_from_cache`; when `None`, it returns no candidates.
/// Cache is the only source; no live API discovery is performed here.
pub fn resource_value_completer(
    cache_path: Option<PathBuf>,
    cache_ttl: Option<Duration>,
) -> ArgValueCompleter {
    ArgValueCompleter::new(move |input: &OsStr| -> Vec<CompletionCandidate> {
        let Some(cache_path) = cache_path.as_ref() else {
            return Vec::new();
        };

        let current_token = input.to_string_lossy().trim().to_string();

        let resources = match crate::resolve_requested_resources_from_cache(
            &ResourceTargetSpec::AllResources,
            cache_path.as_path(),
            cache_ttl,
        ) {
            Ok(resources) => resources,
            Err(_) => return Vec::new(),
        };

        let mut seen = HashSet::new();
        resources
            .into_iter()
            .filter_map(|resource| {
                let singular_name = resource.singular_name.trim();
                if singular_name.is_empty() {
                    None
                } else {
                    Some(singular_name.to_string())
                }
            })
            .filter(|candidate| candidate.starts_with(&current_token))
            .filter(|candidate| seen.insert(candidate.clone()))
            // Append a comma so selecting a candidate keeps typing on the next value.
            .map(|candidate| CompletionCandidate::new(candidate))
            .collect()
    })
}
