#![cfg_attr(not(doctest), doc = include_str!("../README.md"))]
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{collections::HashSet, path::Path, time::Duration};

pub use clap_complete;
pub use k8s_openapi;
pub use kube;

pub mod claputil;
pub use claputil::{context_value_completer, namespace_value_completer, resource_value_completer};
pub mod discover;
pub mod dynamic;

use k8s_openapi::{
    apimachinery::pkg::apis::meta::v1::APIResource,
    chrono::{TimeDelta, Utc},
};
use kube::config::Kubeconfig;

/// Detects the Kubernetes context based on the provided `context` argument.
///
/// Context determination follows this priority:
/// 1. Uses the context if explicitly specified.
/// 2. Retrieves the current context from the kubeconfig file.
///
/// # Errors
/// Returns an error if the kubeconfig file cannot be read or if no current context is set in the kubeconfig.
pub fn determine_context(context: &Option<String>) -> anyhow::Result<String> {
    match context {
        Some(context) => Ok(context.to_string()),
        _ => {
            let kubeconfig = Kubeconfig::read()?;
            Ok(kubeconfig
                .current_context
                .ok_or_else(|| anyhow::anyhow!("current_context is not set"))?)
        }
    }
}

/// Determines the Kubernetes namespace based on the provided `namespace` and `context`.
///
/// Namespace determination follows this priority:
/// 1. Uses the namespace if explicitly specified.
/// 2. Retrieves the default namespace associated with the current context from kubeconfig.
/// 3. Uses "default".
pub fn determine_namespace(namespace: Option<String>, context: &str) -> String {
    if let Some(ns) = namespace {
        return ns;
    }

    match Kubeconfig::read() {
        Ok(kubeconfig) => kubeconfig
            .contexts
            .iter()
            .find(|c| Some(c.name.as_str()) == Some(context))
            .and_then(|context| {
                context
                    .context
                    .as_ref()
                    .and_then(|ctx| ctx.namespace.clone())
            })
            .unwrap_or_else(|| String::from("default")),
        Err(_) => String::from("default"),
    }
}

/// Resolve the requested target resource name from the list of discovered API resources.
pub fn resolve_target(target: &str, api_resources: &[APIResource]) -> Option<APIResource> {
    api_resources
        .iter()
        .find(|api_resource| resource_matches_target(target, api_resource))
        .cloned()
}

/// Checks if the given `api_resource` matches the `target` resource name.
/// Matching is done against the resource's name, singular name, short names, and group-qualified name.
pub fn resource_matches_target(target: &str, api_resource: &APIResource) -> bool {
    api_resource.name == target
        || api_resource.singular_name == target
        || api_resource
            .short_names
            .as_ref()
            .is_some_and(|short_names| short_names.contains(&target.to_string()))
        || api_resource
            .group
            .as_ref()
            .is_some_and(|group| format!("{}.{}", api_resource.name, group) == target)
}

/// Target specification for resource resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceTargetSpec {
    /// Resolve all discovered resources.
    AllResources,
    /// All specified targets must be resolved.
    AllOf(Vec<String>),
    /// At least one specified target must be resolved.
    AnyOf(Vec<String>),
}

/// Resolve target resources against the list of discovered API resources.
pub fn resolve_all_targets(
    spec: &ResourceTargetSpec,
    resources: &[APIResource],
) -> anyhow::Result<Vec<APIResource>> {
    match spec {
        ResourceTargetSpec::AllResources => Ok(resources.to_vec()),
        ResourceTargetSpec::AllOf(targets) => {
            let mut matched = Vec::new();
            let mut unmatched = Vec::new();

            for target in targets {
                if let Some(api_resource) = resolve_target(target, resources) {
                    matched.push(api_resource);
                } else {
                    unmatched.push(target.clone());
                }
            }

            if unmatched.is_empty() {
                Ok(matched)
            } else {
                Err(anyhow::anyhow!(
                    "the following requested resources could not be resolved: {}",
                    unmatched.join(", ")
                ))
            }
        }
        ResourceTargetSpec::AnyOf(targets) => {
            let mut seen = HashSet::new();
            let mut matched = Vec::new();
            for target in targets {
                if let Some(api_resource) = resolve_target(target, resources) {
                    let key = format!(
                        "{}|{}|{}",
                        api_resource.name,
                        api_resource.group.clone().unwrap_or_default(),
                        api_resource.version.clone().unwrap_or_default()
                    );
                    if seen.insert(key) {
                        matched.push(api_resource);
                    }
                }
            }

            if matched.is_empty() {
                Err(anyhow::anyhow!(
                    "none of the requested resources could be resolved: {}",
                    targets.join(", ")
                ))
            } else {
                Ok(matched)
            }
        }
    }
}

/// Resolve requested API resources from discovery cache only.
///
/// This function never performs live discovery against the Kubernetes cluster.
/// It returns an error when `cache_path` is not provided, the cache cannot be loaded,
/// or the cache is expired based on `cache_ttl`.
pub fn resolve_requested_resources_with_cache(
    spec: &ResourceTargetSpec,
    cache_path: &Path,
    cache_ttl: Option<Duration>,
) -> anyhow::Result<Vec<APIResource>> {
    let cache = discover::load_discovery_cache(cache_path)?;

    if let Some(ttl) = cache_ttl {
        let cache_age = Utc::now() - cache.updated_at;
        let ttl = TimeDelta::from_std(ttl).unwrap_or(TimeDelta::MAX);
        if cache_age > ttl {
            return Err(anyhow::anyhow!(
                "discovery cache expired at {cache_path:?} (age: {cache_age:?}, ttl: {ttl:?})"
            ));
        }
    }

    crate::resolve_all_targets(spec, &cache.resources)
}
