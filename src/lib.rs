#![cfg_attr(not(doctest), doc = include_str!("../README.md"))]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use clap;
pub use clap_complete;
pub use k8s_openapi;
pub use kube;

pub mod claputil;
pub use claputil::{context_value_completer, namespace_value_completer};
pub mod discover;
pub mod dynamic;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource;
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

/// Finds and returns the `APIResource` that matches the given `resource` name from the list of `api_resources`.
pub fn find_resource(target: &str, api_resources: &[APIResource]) -> Option<APIResource> {
    api_resources
        .iter()
        .find(|api_resource| match_resource(target, api_resource))
        .cloned()
}

/// Checks if the given `api_resource` matches the `target` resource name.
/// Matching is done against the resource's name, singular name, short names, and group-qualified name.
pub fn match_resource(target: &str, api_resource: &APIResource) -> bool {
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
