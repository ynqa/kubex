#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use k8s_openapi;
pub use kube;

pub mod discover;
pub mod dynamic;

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
