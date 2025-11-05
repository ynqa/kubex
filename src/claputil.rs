use std::ffi::OsStr;

use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};
use k8s_openapi::api::core::v1::Namespace;
use kube::{Config, config::Kubeconfig};

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
            .map(|named_context| {
                let context = named_context.name.clone();
                CompletionCandidate::new(context)
            })
            .collect()
    })
}

/// Create an `ArgValueCompleter` that lists namespaces from the active kubeconfig.
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

        // Create a tokio runtime to execute async code in a sync context
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(_) => return Vec::new(),
        };

        let input_str = input.to_string_lossy();
        let input_str = input_str.trim();

        rt.block_on(async {
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
                .map(|name| CompletionCandidate::new(name.clone()))
                .collect()
        })
    })
}
