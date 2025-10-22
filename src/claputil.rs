use std::{collections::BTreeSet, ffi::OsStr};

use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};
use k8s_openapi::api::core::v1::Namespace;
use kube::{
    Api, Client, Config,
    api::ListParams,
    config::{KubeConfigOptions, Kubeconfig},
};

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

/// Create an `ArgValueCompleter` that lists namespaces, optionally querying the cluster when context is known.
pub fn namespace_value_completer() -> ArgValueCompleter {
    ArgValueCompleter::new(|input: &OsStr| -> Vec<CompletionCandidate> {
        let input = input.to_string_lossy();
        let input = input.trim();

        let namespaces = detect_unique_context_from_args()
            .and_then(|context| namespaces_from_api(&context))
            .unwrap_or_else(fallback_namespaces);

        namespaces
            .into_iter()
            .filter(|namespace| namespace.starts_with(input))
            .map(CompletionCandidate::new)
            .collect()
    })
}

fn detect_unique_context_from_args() -> Option<String> {
    let contexts = parse_contexts_from_args();
    let unique: BTreeSet<_> = contexts.into_iter().collect();
    let mut iter = unique.into_iter();
    let first = iter.next()?;
    if iter.next().is_none() {
        Some(first)
    } else {
        None
    }
}

fn parse_contexts_from_args() -> Vec<String> {
    let mut contexts = Vec::new();
    let mut args = std::env::args_os();
    let _ = args.next();

    let mut expect_value = false;
    for arg in args {
        if expect_value {
            expect_value = false;
            if let Some(value) = arg.to_str() {
                if !value.is_empty() {
                    contexts.push(value.to_string());
                }
            }
            continue;
        }

        if arg == "--" {
            break;
        }

        let arg_string = arg.to_string_lossy();
        if arg_string == "--context" || arg_string == "-c" {
            expect_value = true;
            continue;
        }

        if let Some(value) = arg_string.strip_prefix("--context=") {
            if !value.is_empty() {
                contexts.push(value.to_string());
            }
            continue;
        }

        if let Some(value) = arg_string.strip_prefix("-c") {
            if !value.is_empty() {
                contexts.push(value.to_string());
            }
        }
    }

    contexts
}

fn namespaces_from_api(context: &str) -> Option<Vec<String>> {
    let runtime = tokio::runtime::Runtime::new().ok()?;
    runtime.block_on(async {
        let options = KubeConfigOptions {
            context: Some(context.to_string()),
            ..Default::default()
        };
        let config = Config::from_kubeconfig(&options).await.ok()?;
        let client = Client::try_from(config).ok()?;
        let api: Api<Namespace> = Api::all(client);
        let namespace_list = api.list(&ListParams::default()).await.ok()?;

        let mut namespaces: Vec<String> = namespace_list
            .into_iter()
            .filter_map(|namespace| namespace.metadata.name)
            .collect();
        namespaces.sort();
        namespaces.dedup();
        Some(namespaces)
    })
}

fn fallback_namespaces() -> Vec<String> {
    let mut namespaces: BTreeSet<String> = BTreeSet::from([String::from("default")]);
    if let Ok(kubeconfig) = Kubeconfig::read() {
        for named_context in &kubeconfig.contexts {
            if let Some(namespace) = named_context
                .context
                .as_ref()
                .and_then(|ctx| ctx.namespace.clone())
            {
                if !namespace.is_empty() {
                    namespaces.insert(namespace);
                }
            }
        }
    }

    namespaces.into_iter().collect()
}
