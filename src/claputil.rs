use std::ffi::OsStr;

use clap::{Arg, ValueHint};
use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};
use kube::config::Kubeconfig;

/// Build a `--context` argument with dynamic kubeconfig-aware completions.
pub fn context_arg() -> Arg {
    Arg::new("context")
        .long("context")
        .value_name("CONTEXT")
        .help("Override the Kubernetes context to target")
        .value_hint(ValueHint::Other)
        .add(context_value_completer())
}

/// Create an `ArgValueCompleter` that lists contexts from the active kubeconfig.
pub fn context_value_completer() -> ArgValueCompleter {
    ArgValueCompleter::new(|input: &OsStr| -> Vec<CompletionCandidate> {
        let kubeconfig = match Kubeconfig::read() {
            Ok(config) => config,
            Err(_) => return Vec::new(),
        };

        let current_context = kubeconfig.current_context.clone();

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
