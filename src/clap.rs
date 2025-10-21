use std::ffi::OsStr;

use clap::{Arg, ValueHint, builder::StyledStr};
use clap_complete::engine::{ArgValueCompleter, CompletionCandidate};

pub use clap_complete::env::CompleteEnv;
use kube::config::Kubeconfig;

/// Re-export dynamic completion helpers for consumer crates.
pub use clap_complete::engine;

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
    ArgValueCompleter::new(|current: &OsStr| -> Vec<CompletionCandidate> {
        let kubeconfig = match Kubeconfig::read() {
            Ok(config) => config,
            Err(_) => return Vec::new(),
        };

        let current_context = kubeconfig.current_context.clone();
        let prefix_owned = current.to_string_lossy();
        let prefix = prefix_owned.trim();

        let mut completions = Vec::new();

        for named_context in kubeconfig.contexts.into_iter() {
            let context_name = named_context.name;

            if !prefix.is_empty() && !context_name.starts_with(prefix) {
                continue;
            }

            let mut candidate = CompletionCandidate::new(context_name.clone());
            let is_current = current_context
                .as_ref()
                .is_some_and(|ctx| ctx == &context_name);

            let mut details = Vec::new();
            if let Some(ctx) = named_context.context {
                details.push(format!("cluster={}", ctx.cluster));
                if let Some(namespace) = ctx.namespace {
                    details.push(format!("namespace={namespace}"));
                }
            }

            if is_current {
                details.insert(0, String::from("[current]"));
            }

            if !details.is_empty() {
                let info = details.join(" ");
                candidate = candidate.help(Some(StyledStr::from(info)));
            }

            if is_current {
                candidate = candidate.display_order(Some(0));
                completions.insert(0, candidate);
            } else {
                completions.push(candidate);
            }
        }

        completions
    })
}
