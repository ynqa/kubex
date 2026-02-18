use std::path::{Path, PathBuf};

use clap::{CommandFactory, Parser};
use kube::Client;
use kubex;
use kubex::discover::{client::DiscoverClient, save_discovery_cache};

const CACHE_PATH: &str = ".kubex-discovery-cache.json";

#[derive(Debug, Parser)]
#[command(name = "kubex", about = "Showcase dynamic context completion support")]
struct Cli {
    #[arg(long, add = kubex::context_value_completer())]
    /// Please type the context name you want to target.
    context: Option<String>,

    #[arg(long, add = kubex::namespace_value_completer())]
    /// Please type the namespace name you want to target.
    namespace: Option<String>,

    #[arg(
        add = kubex::resource_value_completer(
            PathBuf::from(CACHE_PATH),
            None,
        ),
    )]
    /// Please type the resource name you want to target.
    resources: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kubex::clap_complete::CompleteEnv::with_factory(Cli::command).complete();

    let cli = Cli::parse();
    println!(
        "Using context: {}",
        cli.context.as_deref().unwrap_or("not specified")
    );
    println!(
        "Using namespace: {}",
        cli.namespace.as_deref().unwrap_or("not specified")
    );
    println!(
        "Using resources: {}",
        if cli.resources.is_empty() {
            "not specified".to_string()
        } else {
            cli.resources.join(", ")
        }
    );

    if !Path::new(CACHE_PATH).exists() {
        println!(
            "Discovery cache not found, for next run, the resource completion will be faster."
        );
        let client = Client::try_default().await?;
        let resources = DiscoverClient::new(client.clone())
            .list_api_resources()
            .await?;
        save_discovery_cache(Path::new(CACHE_PATH), &resources)?;
    }
    Ok(())
}
