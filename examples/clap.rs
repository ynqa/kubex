use clap::{CommandFactory, Parser};
use kubex::{clap as kubex_clap, determine_context};

#[derive(Debug, Parser)]
#[command(
    name = "kubex-clap",
    about = "Showcase dynamic context completion support"
)]
struct Cli {
    #[arg(long, add = kubex_clap::context_value_completer())]
    /// Please type the context name you want to target.
    context: Option<String>,
}

fn main() -> anyhow::Result<()> {
    kubex_clap::CompleteEnv::with_factory(|| Cli::command()).complete();

    let cli = Cli::parse();
    let context = determine_context(&cli.context)?;
    println!("Using context: {context}");
    Ok(())
}
