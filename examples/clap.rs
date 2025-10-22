use clap::{CommandFactory, Parser};
use kubex;

#[derive(Debug, Parser)]
#[command(name = "kubex", about = "Showcase dynamic context completion support")]
struct Cli {
    #[arg(long, add = kubex::claputil::context_value_completer())]
    /// Please type the context name you want to target.
    context: Option<String>,

    /// Please type the namespace name you want to target.
    namespace: Option<String>,
}

fn main() -> anyhow::Result<()> {
    kubex::clap_complete::CompleteEnv::with_factory(Cli::command).complete();

    let cli = Cli::parse();
    println!("Using context: {}", &cli.context.unwrap());
    println!("Using namespace: {}", &cli.namespace.unwrap());
    Ok(())
}
