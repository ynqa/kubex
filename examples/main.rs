use kube::Client;
use kubex::{determine_context, determine_namespace, discover::client::DiscoverClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let context = determine_context(&None)?;
    println!("context={context}");
    let namespace = determine_namespace(None, &context);
    println!("namespace={namespace}");

    let client = Client::try_default().await?;
    let discover = DiscoverClient::new(client);
    let resources = discover.list_api_resources().await?;

    for resource in resources {
        println!(
            "{} {}",
            resource.group.as_deref().unwrap_or("").trim(),
            resource.name
        );
    }

    Ok(())
}
