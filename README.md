# kubex

## Overview

*kubex* is a Rust library that provides utilities for Kubernetes. It complements 
[kube](https://crates.io/crates/kube) and [k8s-openapi](https://crates.io/crates/k8s-openapi)
crates, making it easier to detect contexts and explore API resources.

## Installation

Add the following dependency to your `Cargo.toml`.

```toml
[dependencies]
kubex = "0.3.0"
```

## Example

An example that combines `DiscoverClient` with the helper functions for namespaces and kube contexts.

```bash
cargo run --example main
```

```rust
use kubex::{determine_context, determine_namespace, discover::DiscoverClient};
use kube::Client;

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
        println!("{} {}", resource.group.as_deref().unwrap_or("").trim(), resource.name);
    }

    Ok(())
}
```

## License

This project is released under the MIT License.
