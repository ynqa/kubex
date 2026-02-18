use std::{fs, path::Path, time::Duration};

use anyhow::Context;
use k8s_openapi::{
    apimachinery::pkg::apis::meta::v1::APIResource,
    chrono::{DateTime, TimeDelta, Utc},
};
use serde::{Deserialize, Serialize};

use crate::ResourceTargetSpec;

pub mod client;

/// Represent the discovery cache file format,
/// which includes the timestamp of when the API resources were fetched
/// and the list of resources.
#[derive(Debug, Serialize, Deserialize)]
pub struct DiscoveryCacheFile {
    /// The timestamp when the API resources were saved to the cache.
    updated_at: DateTime<Utc>,
    /// The list of API resources discovered from the Kubernetes cluster.
    resources: Vec<APIResource>,
}

/// Load the discovery cache from a file at the specified path.
pub fn load_discovery_cache(path: &Path) -> anyhow::Result<DiscoveryCacheFile> {
    let cache_data = fs::read_to_string(path).context("Failed to read discovery cache file")?;
    serde_json::from_str(&cache_data).context("Failed to parse discovery cache file")
}

/// Save the discovery cache to a file at the specified path
pub fn save_discovery_cache(path: &Path, resources: &[APIResource]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create cache directory at {:?}", parent))?;
    }

    let cache_file = DiscoveryCacheFile {
        updated_at: Utc::now(),
        resources: resources.to_vec(),
    };

    let cache_data = serde_json::to_string(&cache_file)
        .context("Failed to serialize discovery cache data to JSON")?;

    fs::write(path, cache_data)
        .with_context(|| format!("Failed to write discovery cache to {:?}", path))?;
    Ok(())
}

/// Resolve requested API resources from discovery cache only.
///
/// This function never performs live discovery against the Kubernetes cluster.
/// It returns an error when `cache_path` is not provided, the cache cannot be loaded,
/// or the cache is expired based on `cache_ttl`.
pub fn resolve_requested_resources_with_cache(
    spec: &ResourceTargetSpec,
    cache_path: &Path,
    cache_ttl: Option<Duration>,
) -> anyhow::Result<Vec<APIResource>> {
    let cache = load_discovery_cache(cache_path)?;

    if let Some(ttl) = cache_ttl {
        let cache_age = Utc::now() - cache.updated_at;
        let ttl = TimeDelta::from_std(ttl).unwrap_or(TimeDelta::MAX);
        if cache_age > ttl {
            return Err(anyhow::anyhow!(
                "discovery cache expired at {cache_path:?} (age: {cache_age:?}, ttl: {ttl:?})"
            ));
        }
    }

    crate::resolve_all_targets(spec, &cache.resources)
}
