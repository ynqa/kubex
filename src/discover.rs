use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResource;
use kube::Client;
use serde::{Deserialize, Serialize};

pub mod client;
use client::DiscoverClient;

const DISCOVERY_CACHE_TTL_SECS: u64 = 600;

#[derive(Debug, Serialize, Deserialize)]
struct DiscoveryCacheFile {
    fetched_at_unix_secs: u64,
    resources: Vec<APIResource>,
}

#[derive(Debug)]
struct LoadedDiscoveryCache {
    resources: Vec<APIResource>,
    is_fresh: bool,
}

pub async fn resolve_requested_resources(
    client: &Client,
    context: &str,
    targets: &[String],
) -> anyhow::Result<Vec<APIResource>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let cache_path = discovery_cache_path(context);
    let loaded_cache = match &cache_path {
        Some(path) => load_discovery_cache(path),
        None => None,
    };

    if let Some(cache) = loaded_cache.as_ref().filter(|cache| cache.is_fresh) {
        if let Ok(matched) = match_all_targets(targets, &cache.resources) {
            return Ok(matched);
        }
    }

    match DiscoverClient::new(client.clone())
        .list_api_resources()
        .await
    {
        Ok(resources) => {
            if let Some(path) = &cache_path {
                let _ = save_discovery_cache(path, &resources);
            }
            match_all_targets(targets, &resources)
        }
        Err(err) => {
            if let Some(cache) = loaded_cache {
                if let Ok(matched) = match_all_targets(targets, &cache.resources) {
                    return Ok(matched);
                }
            }
            Err(err).context("failed to discover Kubernetes API resources")
        }
    }
}

fn match_all_targets(
    targets: &[String],
    resources: &[APIResource],
) -> anyhow::Result<Vec<APIResource>> {
    let mut matched = HashMap::new();
    let mut unresolved = Vec::new();

    for target in targets {
        if let Some(api_resource) = resources
            .iter()
            .find(|api_resource| crate::match_resource(target, api_resource))
            .cloned()
        {
            matched
                .entry(api_resource.name.clone())
                .or_insert(api_resource);
        } else {
            unresolved.push(target.clone());
        }
    }

    if unresolved.is_empty() {
        Ok(matched.into_values().collect())
    } else {
        Err(anyhow::anyhow!(
            "resource not found: {}",
            unresolved.join(", ")
        ))
    }
}

fn discovery_cache_path(context: &str) -> Option<PathBuf> {
    let home = dirs::config_dir()?;
    let sanitized_context = context
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    Some(home.join("keye").join(format!("{sanitized_context}.json")))
}

fn load_discovery_cache(path: &Path) -> Option<LoadedDiscoveryCache> {
    let data = fs::read(path).ok()?;
    let cache: DiscoveryCacheFile = serde_json::from_slice(&data).ok()?;
    let now_secs = now_unix_secs();
    let age_secs = now_secs.saturating_sub(cache.fetched_at_unix_secs);
    let is_fresh = age_secs <= DISCOVERY_CACHE_TTL_SECS;
    Some(LoadedDiscoveryCache {
        resources: cache.resources,
        is_fresh,
    })
}

fn save_discovery_cache(path: &Path, resources: &[APIResource]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let cache = DiscoveryCacheFile {
        fetched_at_unix_secs: now_unix_secs(),
        resources: resources.to_vec(),
    };
    let bytes = serde_json::to_vec(&cache)?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, bytes)?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}
