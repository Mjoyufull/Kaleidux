use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn sanitize_cache_component(component: &str) -> String {
    let mut sanitized = String::with_capacity(component.len());
    for ch in component.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
        } else if !sanitized.ends_with('-') {
            sanitized.push('-');
        }
    }
    sanitized.trim_matches('-').to_string()
}

pub(crate) fn path_for_adapter(adapter: &wgpu::Adapter) -> Option<PathBuf> {
    let info = adapter.get_info();
    let cache_dir = dirs::cache_dir()?.join("kaleidux").join("wgpu");
    let adapter_name = sanitize_cache_component(info.name.as_str());
    Some(cache_dir.join(format!(
        "pipeline-cache-v2-{:?}-{:04x}-{:04x}-{}.bin",
        info.backend, info.vendor, info.device, adapter_name
    )))
}

pub(crate) fn load_seed(path: &Path) -> Option<Vec<u8>> {
    match std::fs::read(path) {
        Ok(data) if !data.is_empty() => Some(data),
        Ok(_) => None,
        Err(_) => None,
    }
}

// LRU cache for transition pipelines. Lookups and access updates are O(1), while
// eviction scans `access_order` to find the least-recently-used entry.
pub struct PipelineLRU {
    pipelines: HashMap<String, Arc<wgpu::RenderPipeline>>,
    access_order: HashMap<String, u64>,
    order_counter: u64,
    max_size: usize,
}

impl PipelineLRU {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            pipelines: HashMap::new(),
            access_order: HashMap::new(),
            order_counter: 0,
            max_size,
        }
    }

    pub(crate) fn get(&mut self, key: &str) -> Option<Arc<wgpu::RenderPipeline>> {
        match self.pipelines.get(key).cloned() {
            Some(pipeline) => {
                self.order_counter += 1;
                self.access_order
                    .insert(key.to_string(), self.order_counter);
                Some(pipeline)
            }
            _ => None,
        }
    }

    pub(crate) fn insert(&mut self, key: String, pipeline: Arc<wgpu::RenderPipeline>) {
        if !self.pipelines.contains_key(&key) {
            while self.pipelines.len() >= self.max_size {
                if let Some(lru_key) = self
                    .access_order
                    .iter()
                    .min_by_key(|(_, value)| *value)
                    .map(|(key, _)| key.clone())
                {
                    self.pipelines.remove(&lru_key);
                    self.access_order.remove(&lru_key);
                } else {
                    break;
                }
            }
        }
        self.order_counter += 1;
        self.access_order.insert(key.clone(), self.order_counter);
        self.pipelines.insert(key, pipeline);
    }

    pub fn len(&self) -> usize {
        self.pipelines.len()
    }

    #[allow(dead_code)]
    pub fn contains_key(&self, key: &str) -> bool {
        self.pipelines.contains_key(key)
    }
}
