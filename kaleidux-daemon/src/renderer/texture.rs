// Texture pool entry for LRU cache.
pub struct TexturePoolEntry {
    pub texture: wgpu::Texture,
    pub last_used: std::time::Instant,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RetainedTextureFootprint {
    pub current_bytes: u64,
    pub prev_bytes: u64,
    pub composition_bytes: u64,
    pub video_aux_bytes: u64,
}

impl RetainedTextureFootprint {
    pub fn total_bytes(self) -> u64 {
        self.current_bytes
            .saturating_add(self.prev_bytes)
            .saturating_add(self.composition_bytes)
            .saturating_add(self.video_aux_bytes)
    }
}

pub(crate) fn texture_byte_size(width: u32, height: u32, mip_level_count: u32) -> u64 {
    let mut total = 0u64;
    let mut mip_width = width.max(1);
    let mut mip_height = height.max(1);
    let levels = mip_level_count.max(1);

    for _ in 0..levels {
        total += mip_width as u64 * mip_height as u64 * 4;
        if mip_width == 1 && mip_height == 1 {
            break;
        }
        mip_width = (mip_width / 2).max(1);
        mip_height = (mip_height / 2).max(1);
    }

    total
}

pub fn compute_cover_target_dimensions(
    source_width: u32,
    source_height: u32,
    target_width: u32,
    target_height: u32,
) -> (u32, u32) {
    if source_width == 0 || source_height == 0 || target_width == 0 || target_height == 0 {
        return (source_width.max(1), source_height.max(1));
    }

    let width_scale = target_width as f32 / source_width as f32;
    let height_scale = target_height as f32 / source_height as f32;
    let scale = width_scale.max(height_scale).min(1.0);
    let prepared_width =
        ((source_width as f32 * scale).round() as u32).clamp(1, source_width.max(1));
    let prepared_height =
        ((source_height as f32 * scale).round() as u32).clamp(1, source_height.max(1));
    (prepared_width, prepared_height)
}

pub(crate) fn should_pool_texture(width: u32, height: u32, mip_level_count: u32) -> bool {
    texture_byte_size(width, height, mip_level_count) <= super::MAX_POOLED_TEXTURE_BYTES
}

impl super::WgpuContext {
    /// Get a texture from the pool or create a new one
    pub fn get_texture_from_pool(
        &self,
        width: u32,
        height: u32,
        usage: wgpu::TextureUsages,
        metrics: Option<&crate::metrics::PerformanceMetrics>,
    ) -> wgpu::Texture {
        self.get_texture_from_pool_with_mips(width, height, 1, usage, metrics)
    }

    /// Get a texture from the pool or create a new one, with specified mip level count
    pub fn get_texture_from_pool_with_mips(
        &self,
        width: u32,
        height: u32,
        mip_level_count: u32,
        usage: wgpu::TextureUsages,
        metrics: Option<&crate::metrics::PerformanceMetrics>,
    ) -> wgpu::Texture {
        let mut pool = self.texture_pool.lock();
        let key = (width, height, mip_level_count);

        if let Some(entries) = pool.get_mut(&key) {
            let now = std::time::Instant::now();
            entries.retain(|entry| now.duration_since(entry.last_used).as_secs() < 5);

            if let Some(entry) = entries.pop() {
                if let Some(metric) = metrics {
                    metric.record_texture_pool_hit();
                }
                return entry.texture;
            }
        }

        if let Some(metric) = metrics {
            metric.record_texture_pool_miss();
        }
        self.device.create_texture(&wgpu::TextureDescriptor {
            label: Some("Pooled Texture"),
            size: wgpu::Extent3d {
                width,
                height,
                depth_or_array_layers: 1,
            },
            mip_level_count,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Rgba8UnormSrgb,
            usage,
            view_formats: &[],
        })
    }

    /// Return a texture to the pool for reuse
    pub fn return_texture_to_pool(&self, texture: wgpu::Texture, width: u32, height: u32) {
        let mut pool = self.texture_pool.lock();
        let mip_level_count = texture.mip_level_count();
        let key = (width, height, mip_level_count);
        let texture_bytes = texture_byte_size(width, height, mip_level_count);

        if !should_pool_texture(width, height, mip_level_count) {
            return;
        }

        let total_textures: usize = pool.values().map(|entries| entries.len()).sum();
        let total_bytes: u64 = pool
            .iter()
            .map(|(&(width, height, mip_level_count), entries)| {
                texture_byte_size(width, height, mip_level_count) * entries.len() as u64
            })
            .sum();

        if total_textures >= super::MAX_TEXTURE_POOL_SIZE
            || total_bytes.saturating_add(texture_bytes) > super::MAX_TEXTURE_POOL_BYTES
        {
            return;
        }

        let entries = pool.entry(key).or_default();
        if entries.is_empty() {
            entries.push(TexturePoolEntry {
                texture,
                last_used: std::time::Instant::now(),
            });
        }
    }

    /// Clean up old textures from pool
    pub fn cleanup_texture_pool(&self, metrics: Option<&crate::metrics::PerformanceMetrics>) {
        let mut pool = self.texture_pool.lock();
        let now = std::time::Instant::now();

        for entries in pool.values_mut() {
            entries.retain(|entry| now.duration_since(entry.last_used).as_secs() < 10);
        }
        pool.retain(|_, entries| !entries.is_empty());

        let mut total_textures: usize = pool.values().map(|entries| entries.len()).sum();
        let mut total_bytes: u64 = pool
            .iter()
            .map(|(&(width, height, mip_level_count), entries)| {
                texture_byte_size(width, height, mip_level_count) * entries.len() as u64
            })
            .sum();
        if total_textures > super::MAX_TEXTURE_POOL_SIZE
            || total_bytes > super::MAX_TEXTURE_POOL_BYTES
        {
            let mut oldest: Vec<((u32, u32, u32), usize, std::time::Instant, u64)> = pool
                .iter()
                .flat_map(|(&key, entries)| {
                    entries.iter().enumerate().map(move |(index, entry)| {
                        (
                            key,
                            index,
                            entry.last_used,
                            texture_byte_size(key.0, key.1, key.2),
                        )
                    })
                })
                .collect();
            oldest.sort_by(|left, right| right.3.cmp(&left.3).then(left.2.cmp(&right.2)));

            let mut removed_per_key: std::collections::HashMap<(u32, u32, u32), Vec<usize>> =
                std::collections::HashMap::new();
            for &(key, index, _, bytes) in &oldest {
                if total_textures <= super::MAX_TEXTURE_POOL_SIZE
                    && total_bytes <= super::MAX_TEXTURE_POOL_BYTES
                {
                    break;
                }
                removed_per_key.entry(key).or_default().push(index);
                total_textures = total_textures.saturating_sub(1);
                total_bytes = total_bytes.saturating_sub(bytes);
            }
            for (key, mut indices) in removed_per_key {
                indices.sort_unstable_by(|left, right| right.cmp(left));
                if let Some(entries) = pool.get_mut(&key) {
                    for index in indices {
                        if index < entries.len() {
                            entries.remove(index);
                        }
                    }
                }
            }
            pool.retain(|_, entries| !entries.is_empty());
        }

        let pool_size: usize = pool.values().map(|entries| entries.len()).sum();
        if let Some(metric) = metrics {
            metric.record_texture_pool_size(pool_size);
        }
    }

    pub fn texture_pool_stats(&self) -> (usize, u64) {
        let pool = self.texture_pool.lock();
        let count = pool.values().map(|entries| entries.len()).sum();
        let bytes = pool
            .iter()
            .map(|(&(width, height, mip_level_count), entries)| {
                texture_byte_size(width, height, mip_level_count) * entries.len() as u64
            })
            .sum();
        (count, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn texture_size_matches_rgba_pixels_for_single_mip() {
        assert_eq!(texture_byte_size(1920, 1080, 1), 1920 * 1080 * 4);
    }

    #[test]
    fn texture_size_includes_additional_mips() {
        assert!(texture_byte_size(1920, 1080, 4) > texture_byte_size(1920, 1080, 1));
    }

    #[test]
    fn pool_policy_rejects_4k_rgba_textures() {
        assert!(!should_pool_texture(3840, 2160, 1));
        assert!(should_pool_texture(1920, 1080, 1));
    }

    #[test]
    fn retained_footprint_total_sums_sections() {
        let fp = RetainedTextureFootprint {
            current_bytes: 10,
            prev_bytes: 20,
            composition_bytes: 30,
            video_aux_bytes: 40,
        };
        assert_eq!(fp.total_bytes(), 100);
    }

    #[test]
    fn cover_target_preserves_minimum_cover_without_upscaling() {
        assert_eq!(
            compute_cover_target_dimensions(3840, 2160, 1366, 768),
            (1366, 768)
        );
        assert_eq!(
            compute_cover_target_dimensions(3840, 2160, 1280, 1024),
            (1820, 1024)
        );
        assert_eq!(
            compute_cover_target_dimensions(800, 600, 1920, 1080),
            (800, 600)
        );
    }
}
