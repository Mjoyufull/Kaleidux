pub use kaleidux_common::Transition;

mod builtin_mapping;
mod builtin_sources;
mod safe_sources;

use safe_sources::{CUBE_SAFE_GLSL, DISPLACEMENT_SAFE_GLSL, GLSL_PRELUDE};

pub struct ShaderManager;

const WGSL_DISK_CACHE_VERSION: u32 = 2;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct CachedWgslEntry {
    fingerprint: u64,
    wgsl: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WgslDiskCache {
    version: u32,
    entries: std::collections::HashMap<String, CachedWgslEntry>,
}

// Process-wide cache of compiled WGSL shader strings (P-21)
// Keyed by transition name — avoids duplicate GLSL→WGSL compilation across renderers
static WGSL_CACHE: once_cell::sync::Lazy<
    parking_lot::Mutex<std::collections::HashMap<String, CachedWgslEntry>>,
> = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));
static BROKEN_TRANSITIONS: once_cell::sync::Lazy<
    parking_lot::Mutex<std::collections::HashSet<String>>,
> = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(std::collections::HashSet::new()));

use anyhow::Context;

fn stable_shader_fingerprint(parts: &[&str]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

impl ShaderManager {
    pub fn mark_transition_broken(name: &str) {
        BROKEN_TRANSITIONS.lock().insert(name.to_string());
    }

    pub fn is_transition_broken(name: &str) -> bool {
        BROKEN_TRANSITIONS.lock().contains(name)
    }

    pub fn is_shader_cached(transition: &Transition) -> bool {
        let name = transition.name();
        WGSL_CACHE
            .lock()
            .get(&name)
            .is_some_and(|entry| Self::cache_entry_matches_transition(transition, entry))
    }

    pub fn save_cache() -> anyhow::Result<()> {
        let cache_dir = dirs::cache_dir().context("no cache dir")?.join("kaleidux");
        std::fs::create_dir_all(&cache_dir)?;
        let data = {
            let cache = WGSL_CACHE.lock();
            postcard::to_allocvec(&WgslDiskCache {
                version: WGSL_DISK_CACHE_VERSION,
                entries: cache.clone(),
            })?
        };
        let tmp = cache_dir.join("wgsl_cache.bin.tmp");
        let dst = cache_dir.join("wgsl_cache.bin");
        std::fs::write(&tmp, &data)?;
        std::fs::rename(&tmp, &dst)?;
        Ok(())
    }

    pub fn load_cache() -> anyhow::Result<()> {
        let path = dirs::cache_dir()
            .context("no cache dir")?
            .join("kaleidux")
            .join("wgsl_cache.bin");
        if path.exists() {
            let data = std::fs::read(&path)?;
            let mut cache = WGSL_CACHE.lock();
            let loaded: std::collections::HashMap<String, CachedWgslEntry> =
                match postcard::from_bytes::<WgslDiskCache>(&data) {
                    Ok(blob) if blob.version == WGSL_DISK_CACHE_VERSION => blob.entries,
                    Ok(blob) => {
                        tracing::info!(
                            "[SHADER] Ignoring WGSL disk cache (file version {} != {})",
                            blob.version,
                            WGSL_DISK_CACHE_VERSION
                        );
                        return Ok(());
                    }
                    Err(_) => {
                        tracing::info!(
                            "[SHADER] Ignoring legacy WGSL disk cache (missing version header)"
                        );
                        return Ok(());
                    }
                };
            for (k, v) in loaded {
                cache.entry(k).or_insert(v);
            }
            tracing::info!(
                "[SHADER] Loaded {} cached WGSL shaders from disk",
                cache.len()
            );
        }
        Ok(())
    }

    pub fn pick_random_transition() -> Transition {
        use rand::seq::SliceRandom;

        let mut candidates: Vec<Transition> = Transition::random_candidate_names()
            .iter()
            .map(|name| Transition::from_name(name))
            .collect();
        let mut rng = rand::thread_rng();

        candidates.shuffle(&mut rng);
        candidates.sort_by_key(|transition| {
            let name = transition.name();
            (
                Self::is_transition_broken(&name),
                !Self::is_shader_cached(transition),
            )
        });

        for transition in candidates {
            let name = transition.name();
            if Self::is_transition_broken(&name) {
                continue;
            }
            match Self::get_builtin_shader(&transition) {
                Ok(_) => return transition,
                Err(e) => {
                    Self::mark_transition_broken(&name);
                    tracing::warn!(
                        "[SHADER] Skipping random transition {} after compile failure: {}",
                        name,
                        e
                    );
                }
            }
        }

        tracing::warn!("[SHADER] No working random transitions available, falling back to fade");
        Transition::Fade
    }

    pub fn compile_glsl(
        name: &str,
        user_code: &str,
        params_mapping: &str,
    ) -> anyhow::Result<String> {
        // 1. Convert params_mapping from "type var = val;" to "#define var (val)"
        let mut defines = String::new();
        // Regex matches "type name = value" ignoring trailing semicolon
        // Use lazy static to compile once and reuse
        static MAPPING_REGEX: once_cell::sync::Lazy<regex::Regex> =
            once_cell::sync::Lazy::new(|| {
                regex::Regex::new(r"^\s*\w+\s+(\w+)\s*=\s*(.+)$").expect("Failed to compile regex")
            });
        let mapping_regex = &*MAPPING_REGEX;

        for stmt in params_mapping.split(';') {
            let s = stmt.trim();
            if s.is_empty() {
                continue;
            }
            if let Some(caps) = mapping_regex.captures(s) {
                let var_name = &caps[1];
                let val = &caps[2];
                // Check if value ends with semicolon (regex greedy match might capture it if not careful,
                // but split(';') removes the delimiter. If implicit semicolon was in the regex match, it might be an issue.
                // Our input strings in get_builtin_shader don't have nested semicolons usually.
                defines.push_str(&format!("#define {} ({})\n", var_name, val));
            } else {
                // Fallback: just include it? If it's a statement not matching, it might be valid code?
                // But usually our mappings are strictly "type name = val".
                // If we fail to macro-ize it, we inject it as is.
                defines.push_str(s);
                defines.push_str(";\n");
            }
        }

        // 2. Strip "uniform type name;" from user_code because Naga requires bindings for uniforms.
        // We replace them with comments.
        // Manual line-based processing is more robust than regex for this specific case, avoiding potential multiline/regex engine quirks.
        let stripped_user_code = user_code
            .lines()
            .map(|line| {
                let ops = line.trim_start();
                if ops.starts_with("uniform ") {
                    format!("// {}", line)
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        let full_glsl = format!(
            "{}\n{}\n{}\nvoid main() {{ o_color = transition(v_uv); }}",
            GLSL_PRELUDE, defines, stripped_user_code
        );

        // Log the generated shader for debugging purposes
        tracing::debug!("Compiling GLSL shader '{}'", name);

        let mut parser = naga::front::glsl::Frontend::default();
        let module = parser
            .parse(
                &naga::front::glsl::Options {
                    stage: naga::ShaderStage::Fragment,
                    defines: naga::FastHashMap::default(),
                },
                &full_glsl,
            )
            .map_err(|e| {
                tracing::error!(
                    "GLSL Parse Error in {}: {:?}\nSource:\n{}",
                    name,
                    e,
                    full_glsl
                );
                anyhow::anyhow!("GLSL Parse Error in {}: {:?}", name, e)
            })?;

        let info = naga::valid::Validator::new(
            naga::valid::ValidationFlags::all(),
            naga::valid::Capabilities::all(),
        )
        .validate(&module)
        .map_err(|e| anyhow::anyhow!("Shader Validation Error in {}: {:?}", name, e))?;

        let mut out = String::new();
        let mut writer =
            naga::back::wgsl::Writer::new(&mut out, naga::back::wgsl::WriterFlags::empty());
        writer
            .write(&module, &info)
            .map_err(|e| anyhow::anyhow!("WGSL Generation Error in {}: {:?}", name, e))?;

        Ok(out)
    }

    #[allow(dead_code)]
    pub fn get_shader(transition: &Transition) -> anyhow::Result<String> {
        match transition {
            Transition::Custom { shader, params } => {
                let glsl = Self::load_external_glsl(shader)?;
                let mut mapping = String::new();
                for (name, val) in params {
                    mapping.push_str(&format!("float {} = {}; ", name, val));
                }
                Self::compile_glsl(shader, &glsl, &mapping)
            }
            Transition::Random => {
                let picked = Self::pick_random_transition();
                Self::get_builtin_shader(&picked)
            }
            _ => Self::get_builtin_shader(transition),
        }
    }

    #[allow(dead_code)]
    pub async fn load_external_glsl_async(name: &str) -> anyhow::Result<String> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| anyhow::anyhow!("Failed to get config directory"))?
            .join("kaleidux")
            .join("shaders");

        // Try .glsl then .wgsl (though compile_glsl expects glsl)
        let path = config_dir.join(format!("{}.glsl", name));
        if tokio::fs::metadata(&path).await.is_ok() {
            return tokio::fs::read_to_string(path)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read shader: {}", e));
        }

        anyhow::bail!("Shader not found in ~/.config/kaleidux/shaders/: {}", name)
    }

    #[allow(dead_code)]
    pub fn load_external_glsl(name: &str) -> anyhow::Result<String> {
        // Use block_in_place to call async version from sync context
        tokio::task::block_in_place(|| -> anyhow::Result<String> {
            if let Ok(handle) = tokio::runtime::Handle::try_current()
                && let Ok(result) = handle.block_on(Self::load_external_glsl_async(name))
            {
                return Ok(result);
            }
            // Fallback to sync if no runtime available
            let config_dir = dirs::config_dir()
                .ok_or_else(|| anyhow::anyhow!("Failed to get config directory"))?
                .join("kaleidux")
                .join("shaders");
            let path = config_dir.join(format!("{}.glsl", name));
            if path.exists() {
                std::fs::read_to_string(path)
                    .map_err(|e| anyhow::anyhow!("Failed to read shader: {}", e))
            } else {
                Err(anyhow::anyhow!(
                    "Shader not found in ~/.config/kaleidux/shaders/: {}",
                    name
                ))
            }
        })
    }

    pub fn get_builtin_shader(transition: &Transition) -> anyhow::Result<String> {
        let name = transition.name();
        let glsl = Self::builtin_shader_source(transition, &name)?;
        let mapping = Self::builtin_shader_mapping(transition);
        let fingerprint = Self::builtin_shader_cache_fingerprint(&name, glsl, mapping);

        // Check process-wide cache first (P-21)
        if let Some(cached) = WGSL_CACHE.lock().get(&name)
            && cached.fingerprint == fingerprint
        {
            return Ok(cached.wgsl.clone());
        }

        // Note: We use getFromParams(i) which handles the aligned vec4 array access
        // We must map Rust struct fields to the EXACT uniform names used in the GLSL shaders.
        let wgsl = Self::compile_glsl(&name, glsl, mapping)?;

        // Store in process-wide cache (P-21)
        WGSL_CACHE.lock().insert(
            name,
            CachedWgslEntry {
                fingerprint,
                wgsl: wgsl.clone(),
            },
        );
        Ok(wgsl)
    }

    fn cache_entry_matches_transition(transition: &Transition, entry: &CachedWgslEntry) -> bool {
        match Self::builtin_shader_cache_fingerprint_for_transition(transition) {
            Ok(fingerprint) => entry.fingerprint == fingerprint,
            Err(_) => false,
        }
    }

    fn builtin_shader_cache_fingerprint_for_transition(
        transition: &Transition,
    ) -> anyhow::Result<u64> {
        let name = transition.name();
        let glsl = Self::builtin_shader_source(transition, &name)?;
        let mapping = Self::builtin_shader_mapping(transition);
        Ok(Self::builtin_shader_cache_fingerprint(&name, glsl, mapping))
    }

    fn builtin_shader_cache_fingerprint(name: &str, glsl: &str, mapping: &str) -> u64 {
        stable_shader_fingerprint(&[name, GLSL_PRELUDE, glsl, mapping])
    }

    fn builtin_shader_source(transition: &Transition, name: &str) -> anyhow::Result<&'static str> {
        match transition {
            Transition::Cube { .. } => Ok(CUBE_SAFE_GLSL),
            Transition::Displacement => Ok(DISPLACEMENT_SAFE_GLSL),
            Transition::Luma => Self::get_builtin_glsl("fade")
                .ok_or_else(|| anyhow::anyhow!("Failed to find fallback shader 'fade'")),
            _ => Self::get_builtin_glsl(name)
                .ok_or_else(|| anyhow::anyhow!("Builtin shader not found: {}", name)),
        }
    }

    fn builtin_shader_mapping(transition: &Transition) -> &'static str {
        builtin_mapping::builtin_shader_mapping(transition)
    }

    pub fn get_builtin_glsl(name: &str) -> Option<&'static str> {
        builtin_sources::get_builtin_glsl(name)
    }
}

#[cfg(test)]
mod tests;
