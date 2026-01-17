use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use regex::Regex;
use anyhow::{Result, Context};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum MonitorBehavior {
    #[default]
    Independent,
    Synchronized,
    Grouped(Vec<Vec<String>>),
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SortingStrategy {
    #[default]
    Loveit,
    Random,
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct OutputConfig {
    pub path: Option<PathBuf>,
    #[serde(with = "humantime_serde", default = "default_duration")]
    pub duration: Duration,
    #[serde(default = "default_video_ratio")]
    pub video_ratio: u8,
    pub transition: crate::shaders::Transition,
    #[serde(default = "default_transition_time")]
    pub transition_time: u32,
    #[serde(default = "default_volume")]
    pub volume: u8,
    #[serde(default)]
    pub sorting: SortingStrategy,
    #[serde(default = "default_layer")]
    pub layer: Layer,
    pub default_playlist: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Layer {
    #[default]
    Background,
    Bottom,
    Top,
    Overlay,
}

impl From<Layer> for smithay_client_toolkit::shell::wlr_layer::Layer {
    fn from(l: Layer) -> Self {
        match l {
            Layer::Background => smithay_client_toolkit::shell::wlr_layer::Layer::Background,
            Layer::Bottom => smithay_client_toolkit::shell::wlr_layer::Layer::Bottom,
            Layer::Top => smithay_client_toolkit::shell::wlr_layer::Layer::Top,
            Layer::Overlay => smithay_client_toolkit::shell::wlr_layer::Layer::Overlay,
        }
    }
}

fn default_layer() -> Layer {
    Layer::Background
}

fn default_duration() -> Duration {
    Duration::from_secs(300)
}

fn default_video_ratio() -> u8 {
    50
}

fn default_transition_time() -> u32 {
    1000
}

fn default_volume() -> u8 {
    100
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub global: GlobalConfig,
    #[serde(default)]
    pub any: PartialOutputConfig,
    #[serde(flatten)]
    pub outputs: HashMap<String, PartialOutputConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct GlobalConfig {
    pub monitor_behavior: MonitorBehavior,
    #[serde(default)]
    pub _custom_transitions: bool,
    pub video_ratio: Option<u8>,
    pub transition_time: Option<u32>,
    pub volume: Option<u8>,
    pub script_path: Option<PathBuf>,
    pub sorting: Option<SortingStrategy>,
    /// How often to tick Rhai scripts (in seconds), default 1
    #[serde(default = "default_script_tick_interval")]
    pub script_tick_interval: u64,
    pub default_playlist: Option<String>,
}

fn default_script_tick_interval() -> u64 {
    1
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct PartialOutputConfig {
    pub path: Option<PathBuf>,
    #[serde(with = "humantime_serde", default)]
    pub duration: Option<Duration>,
    pub video_ratio: Option<u8>,
    pub transition: Option<crate::shaders::Transition>,
    pub transition_time: Option<u32>,
    pub volume: Option<u8>,
    pub sorting: Option<SortingStrategy>,
    pub layer: Option<Layer>,
    pub default_playlist: Option<String>,
}

impl Config {
    pub async fn load() -> Result<Self> {
        let config_dir = dirs::config_dir()
            .context("Failed to get config directory")?
            .join("kaleidux");
        let config_path = config_dir.join("config.toml");

        if !config_path.exists() {
            tracing::warn!("No config file found at {:?}, using defaults", config_path);
            return Ok(Self::default());
        }

        let content = tokio::fs::read_to_string(&config_path).await
            .with_context(|| format!("Failed to read config file: {:?}", config_path))?;
        
        // Parse as raw TOML table first to work around serde(flatten) issues
        let table: toml::Table = toml::from_str(&content)
            .with_context(|| "Failed to parse config TOML")?;
        
        // Extract reserved sections
        let global: GlobalConfig = if let Some(v) = table.get("global") {
            v.clone().try_into().unwrap_or_else(|e| {
                tracing::error!("Failed to parse [global] config section: {}", e);
                GlobalConfig::default()
            })
        } else {
            GlobalConfig::default()
        };
        
        let any: PartialOutputConfig = if let Some(v) = table.get("any") {
            v.clone().try_into().unwrap_or_else(|e| {
                tracing::error!("Failed to parse [any] config section: {}", e);
                PartialOutputConfig::default()
            })
        } else {
            PartialOutputConfig::default()
        };
        
        // Collect remaining sections as per-output configs
        let mut outputs = HashMap::new();
        let mut config_errors = Vec::new();
        for (key, value) in &table {
            if key != "global" && key != "any" {
                match value.clone().try_into::<PartialOutputConfig>() {
                    Ok(cfg) => {
                        outputs.insert(key.clone(), cfg);
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to parse output config for [{}]: {}", key, e);
                        tracing::error!("{}", error_msg);
                        config_errors.push(error_msg);
                    }
                }
            }
        }
        
        if !config_errors.is_empty() {
            tracing::warn!("{} output configuration section(s) had errors and were skipped", config_errors.len());
        }
        
        tracing::info!("Loaded config with {} output overrides", outputs.len());
        
        Ok(Config { global, any, outputs })
    }

    pub fn get_config_for_output(&self, name: &str, description: &str) -> OutputConfig {
        // 1. Start with global defaults
        let mut final_config = PartialOutputConfig {
            path: None,
            duration: None, 
            video_ratio: self.global.video_ratio,
            transition: None,
            transition_time: self.global.transition_time,
            volume: self.global.volume,
            sorting: self.global.sorting.clone(),
            layer: None,
            default_playlist: self.global.default_playlist.clone(),
        };

        // 2. Merge [any] fallback
        final_config.merge(&self.any);

        // 3. Match specific output
        let mut matched = None;
        for (key, val) in &self.outputs {
            if key.starts_with("re:") {
                if let Ok(re) = Regex::new(&key[3..]) {
                    if re.is_match(description) {
                        matched = Some(val);
                        break;
                    }
                }
            } else if key == name {
                matched = Some(val);
                break;
            }
        }

        if let Some(output_val) = matched {
            final_config.merge(output_val);
        }

        final_config.into_output_config()
    }
}

impl PartialOutputConfig {
    fn merge(&mut self, other: &Self) {
        if other.path.is_some() { self.path = other.path.clone(); }
        if other.duration.is_some() { self.duration = other.duration; }
        if other.video_ratio.is_some() { self.video_ratio = other.video_ratio; }
        if other.transition.is_some() { self.transition = other.transition.clone(); }
        if other.transition_time.is_some() { self.transition_time = other.transition_time; }
        if other.volume.is_some() { self.volume = other.volume; }
        if other.sorting.is_some() { self.sorting = other.sorting.clone(); }
        if other.layer.is_some() { self.layer = other.layer.clone(); }
        if other.default_playlist.is_some() { self.default_playlist = other.default_playlist.clone(); }
    }

    fn into_output_config(self) -> OutputConfig {
        OutputConfig {
            path: self.path,
            duration: self.duration.unwrap_or_else(default_duration),
            video_ratio: self.video_ratio.unwrap_or(50),
            transition: self.transition.unwrap_or(crate::shaders::Transition::Fade),
            transition_time: self.transition_time.unwrap_or(1000),
            volume: self.volume.unwrap_or(100),
            sorting: self.sorting.unwrap_or_default(),
            layer: self.layer.unwrap_or_default(),
            default_playlist: self.default_playlist,
        }
    }
}
