use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize, Default, PartialEq)]
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

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PerformanceProfile {
    Quality,
    #[default]
    Balanced,
    LowPower,
    Debug,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum VideoFpsProfile {
    #[default]
    Low,
    Medium,
    High,
    Unlimited,
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
    #[serde(default)]
    pub performance: PerformanceProfile,
    pub video_fps: VideoFpsProfile,
    #[serde(default)]
    pub frame_latency: Option<u32>,
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
    #[serde(skip)]
    regex_outputs: Vec<RegexOutputOverride>,
}

#[derive(Debug, Clone)]
struct RegexOutputOverride {
    regex: Regex,
    config: PartialOutputConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct GlobalConfig {
    #[serde(default)]
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
    pub performance: Option<PerformanceProfile>,
    pub video_fps: Option<VideoFpsProfile>,
    pub frame_latency: Option<u32>,
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
    #[serde(
        default,
        deserialize_with = "transition_config::deserialize_optional_transition"
    )]
    pub transition: Option<crate::shaders::Transition>,
    pub transition_time: Option<u32>,
    pub volume: Option<u8>,
    pub sorting: Option<SortingStrategy>,
    pub layer: Option<Layer>,
    pub default_playlist: Option<String>,
    pub performance: Option<PerformanceProfile>,
    pub video_fps: Option<VideoFpsProfile>,
    pub frame_latency: Option<u32>,
}

impl Config {
    fn compile_regex_outputs(
        outputs: &HashMap<String, PartialOutputConfig>,
    ) -> Vec<RegexOutputOverride> {
        let mut regex_outputs: Vec<_> = outputs
            .iter()
            .filter_map(|(key, config)| {
                key.strip_prefix("re:")
                    .map(|pattern| (key.clone(), pattern.to_string(), config.clone()))
            })
            .collect();
        regex_outputs.sort_by(|left, right| left.0.cmp(&right.0));

        regex_outputs
            .into_iter()
            .filter_map(|(key, pattern, config)| match Regex::new(&pattern) {
                Ok(regex) => Some(RegexOutputOverride { regex, config }),
                Err(e) => {
                    tracing::warn!(
                        "[CONFIG] Ignoring invalid regex output override [{}]: {}",
                        key,
                        e
                    );
                    None
                }
            })
            .collect()
    }

    fn matching_regex_output(&self, description: &str) -> Option<PartialOutputConfig> {
        if let Some(entry) = self
            .regex_outputs
            .iter()
            .find(|entry| entry.regex.is_match(description))
        {
            return Some(entry.config.clone());
        }

        if self.regex_outputs.is_empty() {
            return Self::compile_regex_outputs(&self.outputs)
                .into_iter()
                .find(|entry| entry.regex.is_match(description))
                .map(|entry| entry.config);
        }

        None
    }

    pub(crate) fn from_parts(
        global: GlobalConfig,
        any: PartialOutputConfig,
        outputs: HashMap<String, PartialOutputConfig>,
    ) -> Self {
        let regex_outputs = Self::compile_regex_outputs(&outputs);
        Self {
            global,
            any,
            outputs,
            regex_outputs,
        }
    }

    pub(crate) fn parse_str(content: &str) -> Result<Self> {
        let table: toml::Table =
            toml::from_str(content).with_context(|| "Failed to parse config TOML")?;

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

        let mut outputs = HashMap::new();
        let mut config_errors = Vec::new();
        for (key, value) in &table {
            if key != "global" && key != "any" {
                match value.clone().try_into::<PartialOutputConfig>() {
                    Ok(cfg) => {
                        outputs.insert(key.clone(), cfg);
                    }
                    Err(e) => {
                        let error_msg =
                            format!("Failed to parse output config for [{}]: {}", key, e);
                        tracing::error!("{}", error_msg);
                        config_errors.push(error_msg);
                    }
                }
            }
        }

        if !config_errors.is_empty() {
            tracing::warn!(
                "{} output configuration section(s) had errors and were skipped",
                config_errors.len()
            );
        }

        tracing::info!("Loaded config with {} output overrides", outputs.len());
        Ok(Self::from_parts(global, any, outputs))
    }

    pub async fn load() -> Result<Self> {
        let config_dir = dirs::config_dir()
            .context("Failed to get config directory")?
            .join("kaleidux");
        let config_path = config_dir.join("config.toml");

        if !config_path.exists() {
            tracing::warn!("No config file found at {:?}, using defaults", config_path);
            return Ok(Self::default());
        }

        let content = tokio::fs::read_to_string(&config_path)
            .await
            .with_context(|| format!("Failed to read config file: {:?}", config_path))?;

        Self::parse_str(&content)
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
            sorting: self.global.sorting,
            layer: None,
            default_playlist: self.global.default_playlist.clone(),
            performance: self.global.performance,
            video_fps: self.global.video_fps,
            frame_latency: self.global.frame_latency,
        };

        // 2. Merge [any] fallback
        final_config.merge(&self.any);

        // 3. Exact output names override regex matches.
        let matched = self
            .outputs
            .get(name)
            .cloned()
            .or_else(|| self.matching_regex_output(description));

        if let Some(output_val) = matched {
            final_config.merge(&output_val);
        }

        final_config.into_output_config()
    }
}

impl PartialOutputConfig {
    fn merge(&mut self, other: &Self) {
        if other.path.is_some() {
            self.path = other.path.clone();
        }
        if other.duration.is_some() {
            self.duration = other.duration;
        }
        if other.video_ratio.is_some() {
            self.video_ratio = other.video_ratio;
        }
        if other.transition.is_some() {
            self.transition = other.transition.clone();
        }
        if other.transition_time.is_some() {
            self.transition_time = other.transition_time;
        }
        if other.volume.is_some() {
            self.volume = other.volume;
        }
        if other.sorting.is_some() {
            self.sorting = other.sorting;
        }
        if other.layer.is_some() {
            self.layer = other.layer.clone();
        }
        if other.default_playlist.is_some() {
            self.default_playlist = other.default_playlist.clone();
        }
        if other.performance.is_some() {
            self.performance = other.performance;
        }
        if other.video_fps.is_some() {
            self.video_fps = other.video_fps;
        }
        if other.frame_latency.is_some() {
            self.frame_latency = other.frame_latency;
        }
    }

    fn into_output_config(self) -> OutputConfig {
        let performance = self.performance.unwrap_or_default();
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
            performance,
            video_fps: self.video_fps.unwrap_or(match performance {
                PerformanceProfile::LowPower => VideoFpsProfile::Low,
                PerformanceProfile::Quality => VideoFpsProfile::High,
                PerformanceProfile::Balanced | PerformanceProfile::Debug => {
                    VideoFpsProfile::Unlimited
                }
            }),
            frame_latency: self.frame_latency.or(match performance {
                PerformanceProfile::LowPower => Some(1),
                PerformanceProfile::Quality => Some(2),
                PerformanceProfile::Balanced | PerformanceProfile::Debug => None,
            }),
        }
    }
}

mod transition_config;

#[cfg(test)]
mod tests;
