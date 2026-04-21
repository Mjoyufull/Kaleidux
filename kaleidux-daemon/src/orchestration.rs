use anyhow::{Context, Result};
use regex::Regex;
use serde::{Deserialize, Deserializer};
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

fn deserialize_optional_transition<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<crate::shaders::Transition>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<toml::Value>::deserialize(deserializer)?;
    value
        .map(parse_transition_value)
        .transpose()
        .map_err(serde::de::Error::custom)
}

fn parse_transition_value(value: toml::Value) -> Result<crate::shaders::Transition> {
    if let Ok(transition) = value.clone().try_into::<crate::shaders::Transition>() {
        return Ok(transition);
    }

    match value {
        toml::Value::String(name) => {
            let canonical = canonical_transition_tag(&name)
                .ok_or_else(|| anyhow::anyhow!("unknown transition type `{}`", name))?;
            let mut table = toml::map::Map::new();
            table.insert("type".to_string(), toml::Value::String(canonical));
            toml::Value::Table(table)
                .try_into()
                .map_err(|e| anyhow::anyhow!("failed to parse transition `{}`: {}", name, e))
        }
        toml::Value::Table(mut table) => {
            if let Some(raw_type) = table.remove("type") {
                let type_name = raw_type.as_str().ok_or_else(|| {
                    anyhow::anyhow!(
                        "transition.type must be a string, got {}",
                        raw_type.type_str()
                    )
                })?;
                let canonical = canonical_transition_tag(type_name)
                    .ok_or_else(|| anyhow::anyhow!("unknown transition type `{}`", type_name))?;
                table.insert("type".to_string(), toml::Value::String(canonical));
                toml::Value::Table(table).try_into().map_err(|e| {
                    anyhow::anyhow!("failed to parse transition `{}`: {}", type_name, e)
                })
            } else if table.len() == 1 {
                let (legacy_name, legacy_value) = table.into_iter().next().unwrap();
                let canonical = canonical_transition_tag(&legacy_name)
                    .ok_or_else(|| anyhow::anyhow!("unknown transition type `{}`", legacy_name))?;
                let mut canonical_table = match legacy_value {
                    toml::Value::Table(inner) => inner,
                    other => {
                        return Err(anyhow::anyhow!(
                            "legacy transition `{}` must contain a table of params, got {}",
                            legacy_name,
                            other.type_str()
                        ));
                    }
                };
                canonical_table.insert("type".to_string(), toml::Value::String(canonical));
                toml::Value::Table(canonical_table).try_into().map_err(|e| {
                    anyhow::anyhow!("failed to parse legacy transition `{}`: {}", legacy_name, e)
                })
            } else {
                Err(anyhow::anyhow!(
                    "transition table must use `type = ...` or legacy `{{ name = {{ ... }} }}` syntax"
                ))
            }
        }
        other => Err(anyhow::anyhow!(
            "transition must be a string or table, got {}",
            other.type_str()
        )),
    }
}

fn canonical_transition_tag(name: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.eq_ignore_ascii_case("custom") {
        return Some("custom".to_string());
    }

    let mut direct = toml::map::Map::new();
    direct.insert("type".to_string(), toml::Value::String(trimmed.to_string()));
    if toml::Value::Table(direct)
        .try_into::<crate::shaders::Transition>()
        .is_ok()
    {
        return Some(trimmed.to_string());
    }

    let normalized = trimmed.to_ascii_lowercase().replace([' ', '_'], "-");
    if normalized == "fade" {
        return Some("Fade".to_string());
    }

    let transition = crate::shaders::Transition::from_name(trimmed);
    let canonical = serde_json::to_value(&transition)
        .ok()?
        .get("type")?
        .as_str()?
        .to_string();

    if canonical == "fade" && normalized != "fade" {
        None
    } else {
        Some(canonical)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct PartialOutputConfig {
    pub path: Option<PathBuf>,
    #[serde(with = "humantime_serde", default)]
    pub duration: Option<Duration>,
    pub video_ratio: Option<u8>,
    #[serde(default, deserialize_with = "deserialize_optional_transition")]
    pub transition: Option<crate::shaders::Transition>,
    pub transition_time: Option<u32>,
    pub volume: Option<u8>,
    pub sorting: Option<SortingStrategy>,
    pub layer: Option<Layer>,
    pub default_playlist: Option<String>,
}

impl Config {
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

        Ok(Config {
            global,
            any,
            outputs,
        })
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
        };

        // 2. Merge [any] fallback
        final_config.merge(&self.any);

        // 3. Exact output names override regex matches.
        let mut matched = self.outputs.get(name);
        if matched.is_none() {
            for (key, val) in &self.outputs {
                if let Some(stripped) = key.strip_prefix("re:") {
                    if let Ok(re) = Regex::new(stripped) {
                        if re.is_match(description) {
                            matched = Some(val);
                            break;
                        }
                    }
                }
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

#[cfg(test)]
mod tests {
    use super::{Config, MonitorBehavior, PartialOutputConfig, SortingStrategy};
    use crate::shaders::Transition;

    #[test]
    fn parses_legacy_nested_transition_tables() {
        let cfg: PartialOutputConfig = toml::from_str(
            r#"
            transition = { hexagonalize = { steps = 50, horizontal_hexagons = 20.0 } }
            "#,
        )
        .unwrap();

        assert_eq!(
            cfg.transition,
            Some(Transition::Hexagonalize {
                steps: 50,
                horizontal_hexagons: 20.0,
            })
        );
    }

    #[test]
    fn parses_simple_transition_strings() {
        let cfg: PartialOutputConfig = toml::from_str(
            r#"
            transition = "crosszoom"
            "#,
        )
        .unwrap();

        assert_eq!(
            cfg.transition,
            Some(Transition::CrossZoom { strength: 0.4 })
        );
    }

    #[test]
    fn parses_tagged_transition_aliases() {
        let cfg: PartialOutputConfig = toml::from_str(
            r#"
            transition = { type = "randomsquares", size = [8, 8], smoothness = 0.25 }
            "#,
        )
        .unwrap();

        assert_eq!(
            cfg.transition,
            Some(Transition::RandomSquares {
                size: [8, 8],
                smoothness: 0.25,
            })
        );
    }

    #[test]
    fn parses_grouped_monitor_behavior_from_global() {
        let cfg = Config::parse_str(
            r#"
            [global]
            monitor-behavior = { grouped = [["DP-2", "DP-3"], ["HDMI-A-1"]] }

            [any]
            path = "/tmp/walls"
            "#,
        )
        .unwrap();

        assert_eq!(
            cfg.global.monitor_behavior,
            MonitorBehavior::Grouped(vec![
                vec!["DP-2".to_string(), "DP-3".to_string()],
                vec!["HDMI-A-1".to_string()],
            ])
        );
    }

    #[test]
    fn merges_global_any_regex_and_specific_output_configs() {
        let cfg = Config::parse_str(
            r#"
            [global]
            monitor-behavior = "independent"
            volume = 10
            transition-time = 400
            sorting = "ascending"

            [any]
            path = "/tmp/any"
            duration = "45s"
            transition = "fade"

            ["re:Primary.*"]
            path = "/tmp/regex"
            volume = 20
            transition = "circleopen"

            [DP-2]
            path = "/tmp/specific"
            volume = 30
            transition = "crosszoom"
            sorting = "descending"
            "#,
        )
        .unwrap();

        let specific = cfg.get_config_for_output("DP-2", "Primary Display");
        assert_eq!(
            specific.path.unwrap(),
            std::path::PathBuf::from("/tmp/specific")
        );
        assert_eq!(specific.volume, 30);
        assert_eq!(specific.transition, Transition::CrossZoom { strength: 0.4 });
        assert_eq!(specific.transition_time, 400);
        assert_eq!(specific.sorting, SortingStrategy::Descending);

        let regex = cfg.get_config_for_output("HDMI-A-1", "Primary Display");
        assert_eq!(regex.path.unwrap(), std::path::PathBuf::from("/tmp/regex"));
        assert_eq!(regex.volume, 20);
        assert_eq!(
            regex.transition,
            Transition::CircleOpen {
                smoothness: 0.3,
                opening: true,
            }
        );
        assert_eq!(regex.transition_time, 400);
        assert_eq!(regex.sorting, SortingStrategy::Ascending);

        let fallback = cfg.get_config_for_output("DP-3", "Side Display");
        assert_eq!(fallback.path.unwrap(), std::path::PathBuf::from("/tmp/any"));
        assert_eq!(fallback.volume, 10);
        assert_eq!(fallback.transition, Transition::Fade);
        assert_eq!(fallback.transition_time, 400);
        assert_eq!(fallback.sorting, SortingStrategy::Ascending);
    }
}
