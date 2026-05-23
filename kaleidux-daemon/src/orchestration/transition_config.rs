use anyhow::Result;
use serde::{Deserialize, Deserializer};

pub(super) fn deserialize_optional_transition<'de, D>(
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

    if canonical == "Fade" && normalized != "fade" {
        None
    } else {
        Some(canonical)
    }
}
