//! Resolves command defaults and validates external media before changing queue state.

use super::MonitorManager;

impl MonitorManager {
    /// Resolve command defaults without relying on HashMap iteration order.
    pub(crate) fn command_output<'a>(
        &self,
        requested: Option<String>,
        outputs: impl Iterator<Item = (&'a String, u32, u32)>,
    ) -> anyhow::Result<Option<String>> {
        if requested.as_deref() == Some("all") {
            return Ok(None);
        }
        let configured = self
            .config
            .global
            .main_monitor
            .as_ref()
            .filter(|name| name.as_str() != "auto");
        if let Some(name) = requested {
            anyhow::ensure!(self.outputs.contains_key(&name), "Unknown output: {name}");
            return Ok(Some(name));
        }
        if let Some(name) = configured.filter(|name| self.outputs.contains_key(*name)) {
            return Ok(Some(name.clone()));
        }
        let selected = outputs.max_by(|a, b| {
            (u64::from(a.1) * u64::from(a.2))
                .cmp(&(u64::from(b.1) * u64::from(b.2)))
                .then_with(|| b.0.cmp(a.0))
        });
        Ok(selected.map(|(name, _, _)| name.clone()))
    }

    /// Validate every target before changing any queue. None selects jump/set by root.
    pub(crate) fn prepare_media_selection(
        &mut self,
        path: &str,
        output: &Option<String>,
        jump: Option<bool>,
    ) -> anyhow::Result<()> {
        let source = crate::media_source::MediaSource::parse(path)?;
        let path = match source {
            crate::media_source::MediaSource::File(local) => {
                let canonical = std::fs::canonicalize(local)?;
                anyhow::ensure!(canonical.is_file(), "Wallpaper path is not a file");
                canonical
            }
            virtual_source => virtual_source.locator(),
        };
        let content_type = Self::resolve_content_type(&path, "manual selection")
            .ok_or_else(|| anyhow::anyhow!("Path is not supported media"))?;
        if content_type == crate::queue::ContentType::Image
            && !crate::media_source::MediaSource::is_virtual(&path)
        {
            image::ImageReader::open(&path)?
                .with_guessed_format()?
                .into_dimensions()?;
        }
        if let Some(name) = output {
            anyhow::ensure!(self.outputs.contains_key(name), "Unknown output: {name}");
        }
        let targets = self.manual_queue_targets(output)?;
        self.validate_manual_queues(&targets, &path, jump)?;
        self.enqueue_manual_selection(targets, &path)
    }
}
