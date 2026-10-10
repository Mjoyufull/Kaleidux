use super::MonitorManager;
use crate::orchestration::MonitorBehavior;

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
        let path = std::fs::canonicalize(path)?;
        anyhow::ensure!(path.is_file(), "Wallpaper path is not a file");
        let content_type = Self::resolve_content_type(&path, "manual selection")
            .ok_or_else(|| anyhow::anyhow!("Path is not supported media"))?;
        if content_type == crate::queue::ContentType::Image {
            image::ImageReader::open(&path)?
                .with_guessed_format()?
                .into_dimensions()?;
        }
        if let Some(name) = output {
            anyhow::ensure!(self.outputs.contains_key(name), "Unknown output: {name}");
        }
        let mut queues = Vec::new();
        match &self.config.global.monitor_behavior {
            MonitorBehavior::Synchronized => {
                queues.push(
                    self.shared_queue
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("No slideshow queue"))?,
                );
            }
            MonitorBehavior::Independent | MonitorBehavior::Grouped(_) => {
                let grouped = matches!(
                    self.config.global.monitor_behavior,
                    MonitorBehavior::Grouped(_)
                );
                let mut groups = std::collections::HashSet::new();
                for (name, orch) in &mut self.outputs {
                    if output.as_ref().is_some_and(|target| target != name) {
                        continue;
                    }
                    if grouped && let Some(gid) = self.output_groups.get(name) {
                        groups.insert(*gid);
                    } else {
                        queues.push(
                            orch.queue
                                .as_mut()
                                .ok_or_else(|| anyhow::anyhow!("No slideshow queue for {name}"))?,
                        );
                    }
                }
                for (gid, queue) in &mut self.group_queues {
                    if groups.remove(gid) {
                        queues.push(queue);
                    }
                }
                anyhow::ensure!(groups.is_empty(), "Missing group slideshow queue");
            }
        }
        anyhow::ensure!(!queues.is_empty(), "No slideshow outputs");
        for queue in &queues {
            let in_root =
                std::fs::canonicalize(&queue.root_path).is_ok_and(|root| path.starts_with(root));
            anyhow::ensure!(
                jump != Some(true) || in_root,
                "Wallpaper is outside the slideshow directory"
            );
            anyhow::ensure!(
                !queue.stats.blacklist.iter().any(|blocked| blocked == &path
                    || std::fs::canonicalize(blocked).is_ok_and(|resolved| resolved == path)),
                "Wallpaper is blacklisted"
            );
        }
        for queue in queues {
            queue.enqueue_selected_image(path.clone());
        }
        Ok(())
    }
}
