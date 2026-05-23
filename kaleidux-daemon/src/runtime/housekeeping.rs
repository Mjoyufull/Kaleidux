use crate::background;
use crate::main_loop::MainLoopContext;
use crate::renderer;
use crate::video;
use std::time::{Duration, Instant};
use tracing::{info, warn};

impl MainLoopContext {
    pub async fn housekeeping(&mut self, loop_start: Instant, was_idle: bool) {
        // Skip recording frame time for iterations that entered idle_wait (P-26)
        if !was_idle {
            let frame_time = loop_start.elapsed();
            self.metrics.record_frame_time(frame_time);
        }

        for renderer in self.renderers.values_mut() {
            renderer.trim_idle_retained_resources();
        }

        // Cleanup texture pool periodically (every 3 seconds)
        if self.last_pool_cleanup.elapsed().as_secs() >= 3 {
            if let Some(ctx) = &self.wgpu_ctx {
                ctx.cleanup_texture_pool(Some(&self.metrics));
            }
            self.last_pool_cleanup = Instant::now();
        }

        // Flush stats every 5 seconds (batched writes)
        if self.last_stats_flush.elapsed().as_secs() >= 5 {
            let _ = self.monitor_manager.flush_all_stats();
            self.last_stats_flush = Instant::now();
        }

        // Process directory watcher events and apply pool updates
        if self.last_dir_watch_poll.elapsed() >= Duration::from_millis(250) {
            if let Some(ref mut watcher) = self.dir_watcher {
                let pool_events = watcher.process_events().await;
                self.monitor_manager.apply_pool_events(pool_events);
            }
            self.last_dir_watch_poll = Instant::now();
        }

        // Log metrics summary every 10 seconds
        if self.last_metrics_log.elapsed().as_secs() >= 10 {
            let log_window = self.last_metrics_log.elapsed();
            let loop_counts = self.metrics.wayland_loop_counts();
            let idle_delta = loop_counts.0.saturating_sub(self.last_loop_rate_counts.0);
            let hot_delta = loop_counts.1.saturating_sub(self.last_loop_rate_counts.1);
            let loop_rate = idle_delta.saturating_add(hot_delta) as f64 / log_window.as_secs_f64();
            if loop_rate > 10.0 {
                warn!(
                    "[WAKE] high_loop_rate={:.1}/s idle_delta={} hot_delta={} window_ms={:.1}",
                    loop_rate,
                    idle_delta,
                    hot_delta,
                    log_window.as_secs_f64() * 1000.0
                );
            }
            self.last_loop_rate_counts = loop_counts;
            if let Some(ctx) = &self.wgpu_ctx {
                let (texture_count, texture_pool_bytes) = ctx.texture_pool_stats();
                let pipeline_count = ctx.transition_pipelines.lock().len()
                    + ctx.blit_pipelines.lock().len()
                    + ctx.mipmap_pipelines.lock().len();
                self.metrics.record_texture_count(texture_count);
                self.metrics.record_pipeline_count(pipeline_count);
                let active_video_players = self.video_players.len();
                let active_appsink_video_players = active_video_players;
                let pending_video_stops = self.pending_image_video_stops.len();
                let pending_video_switches = self.pending_video_switches.len();
                let latest_frame_slots = self.latest_video_frames.occupancy();
                let background_snapshot = background::snapshot();
                let appsink_queue_levels = video::AppsinkQueueLevels::default();
                let appsink_queue_players = 0usize;
                let mut retained = renderer::RetainedTextureFootprint::default();
                let mut per_renderer = Vec::new();
                let to_mb = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
                for (name, r) in &self.renderers {
                    let fp = r.retained_texture_footprint();
                    retained.current_bytes =
                        retained.current_bytes.saturating_add(fp.current_bytes);
                    retained.prev_bytes = retained.prev_bytes.saturating_add(fp.prev_bytes);
                    retained.composition_bytes = retained
                        .composition_bytes
                        .saturating_add(fp.composition_bytes);
                    retained.video_aux_bytes =
                        retained.video_aux_bytes.saturating_add(fp.video_aux_bytes);
                    per_renderer.push(format!(
                        "{}={:.1}MB(c={:.1} p={:.1} comp={:.1} aux={:.1})",
                        name,
                        to_mb(fp.total_bytes()),
                        to_mb(fp.current_bytes),
                        to_mb(fp.prev_bytes),
                        to_mb(fp.composition_bytes),
                        to_mb(fp.video_aux_bytes)
                    ));
                }
                info!(
                    "[MEMORY] Renderer retained textures: total={:.1}MB current={:.1}MB prev={:.1}MB composition={:.1}MB video_aux={:.1}MB pool={:.1}MB | video_players={} appsink={} pending_switches={} pending_stops={} latest_frame_slots={} appsink={}q/{}b/{:.1}ms@{}p | background={} | {}",
                    to_mb(retained.total_bytes()),
                    to_mb(retained.current_bytes),
                    to_mb(retained.prev_bytes),
                    to_mb(retained.composition_bytes),
                    to_mb(retained.video_aux_bytes),
                    to_mb(texture_pool_bytes),
                    active_video_players,
                    active_appsink_video_players,
                    pending_video_switches,
                    pending_video_stops,
                    latest_frame_slots,
                    appsink_queue_levels.buffers,
                    appsink_queue_levels.bytes,
                    appsink_queue_levels.time_ns as f64 / 1_000_000.0,
                    appsink_queue_players,
                    background_snapshot.format_compact(),
                    per_renderer.join(" | ")
                );
            }
            self.metrics.log_summary();
            self.last_metrics_log = Instant::now();
        }
    }
}
