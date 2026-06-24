use std::fs;
use std::time::Duration;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
use tracing::{debug, info, warn};

pub struct SystemMonitor {
    sys: System,
    nvml: Option<nvml_wrapper::Nvml>,
    amd_gpu_path: Option<String>,
    metrics: Option<std::sync::Arc<crate::metrics::PerformanceMetrics>>,
}

impl SystemMonitor {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::new_with_metrics(None)
    }

    pub fn new_with_metrics(
        metrics: Option<std::sync::Arc<crate::metrics::PerformanceMetrics>>,
    ) -> Self {
        let mut sys = System::new();
        sys.refresh_cpu_all();
        sys.refresh_memory();

        // Initialize NVML for NVIDIA GPU monitoring (loads libnvidia-ml.so at runtime)
        let nvml = match nvml_wrapper::Nvml::init() {
            Ok(n) => {
                info!("[MONITOR] NVML initialized successfully");
                Some(n)
            }
            Err(e) => {
                debug!("[MONITOR] NVML not available ({}), will try sysfs", e);
                None
            }
        };

        let mut amd_gpu_path = None;
        // Check for AMD/Intel sysfs paths (only if NVML not available)
        if nvml.is_none() {
            for i in 0..3 {
                let path = format!("/sys/class/drm/card{}/device/gpu_busy_percent", i);
                if fs::metadata(&path).is_ok() {
                    amd_gpu_path = Some(format!("/sys/class/drm/card{}/device", i));
                    break;
                }
            }
        }

        Self {
            sys,
            nvml,
            amd_gpu_path,
            metrics,
        }
    }

    fn get_gpu_stats(&self) -> (Option<f32>, Option<f32>, Option<f32>) {
        let mut gpu_usage = None;
        let mut vram_used = None;
        let mut vram_total = None;

        if let Some(nvml) = &self.nvml {
            // Use NVML library calls (sub-microsecond, no subprocess)
            if let Ok(device) = nvml.device_by_index(0) {
                gpu_usage = device.utilization_rates().ok().map(|u| u.gpu as f32);
                let mem = device.memory_info().ok();
                vram_used = mem
                    .as_ref()
                    .map(|m| m.used as f32 / 1024.0 / 1024.0 / 1024.0);
                vram_total = mem
                    .as_ref()
                    .map(|m| m.total as f32 / 1024.0 / 1024.0 / 1024.0);
            }
        } else if let Some(base_path) = &self.amd_gpu_path {
            // AMD/Intel sysfs fallback
            if let Ok(content) = fs::read_to_string(format!("{}/gpu_busy_percent", base_path)) {
                gpu_usage = content.trim().parse::<f32>().ok();
            }
            if let Ok(content) = fs::read_to_string(format!("{}/mem_info_vram_used", base_path)) {
                vram_used = content
                    .trim()
                    .parse::<f32>()
                    .map(|b| b / 1024.0 / 1024.0 / 1024.0)
                    .ok();
            }
            if let Ok(content) = fs::read_to_string(format!("{}/mem_info_vram_total", base_path)) {
                vram_total = content
                    .trim()
                    .parse::<f32>()
                    .map(|b| b / 1024.0 / 1024.0 / 1024.0)
                    .ok();
            }
        }

        (gpu_usage, vram_used, vram_total)
    }

    pub async fn run(mut self) {
        let interval_duration = Duration::from_secs(10);
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + interval_duration,
            interval_duration,
        );

        info!("[MONITOR] Starting resource monitoring...");
        if self.nvml.is_some() {
            info!("[MONITOR] NVIDIA GPU detected (NVML).");
        } else if self.amd_gpu_path.is_some() {
            info!("[MONITOR] AMD/Intel GPU detected (sysfs).");
        } else {
            warn!("[MONITOR] No supported GPU detected for monitoring.");
        }

        loop {
            interval.tick().await;

            self.sys.refresh_cpu_all();
            self.sys.refresh_memory();

            let load = self.sys.global_cpu_usage();
            let total_mem = self.sys.total_memory() as f32 / 1024.0 / 1024.0 / 1024.0;
            let used_mem = self.sys.used_memory() as f32 / 1024.0 / 1024.0 / 1024.0;

            // Get process-specific info
            let pid = sysinfo::get_current_pid().ok();
            let mut proc_cpu = 0.0;
            let mut proc_mem = 0.0;

            if let Some(p) = pid {
                self.sys.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[p]),
                    false,
                    ProcessRefreshKind::nothing().with_cpu().with_memory(),
                );
                if let Some(process) = self.sys.process(p) {
                    proc_cpu = process.cpu_usage();
                    proc_mem = process.memory() as f32 / 1024.0 / 1024.0; // Bytes to MB (assuming sysinfo returns bytes for process memory)
                    // Record memory usage in metrics
                    if let Some(m) = &self.metrics {
                        m.record_memory_usage(proc_mem as f64);
                    }
                }
            }

            let (gpu_load, vram_used, vram_total) = self.get_gpu_stats();

            // Record GPU utilization in metrics
            if let Some(gl) = gpu_load {
                if let Some(m) = &self.metrics {
                    m.record_gpu_utilization(gl as f64);
                }
            }

            let mut log_msg = format!(
                "[MONITOR] App: {:.1}% CPU, {:.1}MB | Sys: {:.1}% CPU, {:.2}GB / {:.2}GB",
                proc_cpu, proc_mem, load, used_mem, total_mem
            );

            if let Some(gl) = gpu_load {
                log_msg.push_str(&format!(" | GPU: {:.1}%", gl));
            }
            if let (Some(vu), Some(vt)) = (vram_used, vram_total) {
                log_msg.push_str(&format!(" | VRAM: {:.2}GB / {:.2}GB", vu, vt));
            }

            info!("{}", log_msg);

            // Log individual core spikes if high
            for (i, cpu) in self.sys.cpus().iter().enumerate() {
                let usage = cpu.cpu_usage();
                if usage > 90.0 {
                    debug!("[MONITOR] CORE spiking: Core {} at {:.1}%", i, usage);
                }
            }
        }
    }
}
