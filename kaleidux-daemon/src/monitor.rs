use sysinfo::{System, ProcessRefreshKind, ProcessesToUpdate};
use tracing::{info, debug, warn};
use std::time::Duration;
use tokio::time::interval;
use std::fs;
use std::process::Command;

pub struct SystemMonitor {
    sys: System,
    has_nvidia: bool,
    amd_gpu_path: Option<String>,
}

impl SystemMonitor {
    pub fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        
        // Detect GPU type
        let has_nvidia = fs::metadata("/proc/driver/nvidia/gpus").is_ok();
        
        let mut amd_gpu_path = None;
        // Check for common AMD/Intel paths
        for i in 0..3 {
            let path = format!("/sys/class/drm/card{}/device/gpu_busy_percent", i);
            if fs::metadata(&path).is_ok() {
                amd_gpu_path = Some(format!("/sys/class/drm/card{}/device", i));
                break;
            }
        }

        Self { sys, has_nvidia, amd_gpu_path }
    }

    fn get_gpu_stats(&self) -> (Option<f32>, Option<f32>, Option<f32>) {
        let mut gpu_usage = None;
        let mut vram_used = None;
        let mut vram_total = None;

        if self.has_nvidia {
            // Try nvidia-smi
            let output = Command::new("nvidia-smi")
                .args(["--query-gpu=utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"])
                .output();
            
            if let Ok(out) = output {
                let s = String::from_utf8_lossy(&out.stdout);
                let parts: Vec<&str> = s.split(',').map(|p| p.trim()).collect();
                if parts.len() >= 3 {
                    gpu_usage = parts[0].parse::<f32>().ok();
                    vram_used = parts[1].parse::<f32>().map(|m| m / 1024.0).ok(); // MB to GB
                    vram_total = parts[2].parse::<f32>().map(|m| m / 1024.0).ok();
                }
            }
        } else if let Some(base_path) = &self.amd_gpu_path {
            // Try AMD sysfs
            if let Ok(content) = fs::read_to_string(format!("{}/gpu_busy_percent", base_path)) {
                gpu_usage = content.trim().parse::<f32>().ok();
            }
            if let Ok(content) = fs::read_to_string(format!("{}/mem_info_vram_used", base_path)) {
                vram_used = content.trim().parse::<f32>().map(|b| b / 1024.0 / 1024.0 / 1024.0).ok(); // Bytes to GB
            }
            if let Ok(content) = fs::read_to_string(format!("{}/mem_info_vram_total", base_path)) {
                vram_total = content.trim().parse::<f32>().map(|b| b / 1024.0 / 1024.0 / 1024.0).ok();
            }
        }

        (gpu_usage, vram_used, vram_total)
    }

    pub async fn run(mut self) {
        let mut interval = interval(Duration::from_secs(10));
        
        info!("[MONITOR] Starting resource monitoring...");
        if self.has_nvidia {
            info!("[MONITOR] NVIDIA GPU detected.");
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
                    ProcessRefreshKind::nothing().with_cpu().with_memory()
                );
                if let Some(process) = self.sys.process(p) {
                    proc_cpu = process.cpu_usage();
                    proc_mem = process.memory() as f32 / 1024.0 / 1024.0; // KB to MB
                }
            }

            let (gpu_load, vram_used, vram_total) = self.get_gpu_stats();
            
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
