use std::fs;

#[derive(Debug, Clone)]
pub struct ThreadCpuSample {
    pub tid: u32,
    pub name: String,
    pub user_ticks: u64,
    pub system_ticks: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ThreadCpuSnapshot {
    pub samples: Vec<ThreadCpuSample>,
}

impl ThreadCpuSnapshot {
    pub fn collect_current() -> Self {
        let Ok(entries) = fs::read_dir("/proc/self/task") else {
            return Self::default();
        };

        let mut samples = Vec::new();
        for entry in entries.flatten() {
            let Some(tid) = entry
                .file_name()
                .to_str()
                .and_then(|value| value.parse::<u32>().ok())
            else {
                continue;
            };
            let Some(sample) = read_thread_sample(tid) else {
                continue;
            };
            samples.push(sample);
        }
        samples.sort_by_key(|sample| sample.tid);
        Self { samples }
    }

    pub fn format_top(&self, previous: Option<&Self>, max_threads: usize) -> String {
        let Some(previous) = previous else {
            return format!("threads={} baseline=pending", self.samples.len());
        };

        let mut deltas = self
            .samples
            .iter()
            .filter_map(|sample| {
                let previous_sample = previous.samples.iter().find(|old| old.tid == sample.tid)?;
                let user_delta = sample.user_ticks.saturating_sub(previous_sample.user_ticks);
                let system_delta = sample
                    .system_ticks
                    .saturating_sub(previous_sample.system_ticks);
                let total_delta = user_delta.saturating_add(system_delta);
                Some((total_delta, user_delta, system_delta, sample))
            })
            .collect::<Vec<_>>();
        deltas.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.3.tid.cmp(&right.3.tid))
        });

        let top = deltas
            .into_iter()
            .take(max_threads)
            .map(|(total, user, system, sample)| {
                format!(
                    "{}:{}t(u{}+s{})",
                    sample.name.replace(' ', "_"),
                    total,
                    user,
                    system
                )
            })
            .collect::<Vec<_>>()
            .join(",");

        format!("threads={} top=[{}]", self.samples.len(), top)
    }
}

fn read_thread_sample(tid: u32) -> Option<ThreadCpuSample> {
    let stat = fs::read_to_string(format!("/proc/self/task/{tid}/stat")).ok()?;
    let name = fs::read_to_string(format!("/proc/self/task/{tid}/comm"))
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|_| format!("tid-{tid}"));
    let close_paren = stat.rfind(')')?;
    let fields = stat
        .get(close_paren + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse().ok()?;
    let system_ticks = fields.get(12)?.parse().ok()?;
    Some(ThreadCpuSample {
        tid,
        name,
        user_ticks,
        system_ticks,
    })
}
