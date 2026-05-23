use super::{ContentType, SmartQueue};
use chrono::{DateTime, Utc};
use rand::Rng;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::path::PathBuf;

impl SmartQueue {
    #[inline]
    pub fn pick_next(&mut self) -> Option<PathBuf> {
        self.pick_next_excluding(&HashSet::new())
    }

    pub fn pick_next_excluding(&mut self, excluded: &HashSet<PathBuf>) -> Option<PathBuf> {
        if self.pool.is_empty() {
            return None;
        }

        let picked = match self.strategy {
            crate::orchestration::SortingStrategy::Loveit => self.pick_loveit_excluding(excluded),
            crate::orchestration::SortingStrategy::Random => self.pick_random_excluding(excluded),
            crate::orchestration::SortingStrategy::Ascending => {
                self.pick_sequential_excluding(false, excluded)
            }
            crate::orchestration::SortingStrategy::Descending => {
                self.pick_sequential_excluding(true, excluded)
            }
        };

        if let Some(ref p) = picked {
            self.record_pick(p);
        }

        picked
    }

    /// Get the next content path without consuming it (for pre-buffering)
    pub fn peek_next(&self) -> Option<(PathBuf, ContentType)> {
        // For sequential strategies, we can peek at the next index
        match self.strategy {
            crate::orchestration::SortingStrategy::Ascending
            | crate::orchestration::SortingStrategy::Descending => {
                let descending = matches!(
                    self.strategy,
                    crate::orchestration::SortingStrategy::Descending
                );
                let planned_type = self.planned_sequential_type.or_else(|| {
                    self.pool
                        .get(self.current_index)
                        .and_then(|path| self.cached_content_type(path))
                });
                if let Some(content_type) = planned_type {
                    if let Some(path) = self.peek_sequential_for_type(descending, content_type) {
                        return Some((path, content_type));
                    }
                    if let Some(path) =
                        self.peek_sequential_for_type(descending, content_type.other())
                    {
                        return Some((path, content_type.other()));
                    }
                }
            }
            _ => {
                // For Random/Loveit, can't predict next easily without pre-rolling
            }
        }
        None
    }

    /// Return a bounded list of upcoming image candidates without mutating queue state.
    ///
    /// For sequential queues this walks the future pool order directly, which is good
    /// enough for prefetch even when video/image ratio may insert videos between them.
    /// For Loveit, this returns the highest-weight image candidates under the current
    /// recency/loveit model. Random remains intentionally conservative.
    pub fn peek_upcoming_images(&self, limit: usize) -> Vec<PathBuf> {
        if limit == 0 || self.pool.is_empty() {
            return Vec::new();
        }

        match self.strategy {
            crate::orchestration::SortingStrategy::Ascending
            | crate::orchestration::SortingStrategy::Descending => {
                let descending = matches!(
                    self.strategy,
                    crate::orchestration::SortingStrategy::Descending
                );
                let mut idx = self.current_index.min(self.pool.len().saturating_sub(1));
                let mut seen = HashSet::new();
                let mut upcoming = Vec::with_capacity(limit.min(self.pool.len()));

                for _ in 0..self.pool.len() {
                    let candidate = &self.pool[idx];
                    if matches!(
                        self.cached_content_type(candidate),
                        Some(ContentType::Image)
                    ) && seen.insert(candidate.clone())
                    {
                        upcoming.push(candidate.clone());
                        if upcoming.len() >= limit {
                            break;
                        }
                    }
                    idx = self.advance_index(idx, descending);
                }

                upcoming
            }
            crate::orchestration::SortingStrategy::Loveit => self.peek_loveit_images(limit),
            crate::orchestration::SortingStrategy::Random => self
                .peek_next()
                .filter(|(_, content_type)| *content_type == ContentType::Image)
                .map(|(path, _)| vec![path])
                .unwrap_or_default(),
        }
    }

    #[inline]
    pub fn pick_prev(&mut self) -> Option<PathBuf> {
        if self.pool.is_empty() {
            return None;
        }

        let picked = match self.strategy {
            crate::orchestration::SortingStrategy::Ascending
            | crate::orchestration::SortingStrategy::Descending => {
                let descending = matches!(
                    self.strategy,
                    crate::orchestration::SortingStrategy::Descending
                );
                self.pick_sequential_raw(!descending)
            }
            _ => {
                // For non-sequential, use history
                if self.history.len() > 1 {
                    self.history.pop_back(); // Remove current
                    self.history.back().cloned()
                } else {
                    None
                }
            }
        };

        if let Some(ref p) = picked {
            self.update_stats(p);
        }

        picked
    }

    fn record_pick(&mut self, path: &PathBuf) {
        self.update_stats(path);
        if self.history.back() != Some(path) {
            self.history.push_back(path.clone());
            if self.history.len() > 50 {
                self.history.pop_front();
            }
        }
    }

    fn pick_random_excluding(&mut self, excluded: &HashSet<PathBuf>) -> Option<PathBuf> {
        let mut rng = rand::thread_rng();
        let is_video_cycle = self.choose_cycle_content_type(&mut rng) == ContentType::Video;

        let sub_pool: Vec<&PathBuf> = self
            .pool
            .iter()
            .filter(|p| {
                if excluded.contains(p.as_path()) {
                    return false;
                }
                let is_video = matches!(self.cached_content_type(p), Some(ContentType::Video));
                is_video == is_video_cycle
            })
            .collect();

        let active_pool = if sub_pool.is_empty() {
            self.pool
                .iter()
                .filter(|p| !excluded.contains(p.as_path()))
                .collect::<Vec<_>>()
        } else {
            sub_pool
        };

        if active_pool.is_empty() {
            return None;
        }

        let idx = rng.gen_range(0..active_pool.len());
        Some(active_pool[idx].clone())
    }

    fn pick_sequential_excluding(
        &mut self,
        descending: bool,
        excluded: &HashSet<PathBuf>,
    ) -> Option<PathBuf> {
        if self.pool.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();
        let requested_type = self
            .planned_sequential_type
            .take()
            .unwrap_or_else(|| self.choose_cycle_content_type(&mut rng));
        let picked = self
            .pick_sequential_for_type_excluding(descending, requested_type, excluded)
            .or_else(|| {
                self.pick_sequential_for_type_excluding(
                    descending,
                    requested_type.other(),
                    excluded,
                )
            })
            .or_else(|| self.pick_sequential_raw_excluding(descending, excluded));
        self.planned_sequential_type = Some(self.choose_cycle_content_type(&mut rng));
        picked
    }

    fn pick_loveit_excluding(&mut self, excluded: &HashSet<PathBuf>) -> Option<PathBuf> {
        let mut rng = rand::thread_rng();

        // 1. Filter by video_ratio probability
        let is_video_cycle = self.choose_cycle_content_type(&mut rng) == ContentType::Video;

        let sub_pool: Vec<&PathBuf> = self
            .pool
            .iter()
            .filter(|p| {
                if excluded.contains(p.as_path()) {
                    return false;
                }
                let content_type = self.cached_content_type(p);
                let is_video = matches!(content_type, Some(ContentType::Video));
                is_video == is_video_cycle
            })
            .collect();

        // Fallback if sub_pool is empty
        let active_pool = if sub_pool.is_empty() {
            self.pool
                .iter()
                .filter(|p| !excluded.contains(p.as_path()))
                .collect::<Vec<_>>()
        } else {
            sub_pool
        };

        if active_pool.is_empty() {
            return None;
        }

        // 2. Weighted Random Selection (Loveit + Recency)
        let mut weights = Vec::new();
        let now = Utc::now();

        for path in &active_pool {
            weights.push(self.loveit_weight_for_path(path, now));
        }

        let total_weight: f32 = weights.iter().sum();
        let mut choice = rng.gen_range(0.0..total_weight);

        for (i, weight) in weights.iter().enumerate() {
            choice -= weight;
            if choice <= 0.0 {
                return Some(active_pool[i].clone());
            }
        }

        Some(active_pool[0].clone())
    }

    fn loveit_weight_for_path(&self, path: &PathBuf, now: DateTime<Utc>) -> f32 {
        let stat = self.stats.files.peek(path).cloned().unwrap_or_default();

        let count_score = 100.0 / (stat.count as f32 + 1.0);
        let recency_factor = if let Some(last) = stat.last_seen {
            let hours_since = now.signed_duration_since(last).num_hours() as f32;
            (hours_since / 24.0).clamp(1.0, 10.0)
        } else {
            10.0
        };
        let love_weight = if stat.love_multiplier > 0.0 {
            stat.love_multiplier
        } else {
            1.0
        };

        count_score * recency_factor * love_weight
    }

    fn peek_loveit_images(&self, limit: usize) -> Vec<PathBuf> {
        let now = Utc::now();
        let mut weighted_images: Vec<(PathBuf, f32)> = self
            .pool
            .iter()
            .filter(|path| matches!(self.cached_content_type(path), Some(ContentType::Image)))
            .map(|path| (path.clone(), self.loveit_weight_for_path(path, now)))
            .collect();

        weighted_images.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
                .then(Ordering::Equal)
        });
        weighted_images.truncate(limit);
        weighted_images
            .into_iter()
            .map(|(path, _weight)| path)
            .collect()
    }

    fn choose_cycle_content_type<R: Rng + ?Sized>(&self, rng: &mut R) -> ContentType {
        if rng.gen_range(0..100) < self.video_ratio {
            ContentType::Video
        } else {
            ContentType::Image
        }
    }

    fn peek_sequential_for_type(
        &self,
        descending: bool,
        requested_type: ContentType,
    ) -> Option<PathBuf> {
        let idx = self.find_next_index_of_type(self.current_index, descending, requested_type)?;
        self.pool.get(idx).cloned()
    }

    fn pick_sequential_for_type_excluding(
        &mut self,
        descending: bool,
        requested_type: ContentType,
        excluded: &HashSet<PathBuf>,
    ) -> Option<PathBuf> {
        let idx = self.find_next_index_of_type_excluding(
            self.current_index,
            descending,
            requested_type,
            excluded,
        )?;
        let picked = self.pool.get(idx).cloned();
        self.current_index = self.advance_index(idx, descending);
        picked
    }

    fn pick_sequential_raw_excluding(
        &mut self,
        descending: bool,
        excluded: &HashSet<PathBuf>,
    ) -> Option<PathBuf> {
        if self.pool.is_empty() {
            return None;
        }

        let start_idx = self.current_index.min(self.pool.len().saturating_sub(1));
        let mut idx = start_idx;
        let mut picked = None;
        for _ in 0..self.pool.len() {
            let candidate = &self.pool[idx];
            if !excluded.contains(candidate.as_path()) {
                picked = Some(candidate.clone());
                break;
            }
            idx = self.advance_index(idx, descending);
        }
        let picked = picked?;
        self.current_index = self.advance_index(idx, descending);
        Some(picked)
    }

    fn pick_sequential_raw(&mut self, descending: bool) -> Option<PathBuf> {
        self.pick_sequential_raw_excluding(descending, &HashSet::new())
    }

    fn advance_index(&self, idx: usize, descending: bool) -> usize {
        if self.pool.is_empty() {
            return 0;
        }

        if descending {
            if idx == 0 {
                self.pool.len() - 1
            } else {
                idx - 1
            }
        } else {
            (idx + 1) % self.pool.len()
        }
    }

    fn find_next_index_of_type_excluding(
        &self,
        start_idx: usize,
        descending: bool,
        requested_type: ContentType,
        excluded: &HashSet<PathBuf>,
    ) -> Option<usize> {
        if self.pool.is_empty() {
            return None;
        }

        let pool_len = self.pool.len();
        let mut idx = start_idx.min(pool_len.saturating_sub(1));
        for _ in 0..pool_len {
            let path = &self.pool[idx];
            if !excluded.contains(path.as_path())
                && self.cached_content_type(path) == Some(requested_type)
            {
                return Some(idx);
            }
            idx = self.advance_index(idx, descending);
        }
        None
    }

    fn find_next_index_of_type(
        &self,
        start_idx: usize,
        descending: bool,
        requested_type: ContentType,
    ) -> Option<usize> {
        self.find_next_index_of_type_excluding(
            start_idx,
            descending,
            requested_type,
            &HashSet::new(),
        )
    }
}
