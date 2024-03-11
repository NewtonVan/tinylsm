use crate::key::KeyBytes;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn range_overlap(
        x_lower: &KeyBytes,
        x_upper: &KeyBytes,
        y_lower: &KeyBytes,
        y_upper: &KeyBytes,
    ) -> bool {
        if x_upper < y_lower {
            return false;
        }
        if x_lower > y_upper {
            return false;
        }
        true
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap().first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap().last_key())
            .max()
            .cloned()
            .unwrap();

        let mut overlap_ssts = vec![];
        for x in snapshot.levels[in_level - 1].1.iter() {
            let sst = &snapshot.sstables[x];
            let begin = sst.first_key();
            let end = sst.last_key();
            if Self::range_overlap(&begin_key, &end_key, begin, end) {
                overlap_ssts.push(*x);
            }
        }

        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // target size
        let mut target_sizes = vec![0; self.options.max_levels];
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        let real_sizes = snapshot
            .levels
            .iter()
            .take(self.options.max_levels)
            .map(|(_, level)| {
                level
                    .iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>() as usize
            })
            .collect::<Vec<_>>();

        target_sizes[self.options.max_levels - 1] =
            real_sizes[self.options.max_levels - 1].max(base_level_size_bytes);
        let mut base_level = self.options.max_levels;
        for i in (0..(self.options.max_levels - 1)).rev() {
            let next_level_size = target_sizes[i + 1];
            if next_level_size <= base_level_size_bytes {
                break;
            }
            target_sizes[i] = next_level_size / self.options.level_size_multiplier;
            if target_sizes[i] > 0 {
                base_level = i + 1;
            }
        }

        let (upper_level, lower_level) =
            if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
                (None, base_level)
            } else {
                let mut upper_level = None;
                let mut max_prior = 1.0;

                for upper_idx in (base_level - 1)..self.options.max_levels {
                    let prior = real_sizes[upper_idx] as f64 / target_sizes[upper_idx] as f64;
                    if prior > max_prior {
                        upper_level = Some(upper_idx + 1);
                        max_prior = prior;
                    }
                }

                if let Some(upper_level) = upper_level {
                    (Some(upper_level), upper_level + 1)
                } else {
                    // no need to compact
                    return None;
                }
            };

        // select sst
        let upper_level_sst_ids = if let Some(upper_level) = upper_level {
            // select the oldest
            vec![snapshot.levels[upper_level - 1]
                .1
                .iter()
                .min()
                .copied()
                .unwrap()]
        } else {
            snapshot.l0_sstables.clone()
        };
        let lower_level_sst_ids =
            self.find_overlapping_ssts(snapshot, upper_level_sst_ids.as_slice(), lower_level);

        Some(LeveledCompactionTask {
            upper_level,
            upper_level_sst_ids,
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level: lower_level == self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let mut rm_upper_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(upper) = task.upper_level {
            snapshot.levels[upper - 1].1 = snapshot.levels[upper - 1]
                .1
                .iter()
                .copied()
                .filter(|id| !rm_upper_set.remove(id))
                .collect();
        } else {
            snapshot.l0_sstables = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|id| !rm_upper_set.remove(id))
                .collect();
        }

        let mut rm_lower_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .copied()
            .filter(|id| !rm_lower_set.remove(id))
            .collect::<Vec<_>>();
        lower_ssts.extend(output);
        lower_ssts.sort_by(|x, y| {
            snapshot
                .sstables
                .get(x)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(y).unwrap().first_key())
        });
        snapshot.levels[task.lower_level - 1].1 = lower_ssts;

        let rm_files = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .copied()
            .collect();
        (snapshot, rm_files)
    }
}
