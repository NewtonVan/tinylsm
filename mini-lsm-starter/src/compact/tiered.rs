use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // space amplification ratio
        let size_before_last = snapshot.levels
            .iter()
            .take(snapshot.levels.len() - 1)
            .map(|(_, x)| x.len())
            .sum::<usize>();
        let size_last = snapshot.levels.last().unwrap().1.len();
        let space_amp_ratio = (size_before_last as f64 / size_last as f64) * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!("trigger compaction by space amplification ratio");
            return Some(
                TieredCompactionTask {
                    tiers: snapshot.levels.clone(),
                    bottom_tier_included: true,
                }
            );
        }

        // size ratio
        let size_ratio_threshold = 1.0 + self.options.size_ratio as f64 / 100.0;
        let mut prev_size = 0;
        for i in 1..snapshot.levels.len() {
            prev_size += snapshot.levels[i - 1].1.len();
            let size_ratio = prev_size as f64 / snapshot.levels[i].1.len() as f64;
            if size_ratio >= size_ratio_threshold {
                println!("trigger compaction by size ratio");
                return Some(
                    TieredCompactionTask {
                        tiers: snapshot.levels
                            .iter()
                            .take(i + 1)
                            .cloned()
                            .collect(),
                        bottom_tier_included: i == snapshot.levels.len() - 1,
                    }
                )
            }
        }

        // reduce sorted run
        println!("trigger compaction by reduce sorted run");
        let num_iters_to_take = snapshot.levels.len() - self.options.num_tiers + 2;
        return Some(
            TieredCompactionTask {
                tiers: snapshot.levels
                    .iter()
                    .take(num_iters_to_take)
                    .cloned()
                    .collect(),
                bottom_tier_included: snapshot.levels.len() == num_iters_to_take,
            }
        );
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();

        // only change levels
        let mut levels = vec![];
        let mut rm_files = vec![];
        let mut compacted_tier = _task.tiers
            .iter()
            .map(|(x, y)| (*x, y.clone()))
            .collect::<HashMap<_, _>>();
        let mut compact_sst_added = false;
        for (tier_id, tier) in snapshot.levels.iter() {
            if let Some(level) = compacted_tier.remove(tier_id) {
                rm_files.extend(level.iter().copied());
            } else {
                levels.push((*tier_id, tier.clone()));
            }
            if compacted_tier.is_empty() && !compact_sst_added {
                compact_sst_added = true;
                levels.push((_output[0], _output.to_vec()));
            }
        }
        snapshot.levels = levels;
        (snapshot, rm_files)
    }
}
