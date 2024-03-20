#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::compact::CompactionTask::ForceFullCompaction;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter<I>(
        &self,
        mut iter: I,
        _compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>>
    where
        for<'a> I: StorageIterator<KeyType<'a> = KeySlice<'a>>,
    {
        let mut builder: Option<SsTableBuilder> = None;
        let mut sstables = vec![];

        let mut prev_key: Vec<u8> = vec![];
        while iter.is_valid() {
            let same_as_last_key = iter.key().key_ref() == prev_key;

            if let Some(builder_inner) = builder.as_mut() {
                if builder_inner.estimated_size() >= self.options.target_sst_size
                    && !same_as_last_key
                {
                    let old_builder = builder.take().unwrap();
                    let sst_id = self.next_sst_id();
                    sstables.push(Arc::new(old_builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?));
                    builder = Some(SsTableBuilder::new(self.options.block_size));
                }
            } else {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }

            let builder_inner = builder.as_mut().unwrap();
            builder_inner.add(iter.key(), iter.value());

            if !same_as_last_key {
                prev_key.clear();
                prev_key.extend(iter.key().key_ref().iter());
            }

            iter.next()?;
        }
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id();
            sstables.push(Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?));
        }
        Ok(sstables)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match _task {
            ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = vec![];
                for id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables[id].clone(),
                    )?));
                }
                let mut l1_sst = vec![];
                for id in l1_sstables.iter() {
                    l1_sst.push(snapshot.sstables[id].clone());
                }

                self.compact_generate_sst_from_iter(
                    TwoMergeIterator::create(
                        MergeIterator::create(l0_iters),
                        SstConcatIterator::create_and_seek_to_first(l1_sst)?,
                    )?,
                    _task.compact_to_bottom_level(),
                )
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _lower_level,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _lower_level,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                Some(_upper_level) => {
                    // upper iter
                    let mut upper_sst = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids {
                        upper_sst.push(snapshot.sstables[id].clone());
                    }
                    // lower iter
                    let mut lower_sst = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids {
                        lower_sst.push(snapshot.sstables[id].clone());
                    }

                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(
                            SstConcatIterator::create_and_seek_to_first(upper_sst)?,
                            SstConcatIterator::create_and_seek_to_first(lower_sst)?,
                        )?,
                        _task.compact_to_bottom_level(),
                    )
                }
                _ => {
                    // l0 iter
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables[id].clone(),
                        )?));
                    }
                    // l1 iter
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids {
                        lower_ssts.push(snapshot.sstables[id].clone());
                    }

                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(
                            MergeIterator::create(upper_iters),
                            SstConcatIterator::create_and_seek_to_first(lower_ssts)?,
                        )?,
                        _task.compact_to_bottom_level(),
                    )
                }
            },
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let mut tier_iters = Vec::with_capacity(tiers.len());
                for (_, tier) in tiers {
                    let mut tier_ssts = Vec::with_capacity(tier.len());
                    for id in tier {
                        tier_ssts.push(snapshot.sstables[id].clone());
                    }
                    tier_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        tier_ssts,
                    )?));
                }
                self.compact_generate_sst_from_iter(
                    MergeIterator::create(tier_iters),
                    *bottom_tier_included,
                )
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let compaction_task = ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let sstables = self.compact(&compaction_task)?;

        let mut ids = vec![];
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            // rm old sst
            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                state.sstables.remove(id).expect("old sst not exist");
            }
            // add new sst
            for sst in sstables {
                ids.push(sst.sst_id());
                let result = state.sstables.insert(sst.sst_id(), sst);
                assert!(result.is_none(), "insert not update");
            }
            // rm old l0
            let mut old_id_set = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables.retain(|id| !old_id_set.remove(id));
            // update old l1
            state.levels[0].1 = ids.clone();

            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &_state_lock,
                ManifestRecord::Compaction(compaction_task, ids.clone()),
            )?;
        }
        // rm files
        for id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };

        let new_sst = self.compact(&task)?;
        let output = new_sst
            .iter()
            .map(|table| table.sst_id())
            .collect::<Vec<_>>();

        let rm_files = {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for table in new_sst.iter() {
                let result = snapshot.sstables.insert(table.sst_id(), table.clone());
                assert!(result.is_none());
            }
            let (mut snapshot, rm_files) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output);
            for id in rm_files.iter() {
                snapshot.sstables.remove(id).unwrap();
            }

            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            self.sync_dir()?;
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&_state_lock, ManifestRecord::Compaction(task, output))?;

            rm_files
        };
        // rm files
        for id in rm_files.iter() {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let guard = self.state.read();
            guard.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
