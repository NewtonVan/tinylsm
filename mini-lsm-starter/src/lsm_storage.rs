#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, map_key_bound_plus_ts, MemTable};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread.join().map_err(|e| anyhow!("{:?}", e))?;
        }
        self.compaction_notifier.send(()).ok();
        let mut compact_thread = self.compaction_thread.lock();
        if let Some(compact_thread) = compact_thread.take() {
            compact_thread.join().map_err(|e| anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        // create memtable and skip updating manifest
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_given_memtable(Arc::new(MemTable::create(self.inner.next_sst_id())))?;
        }

        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner
                .force_flush_next_imm_memtable()
                .context("fail to flush when close")?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

fn range_overlap(
    user_lower: Bound<&[u8]>,
    user_upper: Bound<&[u8]>,
    ss_lower: KeySlice,
    ss_upper: KeySlice,
) -> bool {
    match user_upper {
        Bound::Included(upper) => {
            if upper < ss_lower.key_ref() {
                return false;
            }
        }
        Bound::Excluded(upper) => {
            if upper <= ss_lower.key_ref() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }
    match user_lower {
        Bound::Included(lower) => {
            if lower > ss_upper.key_ref() {
                return false;
            }
        }
        Bound::Excluded(lower) => {
            if lower >= ss_upper.key_ref() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }
    true
}

fn key_within(user_key: &[u8], ss_lower: KeySlice, ss_upper: KeySlice) -> bool {
    // todo: consider ts?
    user_key >= ss_lower.key_ref() && user_key <= ss_upper.key_ref()
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    pub(crate) fn manifest(&self) -> &Manifest {
        self.manifest.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir(path)?;
        }
        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };
        let block_cache = Arc::new(BlockCache::new(1024));
        // manifest
        let manifest_path = path.join("MANIFEST");
        let fetch_sst = |id: usize| {
            let sst_path = Self::path_of_sst_static(path, id);
            if !sst_path.exists() {
                None
            } else {
                Some(Arc::new(
                    SsTable::open(
                        id,
                        Some(block_cache.clone()),
                        FileObject::open(&sst_path).unwrap(),
                    )
                    .unwrap(),
                ))
            }
        };

        let mut latest_sst_id = 0usize;
        let (manifest, records) = if manifest_path.exists() {
            // recover
            let (inner_manifest, records) = Manifest::recover(&manifest_path)?;
            (inner_manifest, Some(records))
        } else {
            // create
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            let inner_manifest =
                Manifest::create(manifest_path).context("failed to create manifest")?;
            inner_manifest
                .add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            (inner_manifest, None)
        };
        let manifest = Some(manifest);

        let mut max_ts = 0;
        if let Some(records) = records {
            let mut memtables = BTreeSet::new();
            for record in records.iter() {
                match record {
                    ManifestRecord::Flush(id) => {
                        let res = memtables.remove(id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            // In leveled compaction or no compaction, simply flush to L0
                            state.l0_sstables.insert(0, *id);
                        } else {
                            // In tiered compaction, create a new tier
                            state.levels.insert(0, (*id, vec![*id]));
                        }
                        if let Some(sst) = fetch_sst(*id) {
                            max_ts = max_ts.max(sst.max_ts());
                            state.sstables.insert(*id, sst);
                        }
                        latest_sst_id = latest_sst_id.max(*id);
                    }
                    ManifestRecord::NewMemtable(id) => {
                        memtables.insert(*id);
                        latest_sst_id = latest_sst_id.max(*id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        for id in output {
                            if let Some(sst) = fetch_sst(*id) {
                                max_ts = max_ts.max(sst.max_ts());
                                state.sstables.insert(*id, sst);
                            }
                        }
                        let (_state, _) =
                            compaction_controller.apply_compaction_result(&state, task, output);
                        state = _state;

                        latest_sst_id = latest_sst_id.max(output.iter().max().copied().unwrap());
                    }
                }
            }

            latest_sst_id += 1;
            state.memtable = if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))
                            .context(format!("fail to recover {:?}", *id))?;
                    if !memtable.is_empty() {
                        let mem_max_ts =
                            memtable.map.iter().map(|x| x.key().ts()).max().unwrap_or(0);
                        max_ts = max_ts.max(mem_max_ts);

                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }
                println!("{} WALs recovered", wal_cnt);
                Arc::new(MemTable::create_with_wal(
                    latest_sst_id,
                    Self::path_of_wal_static(path, latest_sst_id),
                )?)
            } else {
                Arc::new(MemTable::create(latest_sst_id))
            };
            manifest
                .as_ref()
                .unwrap()
                .add_record_when_init(ManifestRecord::NewMemtable(latest_sst_id))?;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(latest_sst_id + 1),
            compaction_controller,
            manifest,
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(max_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.get(key)
    }

    pub fn get_with_ts(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // mem iters
        let bound_begin = Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN));
        let bound_end = Bound::Included(KeySlice::from_slice(key, TS_RANGE_END));
        let mut mem_iters = Vec::with_capacity(1 + snapshot.imm_memtables.len());
        // iter in active memtable
        mem_iters.push(Box::new(snapshot.memtable.scan(bound_begin, bound_end)));
        // iter in immutable memtables
        for mem in snapshot.imm_memtables.iter() {
            mem_iters.push(Box::new(mem.scan(bound_begin, bound_end)));
        }
        let met_iter = MergeIterator::create(mem_iters);

        // sst iters
        let keep_table = |k: &[u8], table: &SsTable| {
            key_within(
                k,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) && table
                .bloom
                .as_ref()
                .map_or(true, |bloom| bloom.may_contain(farmhash::fingerprint32(k)))
        };

        // iter in l0
        let key_slice = KeySlice::from_slice(key, TS_RANGE_BEGIN);
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let sstable = snapshot.sstables[sst_id].clone();
            if keep_table(key, &sstable) {
                let iter = SsTableIterator::create_and_seek_to_key(sstable, key_slice)?;
                l0_iters.push(Box::new(iter));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);

        // iter in levels
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level) in snapshot.levels.iter() {
            let mut cur_level_ssts = Vec::with_capacity(level.len());
            for id in level {
                let sstable = snapshot.sstables[id].clone();
                if keep_table(key, &sstable) {
                    cur_level_ssts.push(sstable);
                }
            }
            level_iters.push(Box::new(SstConcatIterator::create_and_seek_to_key(
                cur_level_ssts,
                key_slice,
            )?));
        }
        let level_iter = MergeIterator::create(level_iters);

        // search the value
        let iter = LsmIterator::new(
            TwoMergeIterator::create(TwoMergeIterator::create(met_iter, l0_iter)?, level_iter)?,
            Bound::Unbounded,
            ts,
        )?;

        if iter.is_valid() && iter.key() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    pub fn write_batch_inner<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<u64> {
        let _mvcc_guard = self.mvcc().write_lock.lock();
        let mvcc_ts = self.mvcc().latest_commit_ts() + 1;
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key shouldn't be empty");
                    assert!(!value.is_empty(), "value shouldn't be empty");

                    let size;
                    {
                        let guard = self.state.read();
                        guard
                            .memtable
                            .put(KeySlice::from_slice(key, mvcc_ts), value)?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key shouldn't be empty");

                    let size;
                    {
                        let guard = self.state.read();
                        guard
                            .memtable
                            .put(KeySlice::from_slice(key, mvcc_ts), b"")?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        self.mvcc().update_commit_ts(mvcc_ts);

        Ok(mvcc_ts)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(batch)?;
        } else {
            let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
            for record in batch {
                match record {
                    WriteBatchRecord::Put(key, value) => {
                        txn.put(key.as_ref(), value.as_ref());
                    }
                    WriteBatchRecord::Del(key) => {
                        txn.delete(key.as_ref());
                    }
                }
            }
            txn.commit()?;
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(&[WriteBatchRecord::Put(key, value)])?;
        } else {
            let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
            txn.put(key, value);
            txn.commit()?;
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        if !self.options.serializable {
            self.write_batch_inner(&[WriteBatchRecord::Del(key)])?;
        } else {
            let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
            txn.delete(key);
            txn.commit()?;
        }

        Ok(())
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                // important need to drop guard first
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    fn freeze_given_memtable(&self, mem_table: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        // swap current mem table with given table
        let mut snapshot = guard.as_ref().clone();
        let old_mem_table = std::mem::replace(&mut snapshot.memtable, mem_table);
        // add frozen table into imm part
        snapshot.imm_memtables.insert(0, old_mem_table.clone());
        // update snapshot
        *guard = Arc::new(snapshot);

        drop(guard);
        old_mem_table.sync_wal()?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let mem_table_id = self.next_sst_id();
        let mem_table = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                mem_table_id,
                self.path_of_wal(mem_table_id),
            )?)
        } else {
            Arc::new(MemTable::create(mem_table_id))
        };

        self.freeze_given_memtable(mem_table)?;

        self.sync_dir()?;
        self.manifest().add_record(
            _state_lock_observer,
            ManifestRecord::NewMemtable(mem_table_id),
        )?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let flush_mem;
        {
            let guard = self.state.read();
            flush_mem = guard
                .imm_memtables
                .last()
                .expect("empty imm mem tables")
                .clone();
        }

        let mut builder = SsTableBuilder::new(self.options.block_size);
        flush_mem.flush(&mut builder)?;
        let sst_id = flush_mem.id();
        let table = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            // pop last imm tables
            let mem = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);
            // push sst l0 table
            if self.compaction_controller.flush_to_l0() {
                // In leveled compaction or no compaction, simply flush to L0
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // In tiered compaction, create a new tier
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            println!("flushed {}.sst with size={}", sst_id, table.table_size());
            snapshot.sstables.insert(sst_id, table);
            // update state
            *guard = Arc::new(snapshot);
            self.sync_dir()?;
            self.manifest()
                .add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        }

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.scan(lower, upper)
    }

    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // build iter on in mem tree
        let mut mem_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mem_iters.push(Box::new(snapshot.memtable.scan(
            map_key_bound_plus_ts(lower, TS_RANGE_BEGIN),
            map_key_bound_plus_ts(upper, TS_RANGE_END),
        )));
        for mem_table in snapshot.imm_memtables.iter() {
            mem_iters.push(Box::new(mem_table.scan(
                map_key_bound_plus_ts(lower, TS_RANGE_BEGIN),
                map_key_bound_plus_ts(upper, TS_RANGE_END),
            )));
        }
        let mut mem_iter = MergeIterator::create(mem_iters);

        if let Bound::Excluded(key) = lower {
            while mem_iter.is_valid() && mem_iter.key().key_ref() == key {
                mem_iter.next()?;
            }
        }

        // build iter on l0 sstable tree
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for it in snapshot.l0_sstables.iter() {
            let sstable = snapshot.sstables[it].clone();
            if range_overlap(
                lower,
                upper,
                sstable.first_key().as_key_slice(),
                sstable.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        sstable,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sstable,
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )?;
                        while iter.is_valid() && iter.key().key_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sstable)?,
                };
                l0_iters.push(Box::new(iter));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);

        // build iter on underlying sst
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level) in snapshot.levels.iter() {
            let mut ssts = Vec::with_capacity(level.len());
            for id in level {
                ssts.push(snapshot.sstables[id].clone());
            }
            let iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    ssts,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        ssts,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    while iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
            };
            level_iters.push(Box::new(iter));
        }
        let level_iter = MergeIterator::create(level_iters);

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(TwoMergeIterator::create(mem_iter, l0_iter)?, level_iter)?,
            map_bound(upper),
            ts,
        )?))
    }
}
