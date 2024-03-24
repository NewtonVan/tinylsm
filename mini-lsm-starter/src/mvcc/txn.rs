#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::WriteBatchRecord;
use crate::mem_table::map_bound;
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::StorageIterator,
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            bail!("committed, can't get");
        }
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let (_, read_set) = &mut *guard;
            read_set.insert(farmhash::hash32(key));
        }
        let entry = self.local_storage.get(key);
        if let Some(entry) = entry {
            if entry.value().is_empty() {
                Ok(None)
            } else {
                Ok(Some(Bytes::copy_from_slice(entry.value())))
            }
        } else {
            self.inner.get_with_ts(key, self.read_ts)
        }
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            bail!("committed, can't scan");
        }
        // prepare like mem_table::scan
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let entry = local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_iter.with_mut(|x| *x.item = entry);

        self.inner
            .scan_with_ts(lower, upper, self.read_ts)
            .and_then(|iter| {
                TxnIterator::create(self.clone(), TwoMergeIterator::create(local_iter, iter)?)
            })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            println!("committed, can't insert");
            return;
        }
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let (write_set, _) = &mut *guard;
            write_set.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            println!("committed, can't delete");
            return;
        }
        if let Some(key_hashes) = &self.key_hashes {
            let mut guard = key_hashes.lock();
            let (write_set, _) = &mut *guard;
            write_set.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::from_static(b""));
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");
        let commit_lock = self.inner.mvcc().commit_lock.lock();

        let check_conflict = if let Some(key_hashes) = self.key_hashes.as_ref() {
            let guard = key_hashes.lock();
            let (write_set, read_set) = &*guard;
            println!(
                "commit txn: write_set: {:?}, read_set: {:?}",
                write_set, read_set
            );

            let conflict = if write_set.is_empty() {
                false
            } else {
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                committed_txns
                    .range((self.read_ts + 1)..)
                    .any(|(_, committed_data)| {
                        committed_data.key_hashes.intersection(read_set).count() > 0
                    })
            };

            if conflict {
                bail!(format!("current tx(ts: {}) meet conflict", self.read_ts));
            }
            true
        } else {
            false
        };

        let local_records = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        let mvcc_ts = self.inner.write_batch_inner(&local_records)?;

        if check_conflict {
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            let mut key_hashes = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, _) = &mut *key_hashes;

            let old_data = committed_txns.insert(
                mvcc_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts: mvcc_ts,
                },
            );
            assert!(old_data.is_none());

            let watermark = self.inner.mvcc().watermark();
            // gc
            committed_txns.retain(|ts, _| *ts >= watermark);
            if !committed_txns.is_empty() {
                assert!(*committed_txns.first_key_value().unwrap().0 >= watermark);
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| Self::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut result = Self { _txn: txn, iter };
        result.skip_deletes()?;
        result.add_to_read_set_if_valid();
        Ok(result)
    }

    fn skip_deletes(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.next()?;
        }
        Ok(())
    }

    fn add_to_read_set_if_valid(&self) {
        if !self.is_valid() {
            return;
        }
        if let Some(guard) = &self._txn.key_hashes {
            let mut key_hashes = guard.lock();
            let (_, read_set) = &mut *key_hashes;
            read_set.insert(farmhash::hash32(self.key()));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()?;
        self.add_to_read_set_if_valid();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
