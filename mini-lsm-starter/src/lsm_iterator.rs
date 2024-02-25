#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::Bound;
use anyhow::{bail, Result};
use bytes::Bytes;

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper_bound: Bound<Bytes>) -> Result<Self> {
        let mut ret = Self {
            is_valid: true,
            inner: iter,
            upper_bound,
        };
        ret.is_valid = ret.is_valid_inner();
        ret.move_to_non_delete()?;
        Ok(ret)
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.next_inner()?;
        }

        Ok(())
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        self.is_valid = self.is_valid_inner();

        Ok(())
    }

    fn is_valid_inner(&self) -> bool {
        if self.inner.is_valid() {
            match self.upper_bound.as_ref() {
                Bound::Included(key) => {
                    self.inner.key().raw_ref() <= key.as_ref()
                }
                Bound::Excluded(key) => {
                    self.inner.key().raw_ref() < key.as_ref()
                }
                Bound::Unbounded => true
            }
        } else {
            false
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_non_delete()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    meet_err: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            meet_err: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.meet_err && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("Invalid access to underlying iterator")
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Invalid access to underlying iterator")
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.meet_err {
            bail!("iterator has been tainted")
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.meet_err = true;
                return Err(e);
            }
        }

        Ok(())
    }
}
