#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::Buf;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};
use crate::table::SIZEOF_U64;

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl Block {
    fn parse_first_key(&self) -> KeyVec {
        let mut entry = self.data.as_slice();
        entry.get_u16();
        let lth = entry.get_u16() as usize;
        let key = &entry[..lth];
        entry.advance(lth);
        let ts = entry.get_u64();
        KeyVec::from_vec_with_ts(key.to_vec(), ts)
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.parse_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let (begin, end) = self.value_range;
        self.block.data[begin..end].as_ref()
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0)
    }

    /// Seeks to the nth key in the block
    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            // reset to end state
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        self.idx = idx;
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
    }

    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = self.block.data[offset..].as_ref();
        let overlap = entry.get_u16() as usize;
        let rest_key_len = entry.get_u16() as usize;
        self.key.clear();
        self.key.append(&self.first_key.key_ref()[..overlap]);
        self.key.append(&entry[..rest_key_len]);
        entry.advance(rest_key_len);
        let ts = entry.get_u64();
        self.key.set_ts(ts);

        let value_lth = entry.get_u16() as usize;
        let value_offset_head = offset + SIZEOF_U16 /* overlap */ + SIZEOF_U16 /* rest key len */ + rest_key_len + SIZEOF_U64 /* ts */ + SIZEOF_U16 /* value len */;
        let value_offset_rear = value_offset_head + value_lth;
        assert_eq!(
            entry.as_ptr() as usize - self.block.data.as_ptr() as usize,
            value_offset_head,
            "incorrect cursor"
        );
        self.value_range = (value_offset_head, value_offset_rear);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut l = 0;
        let mut r = self.block.offsets.len();
        let key_vec = &key.to_key_vec();
        while l < r {
            let m = (l + r) >> 1;
            self.seek_to(m);
            assert!(self.is_valid());

            match self.key.cmp(key_vec) {
                Ordering::Less => l = m + 1,
                Ordering::Equal => return,
                Ordering::Greater => r = m,
            }
        }
        self.seek_to(l);
    }
}
