#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;
use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Estimate size of data + offsets
    fn estimate_size(&self) -> usize {
        SIZEOF_U16 /* number of kv paris*/ + self.offsets.len() * SIZEOF_U16 /* offsets */ + self.data.len()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_lth = key.len();
        let value_lth = value.len();
        let offset = self.data.len();

        if self.estimate_size() + key_lth + value_lth + SIZEOF_U16 * 3 /* record var length */ > self.block_size && !self.is_empty() {
            return false;
        }

        self.data.put_u16(key_lth as u16);
        self.data.put(key.into_inner());
        self.data.put_u16(value_lth as u16);
        self.data.put(value);
        self.offsets.push(offset as u16);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block shouldn't be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
