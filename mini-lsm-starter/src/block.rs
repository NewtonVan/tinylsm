#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num = self.offsets.len();

        let mut buf = self.data.clone();
        for x in &self.offsets {
            buf.put_u16(*x);
        }
        buf.put_u16(num as u16);

        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // calc idx for data and offsets
        let data_lth = data.len();
        let num = (&data[data_lth - SIZEOF_U16..]).get_u16() as usize;
        let offsets_begin = data_lth - SIZEOF_U16 - num * SIZEOF_U16;
        let raw_offsets = &data[offsets_begin..data_lth - SIZEOF_U16];

        // collect offsets
        let offsets = raw_offsets
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // collect datas
        let data = data[0..offsets_begin].to_vec();

        Self { data, offsets }
    }
}
