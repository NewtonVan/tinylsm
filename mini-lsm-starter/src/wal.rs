#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::table::SIZEOF_U16;
use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)?,
            ))),
        })
    }

    pub fn recover(
        _path: impl AsRef<Path>,
        skip_list: &SkipMap<Bytes, Bytes>,
    ) -> Result<(Self, usize)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("fail to recover wal")?;
        let mut buf = vec![];
        let approximate_size = file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();

        while buf_ptr.has_remaining() {
            let key_len = buf_ptr.get_u16();
            let key_slice = &buf_ptr[..key_len as usize];
            let key = Bytes::copy_from_slice(key_slice);
            buf_ptr.advance(key_len as usize);
            let value_len = buf_ptr.get_u16();
            let value_slice = &buf_ptr[..value_len as usize];
            let value = Bytes::copy_from_slice(value_slice);
            buf_ptr.advance(value_len as usize);

            let mut hasher = crc32fast::Hasher::new();
            hasher.write_u16(key_len);
            hasher.write(key_slice);
            hasher.write_u16(value_len);
            hasher.write(value_slice);
            let checksum = buf_ptr.get_u32();
            if checksum != hasher.finalize() {
                bail!("wal fail to match checksum")
            }

            skip_list.insert(key, value);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(BufWriter::new(file))),
            },
            approximate_size,
        ))
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut writer = self.file.lock();
        let mut buf: Vec<u8> = Vec::with_capacity(key.len() + value.len() + SIZEOF_U16);
        let mut hasher = crc32fast::Hasher::new();

        hasher.write_u16(key.len() as u16);
        hasher.write(key);
        hasher.write_u16(value.len() as u16);
        hasher.write(value);

        buf.put_u16(key.len() as u16);
        buf.put_slice(key);
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
        let checksum = hasher.finalize();
        buf.put_u32(checksum);

        writer.write_all(&buf[..])?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush()?;
        writer.get_mut().sync_all()?;
        Ok(())
    }
}
