#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)?,
            )),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("fail to recover manifest")?;
        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();

        let mut records = vec![];
        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[..len as usize];
            let record: ManifestRecord = serde_json::from_slice(slice)?;
            buf_ptr.advance(len as usize);

            let checksum = crc32fast::hash(slice);
            if checksum != buf_ptr.get_u32() {
                bail!("manifest fail to match checksum");
            }

            records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut buf = serde_json::to_vec(&_record)?;
        let len = buf.len();
        let checksum = crc32fast::hash(&buf[..]);
        buf.put_u32(checksum);

        let mut guard = self.file.lock();

        guard.write_all(&(len as u64).to_be_bytes())?;
        guard.write_all(&buf)?;
        guard.sync_all()?;
        Ok(())
    }
}
