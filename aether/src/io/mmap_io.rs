use std::sync::Arc;

use memmap2::Mmap;
use parking_lot::Mutex;

use crate::errors::{Error, Result};

use super::IOSupporter;

pub struct MmapIO {
    map: Arc<Mutex<Mmap>>,
}

impl MmapIO {
    pub fn new<P: AsRef<std::path::Path>>(file_name: P) -> Result<Self> {
        match std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_name)
        {
            Ok(file) => Ok(MmapIO {
                map: unsafe { Arc::new(Mutex::new(Mmap::map(&file).expect("failed to map file"))) },
            }),
            Err(e) => Err(Error::OpenDataFileFailed(e)),
        }
    }
}

impl IOSupporter for MmapIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let map_f = self.map.lock();

        let end = offset + buf.len() as u64;
        if end > map_f.len() as _ {
            return Err(Error::EndOfFile);
        }

        let data = &map_f[offset as usize..end as usize];
        buf.copy_from_slice(data);

        Ok(data.len() as _)
    }

    fn write(&self, _buf: &[u8]) -> Result<usize> {
        unimplemented!("write is not supported for mmap, the file should be opened only for read")
    }

    fn sync(&self) -> Result<()> {
        unimplemented!("sync is not supported for mmap")
    }

    fn size(&self) -> u64 {
        self.map.lock().len() as _
    }
}
