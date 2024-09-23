use std::{fs::OpenOptions, io::Write, os::unix::fs::FileExt, sync::Arc};

use parking_lot::RwLock;

use crate::errors::{Error, Result};

use super::IOSupporter;

/// 数据库文件
pub struct StdIO {
    fd: Arc<RwLock<std::fs::File>>,
}

impl StdIO {
    /// 通过传入的文件名，创建一个新的文件
    ///
    /// 可能抛出 Error::OpenDataFileFailed
    pub fn new<P: AsRef<std::path::Path>>(file_name: P) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => Ok(Self {
                fd: Arc::new(RwLock::new(file)),
            }),
            Err(e) => Err(Error::OpenDataFileFailed(e)),
        }
    }
}

impl IOSupporter for StdIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        match self.fd.read().read_at(buf, offset) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::ReadDataFileFailed(e)),
        }
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        match self.fd.write().write(buf) {
            Ok(n) => Ok(n),
            Err(e) => Err(Error::WriteDataFileFailed(e)),
        }
    }

    fn sync(&self) -> Result<()> {
        match self.fd.read().sync_all() {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::SyncDataFileFailed(e)),
        }
    }

    fn size(&self) -> u64 {
        self.fd.read().metadata().unwrap().len()
    }
}

/// 测试模块，包含异步文件 I/O 的单元测试。
#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use scopeguard::defer;

    use crate::io::{db_file::StdIO, IOSupporter};

    /// 测试异步写入功能。
    ///
    /// 该测试将数据写入临时文件并验证写入的字节数是否与数据长度匹配。
    #[test]
    fn test_write() {
        let file_name = temp_dir().join("test_write.aeher_test_file_data");
        let io = StdIO::new(&file_name).unwrap();
        defer! {
            let _ = std::fs::remove_file(file_name);
        }

        let data = b"hello world";
        let n = io.write(data).unwrap();
        assert_eq!(n, data.len());

        let data = b"test data";
        let n = io.write(data).unwrap();
        assert_eq!(n, data.len());
    }

    /// 测试异步读取功能。
    ///
    /// 该测试会先写入数据到临时文件，然后读取数据，并验证读取结果是否正确。
    #[test]
    fn test_read() {
        let file_name = temp_dir().join("test_read.aeher_test_file_data");
        let io = StdIO::new(&file_name).unwrap();

        defer! {
            let _ = std::fs::remove_file(file_name);
        }

        let data1 = b"hello world";
        let n = io.write(data1).unwrap();
        assert_eq!(n, data1.len());

        let data2 = b"test data";
        let n = io.write(data2).unwrap();
        assert_eq!(n, data2.len());

        let mut buf1 = [0u8; 11];
        let n = io.read(&mut buf1, 0).unwrap();
        assert_eq!(n, data1.len());
        assert_eq!(&buf1[..n], data1);

        let mut buf2 = [0u8; 9];
        let n = io.read(&mut buf2, data1.len() as u64).unwrap();
        assert_eq!(n, data2.len());
        assert_eq!(&buf2[..n], data2);
    }

    /// 测试同步功能。
    ///
    /// 该测试将数据写入文件后，调用同步方法以确保数据已被持久化。
    #[test]
    fn test_sync() {
        let file_name = temp_dir().join("test_sync.aeher_test_file_data");
        let io = StdIO::new(&file_name).unwrap();
        defer! {
            let _ = std::fs::remove_file(file_name);
        }

        let data = b"hello world";
        let n = io.write(data).unwrap();
        assert_eq!(n, data.len());

        io.sync().unwrap();
    }
}
