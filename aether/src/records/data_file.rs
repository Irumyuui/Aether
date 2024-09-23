#![allow(dead_code, unused, unused_variables)]

use std::{path::PathBuf, sync::Arc};

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::{
    errors::{Error, Result},
    io::{new_io, IOType},
    records::log_record::max_log_record_header_size,
};

use super::log_record::{LogRecord, LogRecordIndex};

/// 数据文件后缀名
pub const DATA_FILE_EXTENTION_NAME: &str = ".data_a";

/// hint 文件
pub const HINT_FILE_NAME: &str = "db-hidxs";

/// 合并清理结束文件
pub const MERGE_FIN_FILE_NAME: &str = "db-mfin";

/// 额外保存事务编号
pub const TS_SEQ_NO_FILE_NAME: &str = "db-tsseqno";

/// 代表数据文件的结构体
pub struct DataFile {
    file_id: Arc<RwLock<u32>>,
    write_offset: Arc<RwLock<u64>>,
    file: Box<dyn crate::io::IOSupporter>,
}

impl DataFile {
    /// 创建一个新的数据文件实例
    #[inline]
    pub fn new<P: AsRef<std::path::Path>>(
        dir_path: P,
        file_id: u32,
        io_type: IOType,
    ) -> Result<Self> {
        let file_name = get_format_file_name(dir_path, file_id);
        let io = new_io(file_name, io_type)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(file_id)),
            write_offset: Arc::new(RwLock::new(0)),
            file: io,
        })
    }

    /// 获取当前写入偏移量
    #[inline]
    pub fn get_write_offset(&self) -> u64 {
        *self.write_offset.read()
    }

    #[inline]
    pub fn set_write_offset(&self, offset: u64) {
        *self.write_offset.write() = offset
    }

    /// 获取文件ID
    #[inline]
    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.file.size()
    }

    #[inline]
    pub fn set_io(&mut self, path: impl AsRef<std::path::Path>, io_type: IOType) {
        self.file = new_io(path, io_type).unwrap();
    }

    /// 存放事务编号的文件
    #[inline]
    pub fn open_seq_no_file<P: AsRef<std::path::Path>>(dir_path: P) -> Result<DataFile> {
        let file_name = dir_path.as_ref().join(TS_SEQ_NO_FILE_NAME);
        let io = new_io(file_name, IOType::StandardIO)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Arc::new(RwLock::new(0)),
            file: io,
        })
    }

    /// 从指定偏移量读取日志记录
    ///
    /// 日志记录的格式为：
    ///
    /// | type | key_size | value_size | key | value | crc |
    /// | :----: | :----: | :----: | :----: | :----: |
    /// |  1 byte | 4 bytes | key_size bytes | value_size bytes | 4 bytes |
    ///
    pub fn read_log_record(&self, offset: u64) -> Result<(LogRecord, usize)> {
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.file.read(&mut header_buf, offset);

        let rec_type = header_buf.get_u8();
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();

        if key_size == 0 && value_size == 0 {
            return Err(Error::EndOfFile);
        }

        let header_size = length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

        // key + value + crc
        let mut data_buf = BytesMut::zeroed(key_size + value_size + 4);

        self.file.read(&mut data_buf, offset + header_size as u64)?;

        let record = LogRecord::new(
            data_buf.get(..key_size).unwrap().into(),
            data_buf
                .get(key_size..key_size + value_size)
                .unwrap()
                .into(),
            rec_type.into(),
        );
        data_buf.advance(key_size + value_size);

        if data_buf.get_u32() != record.get_crc() {
            return Err(Error::InvalidRecord("CRC check failed".to_string()));
        }

        Ok((record, header_size + key_size + value_size + 4))
    }

    /// 写入数据到文件
    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let read_bytes = self.file.write(buf)?;
        *self.write_offset.write() += read_bytes as u64;
        Ok(read_bytes)
    }

    /// 同步数据到文件
    pub fn sync(&self) -> Result<()> {
        self.file.sync()
    }

    /// 打开索引 hint
    pub fn open_hint_file<P: AsRef<std::path::Path>>(dir_path: P) -> Result<DataFile> {
        let file_name = dir_path.as_ref().join(HINT_FILE_NAME);
        let io = new_io(file_name, IOType::StandardIO)?;

        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Arc::new(RwLock::new(0)),
            file: io,
        })
    }

    pub fn write_hint_record(&self, key: Vec<u8>, index: LogRecordIndex) -> Result<()> {
        let data = LogRecord::normal(key, index.encode()).encode();
        self.write(&data)?;
        Ok(())
    }

    /// merge 结束文件
    pub fn open_merge_file<P: AsRef<std::path::Path>>(dir_path: P) -> Result<DataFile> {
        let file_name = dir_path.as_ref().join(MERGE_FIN_FILE_NAME);
        let io = new_io(file_name, IOType::StandardIO)?;
        Ok(DataFile {
            file_id: Arc::new(RwLock::new(0)),
            write_offset: Arc::new(RwLock::new(0)),
            file: io,
        })
    }

    pub fn file_size(&self) -> u64 {
        self.file.size()
    }
}

pub(crate) fn get_format_file_name<P: AsRef<std::path::Path>>(
    dir_path: P,
    file_id: u32,
) -> PathBuf {
    dir_path
        .as_ref()
        .join(format!("{file_id:018}{DATA_FILE_EXTENTION_NAME}"))
}

#[cfg(test)]
mod tests {
    use scopeguard::defer;

    use crate::{
        database::merge::MERGE_DIR_NAME,
        io::IOType,
        records::{
            data_file::{get_format_file_name, HINT_FILE_NAME},
            log_record::LogRecord,
        },
    };

    use super::DataFile;

    /// 测试数据文件是否创建成功
    #[test]
    fn test_new_data_file() {
        let file = DataFile::new(std::env::temp_dir(), 1, IOType::StandardIO).unwrap();
        let path = get_format_file_name(&std::env::temp_dir(), 1);
        defer! {
            let _ = std::fs::remove_file(&path);
        }

        assert_eq!(file.get_file_id(), 1);
        assert_eq!(file.get_write_offset(), 0);
        assert!(path.exists());
    }

    /// 测试数据文件读取日志记录是否成功
    #[test]
    fn test_read_log_record() {
        let file = DataFile::new(std::env::temp_dir(), 2, IOType::StandardIO).unwrap();
        let path = get_format_file_name(&std::env::temp_dir(), 2);
        defer! {
            let _ = std::fs::remove_file(&path);
        }

        // 写入 nomal
        let nomal_record = LogRecord::normal(b"key".to_vec(), b"value".to_vec());
        assert!(file.write(&nomal_record.encode()).is_ok());

        let read_res = file.read_log_record(0);
        assert!(read_res.is_ok());
        let (read_record, read_size) = read_res.unwrap();
        assert_eq!(nomal_record, read_record);

        // 写入 delete
        let del_record = LogRecord::deleted(b"key".to_vec());
        assert!(file.write(&del_record.encode()).is_ok());
        let read_res = file.read_log_record(read_size as u64);
        assert!(read_res.is_ok());
        let (read_record, read_size) = read_res.unwrap();
        assert_eq!(del_record, read_record);
    }

    /// 测试数据文件写入数据是否成功
    #[test]
    fn test_write() {
        let file = DataFile::new(std::env::temp_dir(), 3, IOType::StandardIO).unwrap();
        let path = get_format_file_name(&std::env::temp_dir(), 3);
        defer! {
            let _ = std::fs::remove_file(&path);
        }

        let mut offset = 0;
        assert_eq!(file.get_write_offset(), 0);

        let data = b"hello world";
        let write_bytes = file.write(data).unwrap();
        assert_eq!(write_bytes, data.len());
        offset += data.len() as u64;
        assert_eq!(file.get_write_offset(), offset);

        let data = b"1234567890";
        let write_bytes = file.write(data).unwrap();
        assert_eq!(write_bytes, data.len());
        offset += data.len() as u64;
        assert_eq!(file.get_write_offset(), offset);
    }

    /// 测试数据文件同步是否成功
    #[test]
    fn test_sync() {
        let file = DataFile::new(std::env::temp_dir(), 4, IOType::StandardIO).unwrap();
        let path = get_format_file_name(&std::env::temp_dir(), 4);
        defer! {
            let _ = std::fs::remove_file(&path);
        }

        let data = b"hello world";
        let write_bytes = file.write(data).unwrap();
        assert_eq!(write_bytes, data.len());
        assert_eq!(file.get_write_offset(), data.len() as u64);

        file.sync().unwrap();
    }

    #[test]
    fn test_new_hint_file() {
        let db_path = std::env::temp_dir().join("test_new_hint_file");
        let del_path = db_path.clone();
        defer! {
            let _ = std::fs::remove_dir_all(del_path);
        }

        std::fs::create_dir_all(&db_path).unwrap();
        let merge_dir = db_path.join(MERGE_DIR_NAME);
        std::fs::create_dir_all(&merge_dir).unwrap();
        let hint_file = DataFile::open_hint_file(&merge_dir).unwrap();
        assert!(std::fs::exists(merge_dir.join(HINT_FILE_NAME)).is_ok());
    }
}
