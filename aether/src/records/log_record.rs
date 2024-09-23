use bytes::{BufMut, BytesMut};
use prost::{
    encode_length_delimiter,
    encoding::{decode_varint, encode_varint},
    length_delimiter_len,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRecordType {
    /// 正常 Put 的数据
    Normal = 1,

    /// 被删除的数据标志
    Deleted = 2,

    /// 事务完成
    TransactionEnd = 3,
}

impl LogRecordType {
    fn to_u8(&self) -> u8 {
        *self as u8
    }
}

impl From<u8> for LogRecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Normal,
            2 => Self::Deleted,
            3 => Self::TransactionEnd,
            _ => panic!("Invalid LogRecordType value: {}", value),
        }
    }
}

/// 数据库索引位置信息
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRecordIndex {
    /// 数据存储的文件 id
    file_id: u32,

    /// 该数据所存储在文件中的哪个位置
    offset: u64,
}

impl LogRecordIndex {
    #[inline]
    pub fn new(file_id: u32, offset: u64) -> Self {
        Self { file_id, offset }
    }

    #[inline]
    pub fn get_file_id(&self) -> u32 {
        self.file_id
    }

    #[inline]
    pub fn get_offset(&self) -> u64 {
        self.offset
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        encode_varint(self.file_id as u64, &mut buf);
        encode_varint(self.offset, &mut buf);
        buf.into()
    }
}

pub fn decode_to_log_index(src: &Vec<u8>) -> LogRecordIndex {
    let mut buf = BytesMut::new();
    buf.put_slice(&src);

    let file_id = match decode_varint(&mut buf) {
        Ok(v) => v,
        Err(e) => panic!("decode_varint error: {}", e),
    };
    let offset = match decode_varint(&mut buf) {
        Ok(v) => v,
        Err(e) => panic!("decode_varint error: {}", e),
    };

    LogRecordIndex::new(file_id as u32, offset)
}

#[derive(Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub rec_type: LogRecordType,
}

impl LogRecord {
    #[inline]
    pub fn new(key: Vec<u8>, value: Vec<u8>, rec_type: LogRecordType) -> Self {
        Self {
            key,
            value,
            rec_type,
        }
    }

    /// 创建一个正常的日志记录
    #[inline]
    pub fn normal(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::new(key, value, LogRecordType::Normal)
    }

    /// 创建一个被删除的日志记录
    #[inline]
    pub fn deleted(key: Vec<u8>) -> Self {
        Self::new(key, vec![], LogRecordType::Deleted)
    }

    /// 编码日志记录为字节数组
    pub fn encode(&self) -> Vec<u8> {
        self.encoded_with_crc().0
    }

    /// 获得日志记录的 crc32 校验码
    pub fn get_crc(&self) -> u32 {
        self.encoded_with_crc().1
    }

    /// 获取编码的长度
    ///
    /// type + key_size + value_size + key + value + crc32
    fn encoded_len(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }

    /// 因为最后一个字节是 crc32 校验码，所以序列化之后就得到这两个了
    fn encoded_with_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::with_capacity(self.encoded_len());

        // 序列化
        buf.put_u8(self.rec_type.to_u8());
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);

        // 计算 crc32 校验码
        let crc = crc32fast::hash(&buf);
        buf.put_u32(crc);

        (buf.to_vec(), crc)
    }
}

/// 日志文件头部最大的长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}
