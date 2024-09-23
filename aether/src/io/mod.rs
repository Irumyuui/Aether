pub mod db_file;
pub mod mmap_io;

use db_file::StdIO;

use crate::errors::Result;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum IOType {
    StandardIO,
    Mmap,
}

/// 管理文件 IO 的统一接口
pub trait IOSupporter: Sync + Send {
    /// 从文件中根据偏移量读取对应数据，返回 `Result<usize>` 表示读取字节的大小
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// 写入字节数组到文件中，返回 `Result<usize>` 表示写入字节的大小
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// 持久化数据
    fn sync(&self) -> Result<()>;

    /// 文件大小
    fn size(&self) -> u64;
}

pub fn new_io(
    file_name: impl AsRef<std::path::Path>,
    io_type: IOType,
) -> Result<Box<dyn IOSupporter>> {
    let io: Box<dyn IOSupporter> = match io_type {
        IOType::StandardIO => Box::new(StdIO::new(file_name)?),
        IOType::Mmap => Box::new(mmap_io::MmapIO::new(file_name)?),
    };

    Ok(io)
}
