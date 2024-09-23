// use std::io::Error as IOError;

/// 错误类型
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("读取数据文件失败: {0}")]
    ReadDataFileFailed(std::io::Error),

    #[error("写入数据文件失败: {0}")]
    WriteDataFileFailed(std::io::Error),

    #[error("持久化数据文件失败: {0}")]
    SyncDataFileFailed(std::io::Error),

    #[error("打开数据文件失败: {0}")]
    OpenDataFileFailed(std::io::Error),

    #[error("空键")]
    EmptyKey,

    #[error("索引更新失败")]
    IndexUpdateFailed,

    #[error("键不存在: {0:?}")]
    KeyNotFound(Vec<u8>),

    #[error("数据文件不存在: {0}")]
    DataFileNotFound(String),

    #[error("创建数据库目录失败: {0}")]
    CreateDatabaseDirectoryFailed(std::io::Error),

    #[error("读到文件结尾")]
    EndOfFile,

    #[error("无效的配置: {0}")]
    InvalidConfig(String),

    #[error("读取数据库目录失败: {0}")]
    ReadDatabaseDirectoryFailed(std::io::Error),

    #[error("数据目录损坏: {0}")]
    DataDirectoryCorrupted(anyhow::Error),

    #[error("无效的记录: {0}")]
    InvalidRecord(String),

    #[error("事务提交过多")]
    TooManyPendingWrites,

    #[error("合并正在进行")]
    MergeInProgress,

    #[error("数据库已关闭")]
    DatabaseClosed,

    #[error("写批处理已禁用")]
    WriteBatchIsDisabled,

    #[error("数据库目录正在使用")]
    DatabaseDirIsUsing,
}

/// 错误返回对象
pub type Result<T> = std::result::Result<T, Error>;
