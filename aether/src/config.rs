use std::path::PathBuf;

use crate::{database::DBEngine, errors::Result, memory_index::IndexerType};

/// 数据库核心配置
#[derive(Debug, Clone)]
pub struct DBConfig {
    /// 数据库数据文件目录
    pub dir_path: PathBuf,

    /// 数据库数据文件大小
    pub data_file_size: u64,

    /// 是否每次持久化写入
    pub sync_writes: bool,

    pub indexer_type: IndexerType,

    pub mmap_start: bool,

    pub byte_per_sync: usize,
}

impl Default for DBConfig {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("aether"),
            data_file_size: 1024 * 1024 * 1024,
            sync_writes: false,
            indexer_type: IndexerType::BTree,
            mmap_start: false,
            byte_per_sync: 1024 * 1024,
        }
    }
}

impl DBConfig {
    pub fn set_dir_path(mut self, dir_path: PathBuf) -> Self {
        self.dir_path = dir_path;
        self
    }

    pub fn set_data_file_size(mut self, data_file_size: u64) -> Self {
        self.data_file_size = data_file_size;
        self
    }

    pub fn set_sync_writes(mut self, sync_writes: bool) -> Self {
        self.sync_writes = sync_writes;
        self
    }

    pub fn set_indexer_type(mut self, indexer_type: IndexerType) -> Self {
        self.indexer_type = indexer_type;
        self
    }

    pub fn set_mmap_start(mut self, mmap_start: bool) -> Self {
        self.mmap_start = mmap_start;
        self
    }

    pub fn set_byte_per_sync(mut self, byte_per_sync: usize) -> Self {
        self.byte_per_sync = byte_per_sync;
        self
    }

    pub fn build(self) -> Result<DBEngine> {
        DBEngine::new(self)
    }
}
