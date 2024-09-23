pub mod data_structures;
pub mod index_iterator;

use bytes::Bytes;
use data_structures::{btree, jammdb_bptree, lock_free_skip_list};

use crate::{errors::Result, records::log_record::LogRecordIndex};

/// 内存索引接口
pub trait Indexer: Send + Sync {
    /// 插入键值对
    fn put(&self, key: Vec<u8>, value: LogRecordIndex) -> Option<LogRecordIndex>;

    /// 根据键获取索引
    fn get(&self, key: &Vec<u8>) -> Option<LogRecordIndex>;

    /// 删除键值对
    fn delete(&self, key: &Vec<u8>) -> Option<LogRecordIndex>;

    /// 获取内存数据结构中所保存的所有 key
    fn keys(&self) -> Result<Vec<Bytes>>;

    fn iterator(&self, opts: IteratorOptions) -> Box<dyn IndexIterator>;
}

/// 维护内存索引所使用的数据结构
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IndexerType {
    BTree = 1,
    LockFreeSkipList = 2,
    CrossbeamSkiplist = 3,
    JammDbBptree = 4,
}

/// 内存索引工厂
impl IndexerType {
    /// 根据索引类型创建内存索引
    pub fn to_indexer(&self) -> Box<dyn Indexer> {
        match self {
            IndexerType::BTree => Box::new(btree::BTreeIndexer::new()),
            IndexerType::LockFreeSkipList => {
                Box::new(lock_free_skip_list::LockFreeSkipListIndexer::new())
            }
            IndexerType::CrossbeamSkiplist => {
                Box::new(data_structures::crossbeam_skiplist::SkipListIndexer::new())
            }
            IndexerType::JammDbBptree => unreachable!("Need path"),
        }
    }

    pub fn to_indexer_with_path<P: AsRef<std::path::Path>>(&self, path: P) -> Box<dyn Indexer> {
        match self {
            IndexerType::JammDbBptree => Box::new(jammdb_bptree::BPlusTreeIndexer::new(path)),
            _ => unreachable!(),
        }
    }

    pub fn make_indexer<P: AsRef<std::path::Path>>(&self, path: P) -> Box<dyn Indexer> {
        match self {
            IndexerType::JammDbBptree => self.to_indexer_with_path(path),
            _ => self.to_indexer(),
        }
    }
}

#[derive(Debug, Default)]
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

/// 索引迭代器
pub trait IndexIterator: Send + Sync {
    /// 重置迭代器
    fn reset(&mut self);

    /// 由传入的 key ，根据条件检索到大致符合的 key， 并将迭代器指向该 key 的位置
    fn seek(&mut self, key: &Vec<u8>);

    /// 下一个 key-value 对，如果没有下一个，则返回 None
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordIndex)>;
}
