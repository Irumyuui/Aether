use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    errors::Result,
    memory_index::{self, IndexIterator, Indexer, IteratorOptions},
    records::log_record::LogRecordIndex,
};

/// BTree 索引器
///
/// 基于 BTree 实现的内存索引器，支持 put、get、delete、keys、iterator 方法。
///
/// BTree 索引器的实现并不保证线程安全，因此在多线程环境下需要使用 `Arc<RwLock<BTreeMap<Vec<u8>, LogRecordIndex>>>` 包装。
#[derive(Debug, Default)]
pub struct BTreeIndexer {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordIndex>>>,
}

impl BTreeIndexer {
    #[inline]
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTreeIndexer {
    fn put(&self, key: Vec<u8>, value: LogRecordIndex) -> Option<LogRecordIndex> {
        self.tree.write().insert(key, value)
    }

    fn get(&self, key: &Vec<u8>) -> Option<LogRecordIndex> {
        self.tree.read().get(key).cloned()
    }

    fn delete(&self, key: &Vec<u8>) -> Option<LogRecordIndex> {
        self.tree.write().remove(key)
    }

    /// 一个 `Result<Vec<Bytes>>` 类型的结果，其中 `Vec<Bytes>` 包含所有 key 的字节形式。
    fn keys(&self) -> Result<Vec<Bytes>> {
        Ok(self
            .tree
            .read()
            .keys()
            .cloned()
            .into_iter()
            .map(|k| k.into())
            .collect())
    }

    /// 获取迭代器时，会复制一份数据，即该迭代器表现的是获取迭代器时刻内存索引的状态，不会实时跟随内存索引的变化。
    fn iterator(
        &self,
        opts: memory_index::IteratorOptions,
    ) -> Box<dyn memory_index::IndexIterator> {
        let copied_items = if !opts.reverse {
            self.tree.read().clone().into_iter().collect()
        } else {
            self.tree.read().clone().into_iter().rev().collect()
        };

        Box::new(BTreeIterator {
            kv_pairs: copied_items,
            cur_index: Default::default(),
            opts,
        })
    }
}

/// BTree 迭代器
pub struct BTreeIterator {
    /// Key-Value 当前的状态
    kv_pairs: Vec<(Vec<u8>, LogRecordIndex)>,

    /// 当前便利的游标
    cur_index: usize,

    /// 配置
    opts: IteratorOptions,
}

impl IndexIterator for BTreeIterator {
    /// 重置游标
    fn reset(&mut self) {
        self.cur_index = 0;
    }

    /// 定位游标，定位到第一个小于等于给定 `key` 的位置
    ///
    /// 如果 `opts.reverse` 为 `true` ，则定位到第一个大于等于给定 `key` 的位置
    fn seek(&mut self, key: &Vec<u8>) {
        self.cur_index = match self.kv_pairs.binary_search_by(|(k, _)| {
            if self.opts.reverse {
                key.cmp(k)
            } else {
                k.cmp(key)
            }
        }) {
            Ok(v) => v,
            Err(v) => v,
        };
    }

    /// 移动到下一个位置，如果没有返回 `None`
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordIndex)> {
        if self.cur_index >= self.kv_pairs.len() {
            return None;
        }

        while let Some((key, value)) = self.kv_pairs.get(self.cur_index) {
            self.cur_index += 1;
            if self.opts.prefix.is_empty() || key.starts_with(&self.opts.prefix) {
                return Some((key, value));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{memory_index::Indexer, records::log_record::LogRecordIndex};

    /// 测试 put 方法，测试 put 成功后，get 方法能正确返回值
    #[test]
    fn test_btree_put() {
        let btree = super::BTreeIndexer::new();
        assert_eq!(btree.put(b"key1".to_vec(), LogRecordIndex::new(1, 1)), None);
        assert_eq!(btree.put(b"key2".to_vec(), LogRecordIndex::new(2, 2)), None);
    }

    /// 测试 get 方法，测试 put 成功后，get 方法能正确返回值
    #[test]
    fn test_btree_get() {
        let btree = super::BTreeIndexer::new();

        assert_eq!(btree.put(b"key1".to_vec(), LogRecordIndex::new(1, 1)), None);
        assert_eq!(btree.put(b"key2".to_vec(), LogRecordIndex::new(2, 2)), None);

        assert_eq!(
            btree.get(&b"key1".to_vec()),
            Some(LogRecordIndex::new(1, 1))
        );
        assert_eq!(
            btree.get(&b"key2".to_vec()),
            Some(LogRecordIndex::new(2, 2))
        );
        assert_eq!(btree.get(&b"key3".to_vec()), None);
    }

    /// 测试 delete 方法，测试 put 成功后，delete 方法能正确删除值
    #[test]
    fn test_btree_delete() {
        let btree = super::BTreeIndexer::new();
        assert_eq!(btree.put(b"key1".to_vec(), LogRecordIndex::new(1, 1)), None);
        assert_eq!(
            btree.delete(&b"key1".into()),
            Some(LogRecordIndex::new(1, 1))
        );
    }
}
