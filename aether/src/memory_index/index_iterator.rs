use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{database::DBEngine, errors::Result};

use super::{IndexIterator, IteratorOptions};

/// 通用迭代器实现
pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
    db: &'a DBEngine,
}

impl Iterator<'_> {
    /// 重置迭代器
    pub fn reset(&self) {
        self.index_iter.write().reset();
    }

    /// 定位到指定key
    pub fn seek(&self, key: &Vec<u8>) {
        self.index_iter.write().seek(key);
    }

    /// 定位到第一个key
    ///
    /// 返回第一个 key 的 value，如果到结尾，则返回 None
    pub fn next(&self) -> Option<(Bytes, Bytes)> {
        // self.index_iter.write().next()
        let mut iter = self.index_iter.write();
        match iter.next() {
            Some((k, v)) => Some((
                Bytes::from(k.clone()),
                self.db
                    .get_value_by_index(v)
                    .expect("data not found, maybe corrupted?"),
            )),
            None => None,
        }
    }
}

impl DBEngine {
    /// 创建一个新的迭代器
    ///
    /// 注意：迭代器只能用于一次性遍历，不能在迭代过程中修改数据库内容
    pub fn iter(&self, opts: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.indexer.iterator(opts))),
            db: self,
        }
    }

    /// 获取所有 key
    pub fn keys(&self) -> Result<Vec<Bytes>> {
        self.indexer.keys()
    }

    /// 遍历所有 key-value 对
    pub fn map<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(&Bytes, &Bytes) -> bool,
    {
        let iter = self.iter(IteratorOptions::default());
        while let Some((k, v)) = iter.next() {
            if !f(&k, &v) {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use parking_lot::RwLock;
    use scopeguard::defer;

    use crate::{config::DBConfig, database::DBEngine, memory_index::IteratorOptions};

    /// 测试获取空数据库的 key 列表
    #[test]
    fn test_get_empty_keys() {
        let dir_path = std::env::temp_dir().join("test_get_empty_keys");
        let conf = DBConfig::default().set_dir_path(dir_path.clone());
        let db = DBEngine::new(conf).expect("数据库打开失败");
        defer! {
            let _ = std::fs::remove_dir_all(dir_path);
        }

        // 空数据库
        let keys = db.keys().expect("获取 key 失败");
        assert!(keys.is_empty());
    }

    /// 测试获取非空数据库的 key 列表
    #[test]
    fn test_get_non_empty_keys() {
        let dir_path = std::env::temp_dir().join("test_get_non_empty_keys");
        let conf = DBConfig::default().set_dir_path(dir_path.clone());
        let db = DBEngine::new(conf).expect("数据库打开失败");
        defer! {
            let _ = std::fs::remove_dir_all(dir_path);
        }

        // 写入数据
        let k1 = b"k1".to_vec();
        let v1 = b"v1".to_vec();
        let k2 = b"k2".to_vec();
        let v2 = b"v2".to_vec();
        db.put(k1.clone().into(), v1.clone().into())
            .expect("put 失败");
        db.put(k2.clone().into(), v2.clone().into())
            .expect("put 失败");

        // 获取 key 列表
        let keys = db.keys().expect("获取 key 失败");
        assert_eq!(vec![Bytes::from(k1), Bytes::from(k2)], keys);
    }

    /// 测试遍历所有 key-value 对 并应用 mapping 函数
    #[test]
    fn test_map_all() {
        let path = std::env::temp_dir().join("test_map_all");
        let conf = DBConfig::default().set_dir_path(path.clone());
        let db = DBEngine::new(conf).expect("数据库打开失败");
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        // 写入数据
        let k1 = b"k1".to_vec();
        let v1 = b"v1".to_vec();
        let k2 = b"k2".to_vec();
        let v2 = b"v2".to_vec();
        db.put(k1.clone().into(), v1.clone().into())
            .expect("put 失败");
        db.put(k2.clone().into(), v2.clone().into())
            .expect("put 失败");

        // 遍历所有 key-value 对
        let count = Arc::new(RwLock::new(0));
        let t = Arc::clone(&count);
        db.map(|k, v| {
            assert!(!k.is_empty());
            assert!(!v.is_empty());
            *t.write() += 1;
            true
        })
        .expect("遍历失败");

        assert_eq!(*count.write(), 2);
    }

    /// 测试 seek
    #[test]
    fn test_seek() {
        let path = std::env::temp_dir().join("test_seek");
        let conf = DBConfig::default().set_dir_path(path.clone());
        let db = DBEngine::new(conf).expect("数据库打开失败");
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        // 写入数据
        let k1 = b"k1".to_vec();
        let v1 = b"v1".to_vec();
        let k2 = b"k2".to_vec();
        let v2 = b"v2".to_vec();
        db.put(k1.clone().into(), v1.clone().into())
            .expect("put 失败");
        db.put(k2.clone().into(), v2.clone().into())
            .expect("put 失败");

        // 定位到 k2
        let iter = db.iter(IteratorOptions::default());
        iter.seek(&k2);
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
    }

    /// 测试 next
    #[test]
    fn test_iter_to_next() {
        let path = std::env::temp_dir().join("test_iter_to_next");
        let conf = DBConfig::default().set_dir_path(path.clone());
        let db = DBEngine::new(conf).expect("数据库打开失败");
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        // 写入数据
        let k1 = b"k1".to_vec();
        let v1 = b"v1".to_vec();
        let k2 = b"k2".to_vec();
        let v2 = b"v2".to_vec();
        db.put(k1.clone().into(), v1.clone().into())
            .expect("put 失败");
        db.put(k2.clone().into(), v2.clone().into())
            .expect("put 失败");

        // 遍历所有 key-value 对
        let iter = db.iter(IteratorOptions::default());
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert_eq!(iter.next(), None);
    }

    /// 测试前缀匹配
    #[test]
    fn test_prefix_seek() {
        let path = std::env::temp_dir().join("test_prefix_seek");
        let conf = DBConfig::default().set_dir_path(path.clone());
        let db = DBEngine::new(conf).expect("数据库打开失败");
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        // 写入数据
        let k1 = b"k1".to_vec();
        let v1 = b"v1".to_vec();
        let k2 = b"k2".to_vec();
        let v2 = b"v2".to_vec();
        db.put(k1.clone().into(), v1.clone().into())
            .expect("put 失败");
        db.put(k2.clone().into(), v2.clone().into())
            .expect("put 失败");

        // 前缀匹配
        let iter = db.iter(IteratorOptions {
            prefix: b"k2".to_vec(),
            ..IteratorOptions::default()
        });
        assert_eq!(iter.next(), Some((k2.clone().into(), v2.clone().into())));
    }
}
