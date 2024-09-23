use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;

use crate::{
    memory_index::{IndexIterator, Indexer, IteratorOptions},
    records::log_record::{decode_to_log_index, LogRecordIndex},
};

const BPTREE_INDEX_FILE_NAME: &str = "d_bptree_index";
const BPTREE_BUCKET_NAME: &str = "d_bptree_bucket";

pub struct BPlusTreeIndexer {
    tree: Arc<jammdb::DB>,
}

impl BPlusTreeIndexer {
    pub fn new<P: AsRef<std::path::Path>>(dir_path: P) -> Self {
        let db = Arc::new(
            jammdb::DB::open(dir_path.as_ref().join(BPTREE_INDEX_FILE_NAME))
                .inspect_err(|e| eprintln!("{:?}", e))
                .unwrap(),
        );

        let tx = db.tx(true).unwrap();
        tx.get_or_create_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.commit().unwrap();

        Self { tree: db }
    }
}

impl Indexer for BPlusTreeIndexer {
    fn put(&self, key: Vec<u8>, value: LogRecordIndex) -> Option<LogRecordIndex> {
        let tx = self.tree.tx(true).unwrap();
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        let mut old_v = None;
        if let Some(pair) = bucket.get_kv(&key) {
            old_v = Some(decode_to_log_index(&pair.value().to_vec()));
        }

        bucket.put(key, value.encode()).expect("put failed");
        tx.commit().unwrap();

        old_v
    }

    fn get(&self, key: &Vec<u8>) -> Option<LogRecordIndex> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        if let Some(p) = bucket.get_kv(key) {
            Some(decode_to_log_index(&p.value().to_vec()))
        } else {
            None
        }
    }

    fn delete(&self, key: &Vec<u8>) -> Option<LogRecordIndex> {
        let tx = self.tree.tx(true).unwrap();
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        let mut res = None;
        if let Ok(p) = bucket.delete(key) {
            res = Some(decode_to_log_index(&p.value().to_vec()))
        }
        tx.commit().unwrap();
        res
    }

    fn keys(&self) -> crate::errors::Result<Vec<bytes::Bytes>> {
        Ok(self
            .tree
            .tx(false)
            .unwrap()
            .get_bucket(BPTREE_BUCKET_NAME)
            .unwrap()
            .cursor()
            .map(|d| Bytes::copy_from_slice(d.key()))
            .collect())
    }

    fn iterator(
        &self,
        opts: crate::memory_index::IteratorOptions,
    ) -> Box<dyn crate::memory_index::IndexIterator> {
        let mut items = self
            .tree
            .tx(false)
            .unwrap()
            .get_bucket(BPTREE_BUCKET_NAME)
            .unwrap()
            .cursor()
            .map(|d| {
                (
                    d.key().to_vec(),
                    decode_to_log_index(&d.kv().value().to_vec()),
                )
            })
            .collect_vec();

        if opts.reverse {
            items.reverse();
        }

        Box::new(BPlusTreeIterator {
            items,
            current_index: 0,
            opts,
        })
    }
}

pub struct BPlusTreeIterator {
    items: Vec<(Vec<u8>, LogRecordIndex)>,
    current_index: usize,
    opts: IteratorOptions,
}

impl IndexIterator for BPlusTreeIterator {
    fn reset(&mut self) {
        self.current_index = 0;
    }

    fn seek(&mut self, key: &Vec<u8>) {
        self.current_index = match self.items.binary_search_by(|(k, _)| {
            if self.opts.reverse {
                k.cmp(&key).reverse()
            } else {
                k.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordIndex)> {
        while let Some(item) = self.items.get(self.current_index) {
            self.current_index += 1;

            let prefix = &self.opts.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use scopeguard::defer;

    use crate::{
        memory_index::{data_structures::jammdb_bptree::BPlusTreeIndexer, Indexer},
        records::log_record::LogRecordIndex,
    };

    #[test]
    fn test_bptree_put() {
        let test_path = std::env::temp_dir().join("test_bptree_put");
        let path = test_path.clone();

        std::fs::create_dir_all(&test_path).expect("create test dir failed.");
        defer! {
            let _ = std::fs::remove_dir_all(test_path);
        }

        let bpt = BPlusTreeIndexer::new(&path);

        bpt.put(b"ccbde".to_vec(), LogRecordIndex::new(123, 123));
        bpt.put(b"bbed".to_vec(), LogRecordIndex::new(123, 123));

        assert_eq!(bpt.keys().unwrap().len(), 2);
    }

    #[test]
    fn test_bptrree_get() {
        let test_path = std::env::temp_dir().join("test_bptree_get");
        let path = test_path.clone();

        std::fs::create_dir_all(&test_path).expect("create test dir failed.");
        defer! {
            let _ = std::fs::remove_dir_all(test_path);
        }

        let bpt = BPlusTreeIndexer::new(&path);

        bpt.put(b"ccbde".to_vec(), LogRecordIndex::new(123, 123));
        bpt.put(b"bbed".to_vec(), LogRecordIndex::new(123, 123));

        assert_eq!(
            bpt.get(&b"ccbde".to_vec()).unwrap(),
            LogRecordIndex::new(123, 123)
        );
        assert_eq!(
            bpt.get(&b"bbed".to_vec()).unwrap(),
            LogRecordIndex::new(123, 123)
        );

        drop(bpt);
    }
}
