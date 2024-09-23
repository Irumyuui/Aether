use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::{
    memory_index::{IndexIterator, Indexer, IteratorOptions},
    records::log_record::LogRecordIndex,
};

pub struct SkipListIndexer {
    skip_list: Arc<SkipMap<Vec<u8>, LogRecordIndex>>,
}

impl SkipListIndexer {
    pub fn new() -> Self {
        Self {
            skip_list: Arc::new(SkipMap::new()),
        }
    }
}

impl Indexer for SkipListIndexer {
    fn put(&self, key: Vec<u8>, value: LogRecordIndex) -> Option<LogRecordIndex> {
        if let Some(entry) = self.skip_list.get(&key) {
            let res = Some(*entry.value());
            self.skip_list.insert(key, value);
            res
        } else {
            None
        }
    }

    fn get(&self, key: &Vec<u8>) -> Option<LogRecordIndex> {
        match self.skip_list.get(key) {
            Some(e) => Some(*e.value()),
            None => None,
        }
    }

    fn delete(&self, key: &Vec<u8>) -> Option<LogRecordIndex> {
        if let Some(entry) = self.skip_list.remove(key) {
            Some(*entry.value())
        } else {
            None
        }
    }

    fn keys(&self) -> crate::errors::Result<Vec<bytes::Bytes>> {
        Ok(self
            .skip_list
            .iter()
            .map(|e| Bytes::copy_from_slice(e.key()))
            .collect())
    }

    fn iterator(
        &self,
        opts: crate::memory_index::IteratorOptions,
    ) -> Box<dyn crate::memory_index::IndexIterator> {
        Box::new(SkipListIterator {
            items: self
                .skip_list
                .iter()
                .map(|e| (e.key().to_vec(), *e.value()))
                .collect(),
            current_index: 0,
            opts,
        })
    }
}

pub struct SkipListIterator {
    items: Vec<(Vec<u8>, LogRecordIndex)>,
    current_index: usize,
    opts: IteratorOptions,
}

impl IndexIterator for SkipListIterator {
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
            Ok(v) => v,
            Err(v) => v,
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
