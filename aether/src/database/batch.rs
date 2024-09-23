use std::{collections::HashMap, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prost::{decode_length_delimiter, encode_length_delimiter};

use crate::{
    errors::{Error, Result},
    memory_index::IndexerType,
    records::log_record::{LogRecord, LogRecordType},
};

use super::db::DBEngine;

const TS_FIN_KEY: &[u8] = b"__aether_ts_fin__";

/// 批量写入选项
///
/// # Examples
///
/// ```rust
/// use aether::database::batch::WriteBatchOptions;
///
/// let opts = WriteBatchOptions::default()
///                 .set_max_batch_num(1000)
///                 .set_sync_writes(true);
/// ```
#[derive(Debug)]
pub struct WriteBatchOptions {
    /// 一次中最多写入多少条记录
    max_batch_num: usize,

    /// 提交事务时是否立刻将数据同步到磁盘
    sync_writes: bool,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 114514,
            sync_writes: true,
        }
    }
}

impl WriteBatchOptions {
    #[inline]
    pub fn set_max_batch_num(mut self, max_batch_num: usize) -> Self {
        self.max_batch_num = max_batch_num;
        self
    }

    pub fn max_batch_num(&self) -> usize {
        self.max_batch_num
    }

    #[inline]
    pub fn set_sync_writes(mut self, sync_writes: bool) -> Self {
        self.sync_writes = sync_writes;
        self
    }

    pub fn sync_writes(&self) -> bool {
        self.sync_writes
    }

    pub fn build(self, db: &DBEngine) -> Result<WriteBatch> {
        db.open_write_batch(self)
    }
}

/// 批量写入记录
pub struct WriteBatch<'a> {
    /// 待写入的记录
    pending_writes: Arc<Mutex<HashMap<Vec<u8>, LogRecord>>>,
    db: &'a DBEngine,
    opts: WriteBatchOptions,
}

impl DBEngine {
    pub fn open_write_batch(&self, opts: WriteBatchOptions) -> Result<WriteBatch> {
        if self.conf.indexer_type == IndexerType::JammDbBptree
            && !self.seq_file_exists
            && !self.is_initial
        {
            return Err(Error::WriteBatchIsDisabled);
        }

        Ok(WriteBatch {
            pending_writes: Default::default(), // Arc::new(Mutex::new(HashMap::new())),
            db: self,
            opts,
        })
    }
}

/// 相比于 DB 的实现，这里其实就是再度包装了一下那些需要修改到的操作
///
/// 然后需要提供一个接口，来提交事务
impl WriteBatch<'_> {
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        let record = LogRecord::normal(key.clone().into(), value.into());
        self.pending_writes.lock().insert(key.into(), record);

        Ok(())
    }

    // pub fn get(&self, key: Bytes) -> Result<Bytes> {
    //     todo!()
    // }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        let mut pending_writes = self.pending_writes.lock();

        let key = key.into();
        let index = self.db.indexer.get(&key);

        // 如果没找到这个数据，那么找找之前操作有没有还没写的
        // 否则就是失败操作，不过无所谓
        if index.is_none() {
            if pending_writes.contains_key(&key) {
                pending_writes.remove(&key);
            }
            return Ok(());
        }

        let record = LogRecord::deleted(key.clone());
        pending_writes.insert(key, record);

        Ok(())
    }

    /// 提交事务
    ///
    /// 如果是空的，那么无所谓，直接结束事务即可
    ///
    /// 然后呢，因为现在要保证最强的串行化事务，那么就需要给 db 上大锁
    ///
    /// 最后提交完所有操作后，需要附带一个标识，表示提交了事务
    pub fn commit(&self) -> Result<()> {
        let mut pending_writes = self.pending_writes.lock();
        if pending_writes.is_empty() {
            return Ok(());
        }

        if pending_writes.len() > self.opts.max_batch_num() {
            return Err(Error::TooManyPendingWrites);
        }

        let _db_lock_guard = self.db.batch_commit_lock.lock();

        // 新的事务编号
        let seq_no = self
            .db
            .batch_commit_seq_no
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut indexs = HashMap::new();
        for (_, source_record) in pending_writes.iter() {
            let key = encord_log_key_with_ts(&source_record.key, seq_no);
            let record = LogRecord::new(key, source_record.value.clone(), source_record.rec_type);
            let rec_index = self.db.append_log_record(&record)?;
            indexs.insert(source_record.key.clone(), rec_index);
        }

        let finish_record = LogRecord {
            key: encord_log_key_with_ts(&TS_FIN_KEY.to_vec(), seq_no),
            value: Default::default(),
            rec_type: LogRecordType::TransactionEnd,
        };
        self.db.append_log_record(&finish_record)?;

        // 这里其实可以考虑用一个线程来异步执行，不过先不管了
        if self.opts.sync_writes() {
            self.db.sync()?;
        }

        // fuck match
        for (_, rec) in pending_writes.drain() {
            if rec.rec_type == LogRecordType::Normal {
                self.db
                    .indexer
                    .put(rec.key.clone(), *indexs.get(&rec.key).unwrap());
            } else if rec.rec_type == LogRecordType::Deleted {
                self.db.indexer.delete(&rec.key);
            }
        }

        Ok(())
    }
}

/// 给 key 加上事务编号
pub(crate) fn encord_log_key_with_ts(key: &Vec<u8>, seq_no: usize) -> Vec<u8> {
    let mut res_key = BytesMut::new();
    encode_length_delimiter(seq_no, &mut res_key).unwrap();
    res_key.extend_from_slice(&key);
    res_key.into()
}

/// 从带有事务号的 key 中解析出事务编号和真正的 key ，返回 key 与 事务编号
pub(crate) fn decord_log_key_with_ts(key: &Vec<u8>) -> (Vec<u8>, usize) {
    let mut buf = BytesMut::new();
    buf.put_slice(&key);
    let seq_no = decode_length_delimiter(&mut buf).unwrap();
    (buf.into(), seq_no)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use scopeguard::defer;

    use crate::{config::DBConfig, database::batch::WriteBatchOptions, errors::Error};

    /// 测试写入，然后提交事务
    #[test]
    fn test_wb() {
        let path = std::env::temp_dir().join("test_wb");
        let db = DBConfig::default()
            .set_data_file_size(1024 * 1024)
            .set_dir_path(path.clone())
            .build()
            .expect("数据库启动失败");

        let d_path = path.clone();
        defer! {
            let _ = std::fs::remove_dir_all(d_path);
        }

        let wb = WriteBatchOptions::default()
            .build(&db)
            .expect("创建 WriteBatch 失败");

        // // 写一次，还没提交事务，此时应该没有找到
        // wb.put(Bytes::from("key1"), Bytes::from("1"))
        //     .expect("写入失败");
        // let res = db.get("key1".into());
        // assert!(if let Err(Error::KeyNotFound(_)) = res {
        //     true
        // } else {
        //     false
        // });

        // 写入两条数据，在提交事务之前先检查数据是否被写入了，此时数据没有找到
        // 然后提交事务，检查数据是否被写入了，此时数据应该被找到，且事务号应为 2
        wb.put(Bytes::from("key1"), Bytes::from("1"))
            .expect("写入失败");
        wb.put(Bytes::from("key2"), Bytes::from("2"))
            .expect("写入失败");

        let res = db.get("key1".into());
        assert!(if let Err(Error::KeyNotFound(_)) = res {
            true
        } else {
            false
        });
        let res = db.get("key2".into());
        assert!(if let Err(Error::KeyNotFound(_)) = res {
            true
        } else {
            false
        });

        wb.commit().expect("提交事务失败");

        let res = db.get("key1".into()).expect("key1 应该被找到");
        assert_eq!(res, Bytes::from("1"));

        let res = db.get("key2".into()).expect("key2 应该被找到");
        assert_eq!(res, Bytes::from("2"));

        // 写入的是 1 ，当前事务为 2
        let seq_no = db.get_latest_seq_no();
        assert_eq!(seq_no, 2);

        // 再次写入数据，提交事务，检查数据是否被写入了，此时数据应该被找到，且事务号应为 3
        wb.put(Bytes::from("key1"), Bytes::from("3"))
            .expect("写入失败");
        wb.commit().expect("提交事务失败");
        let res = db.get("key1".into()).expect("key1 应该被找到");
        assert_eq!(res, Bytes::from("3"));

        let seq_no = db.get_latest_seq_no();
        assert_eq!(seq_no, 3);

        drop(wb);
        drop(db);

        // 再次创建数据库，读取数据，检查数据是否正确
        let db = DBConfig::default()
            .set_data_file_size(1024 * 1024)
            .set_dir_path(path.clone())
            .build()
            .expect("数据库启动失败");

        let res = db.get("key1".into()).expect("key1 应该被找到");
        assert_eq!(res, Bytes::from("3"));

        let res = db.get("key2".into()).expect("key2 应该被找到");
        assert_eq!(res, Bytes::from("2"));

        let seq_no = db.get_latest_seq_no();
        assert_eq!(seq_no, 3);

        // 测试删除 key1
        let wb = WriteBatchOptions::default()
            .build(&db)
            .expect("创建 WriteBatch 失败");

        wb.delete(Bytes::from("key1")).expect("删除 key1 失败");
        // 事务还没提交，所以还可以找到
        let res = db.get("key1".into()).expect("key1 应该被找到");
        assert_eq!(res, Bytes::from("3"));

        // 事务提交后，key1 应该被删除
        wb.commit().expect("提交事务失败");

        let res = db.get("key1".into());
        // println!("{:?}", res);
        assert!(if let Err(Error::KeyNotFound(_)) = res {
            true
        } else {
            false
        });

        let res = db.get("key2".into()).expect("key2 应该被找到");
        assert_eq!(res, Bytes::from("2"));

        let seq_no = db.get_latest_seq_no();
        assert_eq!(seq_no, 4);

        drop(wb);
        drop(db);
    }
}
