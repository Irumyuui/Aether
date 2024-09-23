use std::{
    collections::HashMap,
    fs::File,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
};

use bytes::Bytes;
use fs2::FileExt;
use parking_lot::{Mutex, RwLock};

use crate::{
    config::DBConfig,
    errors::{Error, Result},
    memory_index::{Indexer, IndexerType},
    records::{
        data_file::{DataFile, DATA_FILE_EXTENTION_NAME, MERGE_FIN_FILE_NAME, TS_SEQ_NO_FILE_NAME},
        log_record::{LogRecord, LogRecordIndex, LogRecordType},
    },
};

use super::{
    batch::{decord_log_key_with_ts, encord_log_key_with_ts},
    merge::load_merge_files,
};

use std::sync::atomic::Ordering as AtomicOrdering;

/// 最初的数据文件 ID
#[allow(unused)]
const BEGIN_DATA_FILE_ID: u32 = 0;

/// 记录事务编号的文件
const SEQ_NO_KEY: &str = "seq_no";

/// 文件上锁
pub(crate) const FILE_LOCK_NAME: &str = "db_file_lock_marker";

/// 数据库 存储引擎实例
///
/// 有基础的三个操作：put get delete
///
/// 使用样例
///
/// ```rust
/// use scopeguard::defer;
///
/// let db_path = std::env::temp_dir().join("aether_db");
/// let conf = aether::config::DBConfig::default().set_dir_path(db_path.clone());
/// let db = aether::database::DBEngine::new(conf).unwrap();
/// defer! {
///     let _ = std::fs::remove_dir_all(db_path);
/// }

/// db.put("key1".into(), "114514".into()).unwrap();
/// db.put("key2".into(), "1919810".into()).unwrap();

/// println!("key1 = {:?}", db.get("key1".into()));
/// println!("key2 = {:?}", db.get("key2".into()));
/// ```
///
pub struct DBEngine {
    /// 配置选项
    pub(crate) conf: Arc<DBConfig>,

    /// 当前活跃的文件
    pub(crate) active_file: Arc<RwLock<DataFile>>,

    /// 历史文件列表
    pub(crate) older_files: Arc<RwLock<HashMap<u32, DataFile>>>,

    /// 维护一份文件 id
    file_ids: Vec<u32>,

    /// 内存索引
    pub(crate) indexer: Box<dyn Indexer>,

    /// 事务相关
    pub(crate) batch_commit_lock: Mutex<()>,
    pub(crate) batch_commit_seq_no: Arc<AtomicUsize>,

    /// 只允许同时有一个后台线程进行 merge
    pub(crate) merge_lock: Mutex<()>,

    pub(crate) seq_file_exists: bool,
    pub(crate) is_initial: bool,

    pub(crate) is_closed: AtomicBool,

    lock_file: File,
    bytes_writen: Arc<AtomicUsize>,
}

impl DBEngine {
    /// 创建一个数据库实例
    pub fn new(conf: DBConfig) -> Result<Self> {
        check_config(&conf)?;

        let mut is_init = false;
        if !conf.dir_path.is_dir() {
            std::fs::create_dir_all(&conf.dir_path)
                .map_err(|e| Error::CreateDatabaseDirectoryFailed(e))?;
            is_init = true;
        } else if std::fs::read_dir(&conf.dir_path).unwrap().count() == 0 {
            is_init = true;
        }

        // 检查是否数据库目录被使用了
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(conf.dir_path.join(FILE_LOCK_NAME))
            .unwrap();
        // if let Err(e) = lock_file.try_lock_exclusive() {
        //     eprintln!(
        //         "database directory is using by other process, please check it: {}",
        //         e
        //     );
        //     return Err(Error::DatabaseDirIsUsing);
        // }

        lock_file
            .try_lock_exclusive()
            .inspect_err(|e| eprintln!("lock file error: {}", e))
            .map_err(|_| Error::DatabaseDirIsUsing)?;

        load_merge_files(&conf.dir_path)?;

        // data_files 按照文件 id 降序排序的
        let (file_ids, mut data_files) = load_data_files(&conf.dir_path, conf.mmap_start)?;

        // 最大的文件作为活跃文件
        let mut older_files = HashMap::new();
        while data_files.len() > 1 {
            let file = data_files.pop().unwrap();
            older_files.insert(file.get_file_id(), file);
        }

        let active_file = match data_files.pop() {
            Some(file) => file,
            None => DataFile::new(
                conf.dir_path.clone(),
                BEGIN_DATA_FILE_ID,
                crate::io::IOType::StandardIO,
            )?,
        };

        let indexer = conf.indexer_type.make_indexer(&conf.dir_path);

        let mut db = Self {
            conf: Arc::new(conf),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            file_ids,
            indexer: indexer,
            batch_commit_lock: Default::default(),
            batch_commit_seq_no: Arc::new(AtomicUsize::new(1)), // 0 表示没有事务
            merge_lock: Default::default(),
            is_initial: is_init,
            seq_file_exists: false,
            is_closed: AtomicBool::new(false),
            lock_file,
            bytes_writen: Default::default(),
        };

        if db.conf.indexer_type != IndexerType::JammDbBptree {
            // 构建索引
            db.load_index_from_hint_file()?;

            // 加载数据文件
            let prev_seq_no = db.init_index_from_data_file()?;
            if prev_seq_no > 0 {
                db.batch_commit_seq_no
                    .store(prev_seq_no + 1, std::sync::atomic::Ordering::SeqCst);
            }

            if db.conf.mmap_start {
                db.reset_io_type();
            }
        }
        if db.conf.indexer_type == IndexerType::JammDbBptree {
            let (ex, seq_no) = db.load_seq_no();
            if ex {
                db.batch_commit_seq_no
                    .store(seq_no, std::sync::atomic::Ordering::SeqCst);
                db.seq_file_exists = true;
            }

            let af = db.active_file.write();
            af.set_write_offset(af.file_size());
        }

        Ok(db)
    }

    /// 存储键值对
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.check_closed()?;

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        let key = key.into();

        let record = LogRecord::normal(encord_log_key_with_ts(&key, 0), value.clone().into());
        let log_record_pos = self.append_log_record(&record)?;

        // match self.indexer.put(key, log_record_pos) {
        //     Some(_) => Ok(()),
        //     None => Err(Error::IndexUpdateFailed),
        // }
        self.indexer.put(key, log_record_pos);

        Ok(())
    }

    /// 根据键获取值
    ///
    /// 如果键不存在，则返回 `KeyNotFound` 错误
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        self.check_closed()?;

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        let index = match self.indexer.get(&key.clone().into()) {
            Some(v) => v,
            None => return Err(Error::KeyNotFound(key.into())),
        };

        self.get_value_by_index(&index)
    }

    /// 删除键值对
    ///
    /// 该操作不会真的删除，而是标记为删除并追加添加到活跃文件中
    pub fn delete(&self, key: Bytes) -> Result<()> {
        self.check_closed()?;

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        let key = key.into();
        let _ = match self.indexer.get(&key) {
            Some(index) => index,
            None => return Ok(()),
        };

        let record = LogRecord::deleted(encord_log_key_with_ts(&key, 0));
        self.append_log_record(&record)?;

        // match self.indexer.delete(&key) {
        //     Some(_) => Ok(()),
        //     None => Err(Error::IndexUpdateFailed),
        // }

        self.indexer.delete(&key);
        Ok(())
    }

    /// 同步数据到磁盘，这会将当前的活跃文件写入
    pub fn sync(&self) -> Result<()> {
        self.check_closed()?;
        self.active_file.read().sync()
    }

    pub fn get_latest_seq_no(&self) -> usize {
        self.batch_commit_seq_no
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn close(&self) -> Result<()> {
        self.check_closed()?;

        if !self.conf.dir_path.is_dir() {
            return Ok(());
        }

        let seq_no_file = DataFile::open_seq_no_file(&self.conf.dir_path)?;
        let seq_no = self
            .batch_commit_seq_no
            .load(std::sync::atomic::Ordering::SeqCst);
        let record = LogRecord::normal(SEQ_NO_KEY.as_bytes().to_vec(), seq_no.to_string().into());
        seq_no_file.write(&record.encode())?;
        seq_no_file.sync()?;

        self.sync()?;

        self.lock_file
            .unlock()
            .map_err(|e| eprintln!("Error unlock file: {}", e))
            .expect("unlock file failed");

        self.is_closed
            .store(true, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    fn check_closed(&self) -> Result<()> {
        if self.is_closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::DatabaseClosed);
        }
        Ok(())
    }

    fn load_seq_no(&self) -> (bool, usize) {
        let file_name = self.conf.dir_path.join(TS_SEQ_NO_FILE_NAME);
        if !file_name.is_file() {
            return (false, 0);
        }

        let seq_no_file = DataFile::open_seq_no_file(&self.conf.dir_path).unwrap();
        let rec = match seq_no_file.read_log_record(0) {
            Ok(res) => res.0,
            Err(e) => panic!("load seq_no file failed: {}", e),
        };

        let seq_no = String::from_utf8(rec.value)
            .unwrap()
            .parse::<usize>()
            .unwrap();

        std::fs::remove_file(file_name).unwrap();

        (true, seq_no)
    }

    fn reset_io_type(&self) {
        let mut active_file = self.active_file.write();
        active_file.set_io(&self.conf.dir_path, crate::io::IOType::StandardIO);

        self.older_files
            .write()
            .iter_mut()
            .for_each(|(_, f)| f.set_io(&self.conf.dir_path, crate::io::IOType::Mmap));
    }
}

impl Drop for DBEngine {
    /// 在 drop 时将同步数据到磁盘，忽略掉发生的错误
    fn drop(&mut self) {
        let _ = self
            .close()
            .inspect_err(|e| eprintln!("close db failed: {}", e));
    }
}

impl DBEngine {
    /// 追加日志记录
    pub(crate) fn append_log_record(&self, log_record: &LogRecord) -> Result<LogRecordIndex> {
        let dir_path = &self.conf.dir_path;

        let encord_record = log_record.encode();
        let record_len = encord_record.len() as u64;

        let mut active_file = self.active_file.write();

        // 如果活跃文件大小已经超过设置上限，那么重新打开
        if active_file.get_write_offset() + record_len > self.conf.data_file_size {
            active_file.sync()?;

            let current_file_id = active_file.get_file_id();
            let old_file = DataFile::new(
                dir_path.clone(),
                current_file_id,
                crate::io::IOType::StandardIO,
            )?;
            self.older_files.write().insert(current_file_id, old_file);

            let new_file = DataFile::new(
                dir_path.clone(),
                current_file_id + 1,
                crate::io::IOType::StandardIO,
            )?;
            *active_file = new_file;
        }

        let write_offset = active_file.get_write_offset();
        active_file.write(&encord_record)?;

        // if self.conf.sync_writes {
        //     active_file.sync()?;
        // }

        let previos = self
            .bytes_writen
            .fetch_add(encord_record.len(), AtomicOrdering::SeqCst);
        let need_sync = self.conf.sync_writes
            || (self.conf.byte_per_sync > 0
                && previos + encord_record.len() >= self.conf.byte_per_sync);

        if need_sync {
            active_file.sync()?;
            self.bytes_writen.store(0, AtomicOrdering::SeqCst);
        }

        Ok(LogRecordIndex::new(active_file.get_file_id(), write_offset))
    }

    /// 遍历 file id 来构建 index
    fn init_index_from_data_file(&self) -> Result<usize> {
        // 没有文件，事务序列号为 0
        if self.file_ids.is_empty() {
            return Ok(0);
        }

        // 合并 merge 问题
        // 先找一下最后没有参与 merge 的文件 id
        let mut non_merge_file_id = None;
        let merge_finish_file_name = self.conf.dir_path.join(MERGE_FIN_FILE_NAME);
        if merge_finish_file_name.is_file() {
            let (fin_rec, _) =
                DataFile::open_merge_file(&self.conf.dir_path)?.read_log_record(0)?;
            non_merge_file_id = Some(
                String::from_utf8(fin_rec.value)
                    .unwrap()
                    .parse::<u32>()
                    .unwrap(),
            );
        }

        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        let mut ts_records: HashMap<usize, Vec<(LogRecord, LogRecordIndex)>> = HashMap::new(); // 事务的 record 记录
        let mut offset = 0;
        let mut cur_seq_no = 0;

        for &file_id in self.file_ids.iter() {
            // 小于还没 merge 的文件 id ，直接跳过
            if let Some(non_merge_id) = non_merge_file_id {
                if file_id < non_merge_id {
                    continue;
                }
            }

            offset = 0;
            loop {
                // 如果是在当前活跃文件中，则从当前文件中读取
                // 否则从历史文件中读取
                let read_result = match file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => older_files.get(&file_id).unwrap().read_log_record(offset),
                };

                let (mut log_record, read_size) = match read_result {
                    Ok(res) => (res.0, res.1),
                    Err(e) => match e {
                        Error::EndOfFile => break,
                        _ => return Err(e),
                    },
                };

                // defer! {
                //     offset += read_size as u64;
                // }

                let index = LogRecordIndex::new(file_id, offset);
                let (dec_key, seq_no) = decord_log_key_with_ts(&log_record.key);

                // 没带事务的
                if seq_no == 0 {
                    self.update_index(dec_key, index, log_record.rec_type);
                    offset += read_size as u64;
                    continue;
                }

                // 处理事务
                if log_record.rec_type == LogRecordType::TransactionEnd {
                    let mut recs = ts_records.remove(&seq_no).unwrap();
                    for (ts_rec, ts_idx) in recs.drain(..) {
                        self.update_index(ts_rec.key, ts_idx, ts_rec.rec_type);
                    }
                } else {
                    log_record.key = dec_key;
                    ts_records
                        .entry(seq_no)
                        .or_insert(Default::default())
                        .push((log_record, index));
                }

                cur_seq_no = cur_seq_no.max(seq_no);
                offset += read_size as u64;
            }
        }

        // println!("{:?}", cur_seq_no);

        active_file.set_write_offset(offset);
        Ok(cur_seq_no)
    }

    fn update_index(
        &self,
        key: Vec<u8>,
        index: LogRecordIndex,
        rec_type: LogRecordType,
    ) -> Option<LogRecordIndex> {
        match rec_type {
            LogRecordType::Normal => self.indexer.put(key, index),
            LogRecordType::Deleted => self.indexer.delete(&key),
            LogRecordType::TransactionEnd => unreachable!(),
        }
    }

    /// 根据索引获取值
    pub(crate) fn get_value_by_index(&self, index: &LogRecordIndex) -> Result<Bytes> {
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        let (log_record, _) = match active_file.get_file_id() == index.get_file_id() {
            true => active_file.read_log_record(index.get_offset())?,
            false => match older_files.get(&index.get_file_id()) {
                Some(data_file) => data_file.read_log_record(index.get_offset())?,
                None => return Err(Error::DataFileNotFound(index.get_file_id().to_string())),
            },
        };

        match log_record.rec_type {
            LogRecordType::Deleted => Err(Error::KeyNotFound(log_record.key.into())),
            _ => Ok(log_record.value.into()),
        }
    }
}

/// 检查 db config 是否正确
fn check_config(conf: &DBConfig) -> Result<()> {
    let dir_path = conf.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().is_empty() {
        return Err(Error::InvalidConfig("dir_path cannot be empty".to_string()));
    }

    if conf.data_file_size == 0 {
        return Err(Error::InvalidConfig(
            "data_file_size cannot be zero".to_string(),
        ));
    }

    Ok(())
}

/// 加载数据文件
///
/// 返回当前活跃文件 ID 以及所有历史文件列表
///
/// 注意：历史文件列表中，文件 ID 按降序排序
fn load_data_files(
    dir_path: impl AsRef<std::path::Path> + Clone,
    mmap_start: bool,
) -> Result<(Vec<u32>, Vec<DataFile>)> {
    let dir = match std::fs::read_dir(&dir_path) {
        Ok(dir) => dir,
        Err(e) => return Err(Error::ReadDatabaseDirectoryFailed(e)),
    };

    let mut file_ids: Vec<u32> = Vec::with_capacity(16);
    for file in dir.into_iter() {
        if let Ok(file) = file {
            if let Some(file_id_str) = file
                .file_name()
                .to_str()
                .unwrap()
                .strip_suffix(DATA_FILE_EXTENTION_NAME)
            {
                let file_id = match file_id_str.parse::<u32>() {
                    Ok(value) => value,
                    Err(e) => return Err(Error::DataDirectoryCorrupted(e.into())),
                };
                file_ids.push(file_id);
            }
        }
    }

    file_ids.sort();

    let io_type = match mmap_start {
        true => crate::io::IOType::Mmap,
        false => crate::io::IOType::StandardIO,
    };

    let data_files: Vec<DataFile> = file_ids
        .iter()
        .rev()
        .map(|fid| DataFile::new(&dir_path, *fid, io_type))
        .collect::<Result<_>>()?;

    Ok((file_ids, data_files))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use scopeguard::defer;

    use crate::{config::DBConfig, errors::Error};

    use super::DBEngine;

    /// 测试单独放入
    ///
    /// 期望返回正确的数据
    #[test]
    fn test_one_put() {
        let path = std::env::temp_dir().join("test_one_put");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 单条数据
        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        assert_eq!(
            db.get(Bytes::from("1")).expect("数据读取失败"),
            Bytes::from("1")
        );
    }

    /// 测试重复数据多次插入
    ///
    /// 期望返回正确的数据
    #[test]
    fn test_multi_put() {
        let path = std::env::temp_dir().join("test_multi_put");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 多条数据
        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        db.put(Bytes::from("2"), Bytes::from("2"))
            .expect("数据写入失败");
        db.put(Bytes::from("1"), Bytes::from("3"))
            .expect("数据写入失败");
        assert_eq!(
            db.get(Bytes::from("1")).expect("数据读取失败"),
            Bytes::from("3")
        );
        assert_eq!(
            db.get(Bytes::from("2")).expect("数据读取失败"),
            Bytes::from("2")
        );
    }

    /// 测试插入 key 为空
    ///
    /// 期望返回 EmptyKey 错误
    #[test]
    fn test_empty_key() {
        let path = std::env::temp_dir().join("test_empty_key");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 空 key
        let res = db.put(Bytes::from(""), Bytes::from("1"));
        assert!(if let Err(Error::EmptyKey) = res {
            true
        } else {
            false
        });
    }

    /// 测试value为空
    ///
    /// 期望返回正确的数据
    #[test]
    fn test_empty_value() {
        let path = std::env::temp_dir().join("test_empty_value");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 空 value
        db.put(Bytes::from("1"), Bytes::from(""))
            .expect("数据写入失败");
        assert_eq!(
            db.get(Bytes::from("1")).expect("数据读取失败"),
            Bytes::from("")
        );
    }

    /// 测试重新启动数据库之后再 put 数据
    ///
    /// 期望返回正确的数据
    #[test]
    fn test_restart_put() {
        let path = std::env::temp_dir().join("test_restart_put");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        let db = DBEngine::new(conf.clone()).expect("数据库打开失败");

        // 单条数据
        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        assert_eq!(
            db.get(Bytes::from("1")).expect("数据读取失败"),
            Bytes::from("1")
        );

        drop(db);

        // 重新打开数据库
        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 再次写入数据
        db.put(Bytes::from("2"), Bytes::from("2"))
            .expect("数据写入失败");
        assert_eq!(
            db.get(Bytes::from("2")).expect("数据读取失败"),
            Bytes::from("2")
        );
    }

    /// 测试正常读取一条数据
    ///
    /// 期望返回正确的数据
    #[test]
    fn test_one_get() {
        let path = std::env::temp_dir().join("test_one_get");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 单条数据
        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        assert_eq!(
            db.get(Bytes::from("1")).expect("数据读取失败"),
            Bytes::from("1")
        );
    }

    /// 测试从数据库中读取一个不存在的 key
    ///
    /// 期望返回 KeyNotFound 错误
    #[test]
    fn test_not_found_key() {
        let path = std::env::temp_dir().join("test_not_found_key");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 单条数据
        let res = db.get(Bytes::from("1"));
        assert!(if let Err(Error::KeyNotFound(_)) = res {
            true
        } else {
            false
        });
    }

    /// 测试多次向数据库 put 同一个 key 的不同 value，然后读取
    ///
    /// 期望返回最后一次写入的值
    #[test]
    fn test_multi_get() {
        let path = std::env::temp_dir().join("test_multi_get");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 多条数据
        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        db.put(Bytes::from("1"), Bytes::from("2"))
            .expect("数据写入失败");
        db.put(Bytes::from("1"), Bytes::from("3"))
            .expect("数据写入失败");
        assert_eq!(
            db.get(Bytes::from("1")).expect("数据读取失败"),
            Bytes::from("3")
        );
    }

    /// 测试读取一个被删除的 key
    ///
    /// 期望返回 KeyNotFound 错误
    #[test]
    fn test_deleted_key() {
        let path = std::env::temp_dir().join("test_deleted_key");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 单条数据
        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        db.delete(Bytes::from("1")).expect("数据删除失败");
        let res = db.get(Bytes::from("1"));
        assert!(if let Err(Error::KeyNotFound(_)) = res {
            true
        } else {
            false
        });
    }

    /// 从旧数据文件上读取到 value
    #[test]
    fn test_read_from_old_files() {
        let path = std::env::temp_dir().join("test_read_from_old_files");
        let conf = DBConfig::default()
            .set_dir_path(path.clone())
            .set_data_file_size(1024 * 128);
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        let db = DBEngine::new(conf.clone()).expect("数据库打开失败");

        // 写入数据
        let n = 1000000;
        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for i in 0..n {
            let key = Bytes::from(format!("key-{}", i));
            let value = Bytes::from(format!("value-{}", i));
            db.put(key.clone(), value.clone()).expect("数据写入失败");
            keys.push(key);
            values.push(value);
        }

        for i in 0..n {
            assert_eq!(db.get(keys[i].clone()).expect("数据读取失败"), values[i]);
        }
    }

    /// 测试删除一条数据
    ///
    /// 期望返回正确的数据
    #[test]
    fn test_delete_one() {
        let path = std::env::temp_dir().join("test_delete_one");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 单条数据
        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        db.delete(Bytes::from("1")).expect("数据删除失败");
        let res = db.get(Bytes::from("1"));
        assert!(if let Err(Error::KeyNotFound(_)) = res {
            true
        } else {
            false
        });
    }

    /// 测试删除多条数据
    ///
    /// 期望返回正确的数据
    #[test]
    fn test_delete_multi() {
        let path = std::env::temp_dir().join("test_delete_multi");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        let db = DBEngine::new(conf).expect("数据库打开失败");

        // 多条数据
        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        db.put(Bytes::from("2"), Bytes::from("2"))
            .expect("数据写入失败");
        db.put(Bytes::from("3"), Bytes::from("3"))
            .expect("数据写入失败");
        db.delete(Bytes::from("2")).expect("数据删除失败");
        let res = db.get(Bytes::from("2"));
        assert!(if let Err(Error::KeyNotFound(_)) = res {
            true
        } else {
            false
        });
        assert_eq!(
            db.get(Bytes::from("1")).expect("数据读取失败"),
            Bytes::from("1")
        );
        assert_eq!(
            db.get(Bytes::from("3")).expect("数据读取失败"),
            Bytes::from("3")
        );
    }

    /// 测试删除一个不存在的 key
    ///
    /// 期望返回 KeyNotFound 错误
    #[test]
    fn test_delete_empty_key() {
        let path = std::env::temp_dir().join("test_delete_empty_key");
        let conf = DBConfig::default().set_dir_path(path.clone());
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        let db = DBEngine::new(conf).expect("数据库打开失败");
        let res = db.delete(Bytes::from(""));
        assert!(if let Err(Error::EmptyKey) = res {
            true
        } else {
            false
        });
    }

    /// 删除历史数据文件中的值，再读取
    #[test]
    fn test_delete_from_history_files() {
        let path = std::env::temp_dir().join("test_delete_from_history_files");
        let conf = DBConfig::default()
            .set_dir_path(path.clone())
            .set_data_file_size(1024 * 1024);
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }
        let db = DBEngine::new(conf.clone()).expect("数据库打开失败");

        // 写入数据
        let n = 1000000;
        let mut keys = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        for i in 0..n {
            let key = Bytes::from(format!("key-{}", i));
            let value = Bytes::from(format!("value-{}", i));
            db.put(key.clone(), value.clone()).expect("数据写入失败");
            keys.push(key);
            values.push(value);
        }

        // 删除数据
        for i in 0..n {
            db.delete(keys[i].clone()).expect("数据删除失败");
        }

        // 再次写入数据
        for i in 0..n {
            let key = Bytes::from(format!("key-{}", i));
            let value = Bytes::from(format!("value-{}", i));
            db.put(key.clone(), value.clone()).expect("数据写入失败");
        }

        // 读取数据
        for i in 0..n {
            assert_eq!(db.get(keys[i].clone()).expect("数据读取失败"), values[i]);
        }
    }

    /// 测试持久化
    #[test]
    fn test_db_sync() {
        let path = std::env::temp_dir().join("test_db_sync");
        let db =
            DBEngine::new(DBConfig::default().set_dir_path(path.clone())).expect("数据库打开失败");

        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        db.put(Bytes::from("1"), Bytes::from("1"))
            .expect("数据写入失败");
        assert!(db.sync().is_ok());
    }

    #[test]
    #[should_panic]
    fn test_db_file_lock() {
        let test_path = std::env::temp_dir().join("test_db_file_lock");
        let path = test_path.clone();
        defer! {
            let _ = std::fs::remove_dir(test_path);
        }

        let db1 = DBConfig::default()
            .set_dir_path(path.clone())
            .build()
            .unwrap();
        let db2 = DBConfig::default()
            .set_dir_path(path.clone())
            .build()
            .unwrap();

        drop(db1);
        drop(db2);
    }
}
