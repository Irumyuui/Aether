use crate::{
    config::DBConfig,
    database::batch::{decord_log_key_with_ts, encord_log_key_with_ts},
    errors::{Error, Result},
    records::{
        data_file::{
            get_format_file_name, DataFile, HINT_FILE_NAME, MERGE_FIN_FILE_NAME,
            TS_SEQ_NO_FILE_NAME,
        },
        log_record::{decode_to_log_index, LogRecord},
    },
};

use crate::database::db::DBEngine;

use super::FILE_LOCK_NAME;

pub const MERGE_DIR_NAME: &str = "db_merge";
pub const MERGE_FIN_KEY: &[u8] = b"db_m_fin";

fn get_merge_path<P: AsRef<std::path::Path>>(dir_path: P) -> std::path::PathBuf {
    dir_path.as_ref().join(MERGE_DIR_NAME)
}

pub(crate) fn load_merge_files<P: AsRef<std::path::Path>>(dir_path: P) -> Result<()> {
    let merge_path = get_merge_path(&dir_path);
    // 如果不存在merge目录，那么也没 merge 过，跑了
    if !merge_path.is_dir() {
        return Ok(());
    }

    let dir = match std::fs::read_dir(&merge_path) {
        Ok(dir) => dir,
        Err(e) => return Err(Error::ReadDatabaseDirectoryFailed(e)),
    };

    // 查找 merge 完成标志文件
    let mut merge_file_names = vec![];
    let mut is_merge_finished = false;
    for file_res in dir {
        if let Ok(entry) = file_res {
            let file_name = entry.file_name();
            let file_name = file_name.to_str().unwrap();
            if file_name.ends_with(MERGE_FIN_FILE_NAME) {
                is_merge_finished = true;
            }

            // 不加载事务编号文件
            if file_name.ends_with(TS_SEQ_NO_FILE_NAME) || file_name.ends_with(FILE_LOCK_NAME) {
                continue;
            }
            merge_file_names.push(entry.file_name());
        }
    }

    // 如果没有完成 merge，之前的 MERGE 不算，直接丢掉
    if !is_merge_finished {
        let _ = std::fs::remove_dir_all(merge_path);
        return Ok(());
    }

    // 准备 merge
    // 找到所有要 merge 的文件，然后取出其中还没有 merge 的文件 id ，然后 merge 到主文件中
    let merge_finish_file = DataFile::open_merge_file(&merge_path)?;
    let merge_finish_record = merge_finish_file.read_log_record(0)?;
    let non_merge_file_id = String::from_utf8(merge_finish_record.0.value)
        .unwrap()
        .parse::<u32>()
        .unwrap();

    // 删除旧的数据文件
    (0..non_merge_file_id)
        .map(|file_id| get_format_file_name(&dir_path, file_id))
        .filter(|f| f.is_file())
        .for_each(|f| {
            let _ = std::fs::remove_file(f);
        });

    // 合并的数据文件转移到数据目录中
    merge_file_names.into_iter().for_each(|name| {
        let src = merge_path.join(name.clone());
        let dest = dir_path.as_ref().join(name);
        let _ = std::fs::rename(src, dest);
    });

    let _ = std::fs::remove_dir_all(merge_path);
    Ok(())
}

impl DBEngine {
    pub fn merge(&self) -> Result<()> {
        // 加锁，防止 merge 冲突
        let try_lock_res = self.merge_lock.try_lock();
        if try_lock_res.is_none() {
            return Err(Error::MergeInProgress);
        }

        // 准备 merge ，先创建 merge 目录
        let merge_path = get_merge_path(&self.conf.dir_path);
        if merge_path.is_dir() {
            std::fs::remove_dir_all(&merge_path).expect("?");
        }
        if let Err(e) = std::fs::create_dir_all(&merge_path) {
            return Err(Error::CreateDatabaseDirectoryFailed(e));
        }

        // 先拿旧文件的 id 列表，然后写到一个新的文件中
        let mut merge_file_ids = vec![];
        let mut older_files = self.older_files.write();
        older_files
            .keys()
            .for_each(|file_id| merge_file_ids.push(*file_id));

        let mut active_file = self.active_file.write();
        active_file.sync()?;
        let actives_file_id = active_file.get_file_id();
        *active_file = DataFile::new(
            &self.conf.dir_path,
            actives_file_id + 1,
            crate::io::IOType::StandardIO,
        )?;

        older_files.insert(
            actives_file_id,
            DataFile::new(
                &self.conf.dir_path,
                actives_file_id,
                crate::io::IOType::StandardIO,
            )?,
        );
        merge_file_ids.push(actives_file_id);
        merge_file_ids.sort();

        // 打开所有需要 merge 的文件
        let merge_files = merge_file_ids
            .into_iter()
            .map(|file_id| {
                DataFile::new(&self.conf.dir_path, file_id, crate::io::IOType::StandardIO)
            })
            .collect::<Result<Vec<DataFile>>>()?;

        let merge_db = DBConfig::default()
            .set_dir_path(merge_path.clone())
            .set_data_file_size(self.conf.data_file_size)
            .build()?;

        let hint_file = DataFile::open_hint_file(&merge_path)?;
        let non_merge_file_id = merge_files.last().unwrap().get_file_id() + 1;

        // 重新写一遍
        merge_files.into_iter().try_for_each(|f| {
            let mut offset = 0;
            loop {
                let (mut rec, size) = match f.read_log_record(offset) {
                    Ok((rec, size)) => (rec, size),
                    Err(e) => {
                        if let Error::EndOfFile = e {
                            break;
                        }
                        return Err(e);
                    }
                };

                let (key, _) = decord_log_key_with_ts(&rec.key);
                if let Some(index) = self.indexer.get(&key) {
                    if index.get_file_id() == f.get_file_id() && index.get_offset() == offset {
                        rec.key = encord_log_key_with_ts(&key, 0);
                        let index = merge_db.append_log_record(&rec)?;
                        hint_file.write_hint_record(key, index)?;
                    }
                }

                offset += size as u64;
            }

            Ok(())
        })?;

        merge_db.sync()?;
        hint_file.sync()?;

        // 找到最近的，没有参与 merge 的文件 id ，然后写到 merge 完成文件中
        let merge_finish_file = DataFile::open_merge_file(&merge_path)?;
        let merge_finish_rec =
            LogRecord::normal(MERGE_FIN_KEY.into(), non_merge_file_id.to_string().into());
        merge_finish_file.write(&merge_finish_rec.encode())?;
        merge_finish_file.sync()?;

        Ok(())
    }

    //
    // fn rotate_merge_files(&self) -> Result<Vec<DataFile>> {
    //     todo!()
    // }

    // 加载 hint 文件中的索引
    pub(crate) fn load_index_from_hint_file(&self) -> Result<()> {
        let hint_file_name = self.conf.dir_path.join(HINT_FILE_NAME);
        if !hint_file_name.is_file() {
            return Ok(());
        }

        let hint_file = DataFile::open_hint_file(&self.conf.dir_path)?;
        let mut offset = 0;
        loop {
            let (rec, read_size) = match hint_file.read_log_record(offset) {
                Ok((rec, size)) => (rec, size),
                Err(e) => {
                    if let Error::EndOfFile = e {
                        break;
                    }
                    return Err(e);
                }
            };

            let index = decode_to_log_index(&rec.value);
            self.indexer.put(rec.key, index);
            offset += read_size as u64;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use scopeguard::defer;

    #[test]
    fn test_empty_merge() {
        let path = std::env::temp_dir().join("test_empty_merge");
        let db = crate::config::DBConfig::default()
            .set_dir_path(path.clone())
            .build()
            .unwrap();
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        db.merge().unwrap();
    }

    #[test]
    fn test_one_data_to_merge() {
        let path = std::env::temp_dir().join("test_one_data_to_merge");

        let conf = crate::config::DBConfig::default().set_dir_path(path.clone());

        let db = conf.clone().build().unwrap();
        defer! {
            let _ = std::fs::remove_dir_all(path);
        }

        let key = Bytes::from("first-key");
        let value = Bytes::from("first-value");
        assert!(db.put(key.clone(), value.clone()).is_ok());
        assert_eq!(db.get(key.clone()).unwrap(), value);

        db.merge().expect("merge failed");
        drop(db);

        let db = conf.build().unwrap();
        assert_eq!(db.get(key.clone()).unwrap(), value);
    }
}
