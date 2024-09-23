#![allow(unused_variables)]
#![allow(unused)]
#![allow(dead_code)]

use std::{
    mem::ManuallyDrop,
    ptr::{self, NonNull},
    sync::{
        atomic::{AtomicPtr, AtomicU8, AtomicUsize},
        Arc,
    },
};

use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::Ordering as AtomicOrdering;

use crate::{
    memory_index::{IndexIterator, Indexer},
    records::log_record::LogRecordIndex,
};

const MAX_HEIGHT: usize = 31;

type Ptr<T> = Option<NonNull<T>>;

pub struct LockFreeSkipList<K, V> {
    cur_height: AtomicU8,
    lanes: [AtomicPtr<Node<K, V>>; MAX_HEIGHT],
    size: AtomicUsize,
}

pub struct Node<K, V> {
    key: Option<K>,
    value: Option<V>,
    height: u8,
    next: Vec<AtomicPtr<Node<K, V>>>,
}

fn random_height() -> usize {
    const MASK: u32 = 1 << (MAX_HEIGHT - 1);
    1 + (rand::random::<u32>() | MASK).trailing_zeros() as usize
}

impl<K, V> Node<K, V> {
    fn alloc(k: K, v: V, max_height: &AtomicU8) -> NonNull<Node<K, V>> {
        let height = random_height();
        max_height.fetch_max(height as _, AtomicOrdering::SeqCst);

        let mut next = Vec::with_capacity(height);
        for _ in 0..height {
            next.push(AtomicPtr::default());
        }

        unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                key: Some(k),
                value: Some(v),
                height: height as _,
                next,
            })))
        }
    }

    fn dealloc(&mut self) -> Option<(K, V)> {
        let key = std::mem::replace(&mut self.key, None);
        let value = std::mem::replace(&mut self.value, None);

        let _defer = unsafe { Box::from_raw(self) };

        if value.is_some() {
            Some((key.unwrap(), value.unwrap()))
        } else {
            None
        }
    }

    fn lanes(&self) -> &[AtomicPtr<Node<K, V>>] {
        &self.next[..self.height()]
    }

    fn height(&self) -> usize {
        self.height as _
    }

    fn next(&self) -> Ptr<Node<K, V>> {
        NonNull::new(self.lanes().last().unwrap().load(AtomicOrdering::Acquire))
    }
}

impl<K, V> Drop for LockFreeSkipList<K, V> {
    fn drop(&mut self) {
        let mut first = self.lanes[MAX_HEIGHT - 1].load(AtomicOrdering::Acquire);
        while !first.is_null() {
            unsafe {
                let bake = (*first).next();
                (*first).dealloc();

                if bake.is_some() {
                    first = bake.unwrap().as_ptr();
                } else {
                    break;
                }
            }
        }
    }
}

impl<K, V> LockFreeSkipList<K, V>
where
    K: Ord,
{
    pub fn new() -> Self {
        Self {
            cur_height: AtomicU8::new(8),
            lanes: Default::default(),
            size: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.size.load(AtomicOrdering::Relaxed)
    }

    fn inner_find(&self, key: &K) -> Ptr<Node<K, V>> {
        let mut lanes = &self.lanes[..];
        let mut height = lanes.len();

        let mut node_ptr = NonNull::new(ptr::null_mut());

        'across: while height > 0 {
            'down: for ap in lanes.iter() {
                node_ptr = NonNull::new(ap.load(AtomicOrdering::Acquire));

                match node_ptr {
                    Some(ptr) => {
                        let node = unsafe { &*ptr.as_ptr() };

                        match key.cmp(&node.key.as_ref().unwrap()) {
                            CmpOrdering::Less => {
                                // 去下面看看
                                height -= 1;
                                continue 'down;
                            }

                            CmpOrdering::Equal => break 'across, // 就是这里

                            CmpOrdering::Greater => {
                                // 换一个位置
                                lanes = &node.lanes()[(node.height() - height)..];
                                continue 'across;
                            }
                        }
                    }

                    // 下去看看
                    None => {
                        height -= 1;
                        continue 'down;
                    }
                }
            }
        }

        node_ptr
    }

    pub fn range_get(&self, start_key: &K, end_key: &K) -> Vec<&V> {
        let mut node_ptr = self.inner_find(&start_key);
        let mut res = vec![];

        unsafe {
            while node_ptr.is_some()
                && node_ptr
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .key
                    .as_ref()
                    .unwrap()
                    .cmp(end_key)
                    .is_lt()
            {
                if let Some(v) = node_ptr.as_ref().unwrap().as_ref().value.as_ref() {
                    res.push(v);
                }

                node_ptr = node_ptr.as_ref().unwrap().as_ref().next();
            }
        }

        res
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        unsafe {
            match self.inner_find(key) {
                Some(node) if node.as_ref().key.as_ref().unwrap().cmp(key).is_eq() => {
                    match node.as_ref().value.as_ref() {
                        Some(ref_value) => Some(ref_value),
                        None => None,
                    }
                }
                _ => None,
            }
        }
    }

    pub fn range_delete(&self, start_key: &K, end_key: &K) -> Vec<V> {
        let mut node_ptr = self.inner_find(start_key);
        let mut res = vec![];

        unsafe {
            while node_ptr.is_some()
                && node_ptr
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .key
                    .as_ref()
                    .unwrap()
                    .cmp(end_key)
                    .is_lt()
            {
                let mut data =
                    std::mem::replace(&mut node_ptr.as_mut().unwrap().as_mut().value, None);
                if let Some(v) = data.take() {
                    res.push(v);
                }

                node_ptr = node_ptr.as_ref().unwrap().as_ref().next();
            }
        }

        self.size.fetch_sub(res.len(), AtomicOrdering::SeqCst);

        res
    }

    pub fn delete(&self, key: &K) -> Option<V> {
        let mut node_ptr = self.inner_find(key);
        unsafe {
            match node_ptr {
                Some(node) if node.as_ref().key.as_ref().unwrap().cmp(key).is_eq() => {
                    self.size.fetch_sub(1, AtomicOrdering::SeqCst);
                    std::mem::replace(&mut node_ptr.as_mut().unwrap().as_mut().value, None)
                }
                _ => None,
            }
        }
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let max_height = &self.cur_height;
        let mut data = ManuallyDrop::new((key, value));

        let mut new_node: Ptr<Node<K, V>> = None;

        'retry: loop {
            let mut lanes = &self.lanes[..];
            let mut height = lanes.len();
            let mut prevs_succs: [(*const AtomicPtr<Node<K, V>>, *mut Node<K, V>); MAX_HEIGHT] =
                [(ptr::null(), ptr::null_mut()); MAX_HEIGHT];

            'across: while height > 0 {
                'down: for ap in lanes.iter() {
                    let ptr = NonNull::new(ap.load(AtomicOrdering::Acquire));

                    match ptr {
                        // 本次链表的末尾了，继续向下找
                        None => {
                            height -= 1;
                            prevs_succs[height] = (ap, ptr::null_mut());
                            continue 'down;
                        }

                        // 还有节点，二分判断位置
                        Some(ptr) => unsafe {
                            let node = &mut *ptr.as_ptr();
                            let (ref_key, _ref_value) = &*data;

                            match ref_key.cmp(&node.key.as_ref().unwrap()) {
                                // 找到了，替换掉这个位置的 value
                                CmpOrdering::Equal => match &mut new_node {
                                    // 有就换掉
                                    Some(new_node) => {
                                        return Some(std::mem::replace(
                                            node.value.as_mut().unwrap(),
                                            new_node.as_mut().dealloc().unwrap().1,
                                        ))
                                    }

                                    // 没有就寄
                                    None => {
                                        if let Some(v) = node.value.as_mut() {
                                            return Some(std::mem::replace(
                                                v,
                                                ManuallyDrop::take(&mut data).1,
                                            ));
                                        } else {
                                            self.size.fetch_add(1, AtomicOrdering::SeqCst);
                                            return None;
                                        }
                                    }
                                },

                                // 当前节点小于 key
                                CmpOrdering::Less => {
                                    height -= 1;
                                    prevs_succs[height] = (ap, ptr.as_ptr());
                                    continue 'down;
                                }

                                // 大于key，那么继续往下找
                                CmpOrdering::Greater => {
                                    lanes = &node.lanes()[(node.height() - height)..];
                                    continue 'across;
                                }
                            }
                        },
                    }
                }
            }

            // 最底层的循环
            // 创建一个新节点
            let new_node: NonNull<Node<K, V>> = match new_node {
                Some(node) => node,
                None => {
                    let (key, value) = unsafe { ManuallyDrop::take(&mut data) };
                    let node = Node::alloc(key, value, max_height);
                    new_node = Some(node);
                    new_node.unwrap()
                }
            };

            let new_node_ptr = new_node.as_ptr();
            let new_node_lanes = unsafe { new_node.as_ref().lanes() };
            let mut inserted = false;

            'insert: for (new, &(pred, succ)) in new_node_lanes.iter().rev().zip(&prevs_succs) {
                let pred = unsafe { &*pred };
                new.store(succ, AtomicOrdering::Release);

                match pred
                    .compare_exchange(
                        succ,
                        new_node_ptr,
                        AtomicOrdering::Acquire,
                        AtomicOrdering::Acquire,
                    )
                    .is_ok()
                {
                    true => inserted = true,            // 成功插入
                    false if inserted => break 'insert, // 插入失败，最底层已经成功插入了
                    false => continue 'retry,           // 插入失败，没有被插入
                }
            }

            self.size.fetch_add(1, AtomicOrdering::SeqCst);

            return None;
        }
    }
}

pub struct LockFreeSkipListIndexer {
    skip_list: Arc<LockFreeSkipList<Vec<u8>, LogRecordIndex>>,
}

impl LockFreeSkipListIndexer {
    pub fn new() -> Self {
        Self {
            skip_list: Arc::new(LockFreeSkipList::<Vec<u8>, LogRecordIndex>::new()),
        }
    }
}

impl Indexer for LockFreeSkipListIndexer {
    fn put(&self, key: Vec<u8>, value: LogRecordIndex) -> Option<LogRecordIndex> {
        self.skip_list.insert(key, value)
    }

    fn get(&self, key: &Vec<u8>) -> Option<LogRecordIndex> {
        match self.skip_list.get(key) {
            Some(v) => Some(*v),
            None => None,
        }
    }

    fn delete(&self, key: &Vec<u8>) -> Option<LogRecordIndex> {
        self.skip_list.delete(key)
    }

    fn keys(&self) -> crate::errors::Result<Vec<bytes::Bytes>> {
        todo!()
    }

    fn iterator(
        &self,
        opts: crate::memory_index::IteratorOptions,
    ) -> Box<dyn crate::memory_index::IndexIterator> {
        todo!()
    }
}

pub struct SkipListIterator {}

impl IndexIterator for SkipListIterator {
    fn reset(&mut self) {
        todo!()
    }

    fn seek(&mut self, key: &Vec<u8>) {
        todo!()
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordIndex)> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, thread};

    use parking_lot::Mutex;
    use rand::Rng;

    use super::*;

    #[test]
    /// 单线程批量插入
    fn test_insert_batch() {
        // prepare
        let mut rng = rand::thread_rng();
        let mut std_map = BTreeMap::<i32, i32>::new();
        let our_map = Arc::new(LockFreeSkipList::<i32, i32>::new());

        // insert
        for key in 1..=10000 {
            let value = rng.gen_range(-10000..=10000);
            std_map.insert(key, value);
            our_map.insert(key, value);
        }

        // checkout
        for key in 1..=10000 {
            let want = std_map.get(&key);

            let len_0 = our_map.range_get(&key, &key);
            assert_eq!(len_0.len(), 0);

            let len_1 = our_map.range_get(&key, &(key + 1));
            assert_eq!(len_1.len(), 1);
            assert_eq!(len_1[0], want.unwrap());
        }
    }

    #[test]
    /// 测试重复插入
    fn test_insert_repeat() {
        let our_map = Arc::new(LockFreeSkipList::<i32, i32>::new());
        let (key, range_end) = (1, 3);
        let val_1 = 1;
        let val_2 = 2;

        our_map.insert(key, val_1);
        our_map.insert(key, val_2);

        let got = our_map.range_get(&key, &range_end);
        assert_eq!(got.len(), 1, "len is wrong");
        assert_eq!(*got[0], 2, "val isn't the last insert val");
    }

    #[test]
    /// 测试获取不存在的值
    fn test_get_non_exist() {
        let our_map = Arc::new(LockFreeSkipList::<i32, i32>::new());
        let (key, range_end) = (3, 7);
        let (key_left, val_left) = (1, 1);
        let (key_right, val_right) = (7, 7);

        our_map.insert(key_left, val_left);
        our_map.insert(key_right, val_right);

        // 原本应取得 3<=key<7 之间的集合
        // 但是此处不存在对应的取值
        // 因此 数组长度为 0
        let got = our_map.range_get(&key, &range_end);
        assert_eq!(got.len(), 0, "len is wrong")
    }

    #[test]
    /// 单线程批量删除
    fn test_delete_batch() {
        // 初始化
        let mut rng = rand::thread_rng();
        let mut std_map = BTreeMap::<i32, i32>::new();
        let our_map = Arc::new(LockFreeSkipList::<i32, i32>::new());

        // 批量插入10条数据
        for key in 1..=10 {
            std_map.insert(key, 10000 - key);
            our_map.insert(key, 10000 - key);
        }

        // 随机生成 9个用于被删除的 key
        let mut keys = Vec::<i32>::new();
        loop {
            let item = rng.gen_range(1..=10);
            if !keys.contains(&item) {
                keys.push(item);
            }
            if keys.len() == 9 {
                break;
            }
        }

        // 删除
        for key in keys {
            std_map.remove(&key);
            our_map.delete(&key);
        }

        // 未删除的key(仅剩一个)
        for key in std_map.keys() {
            let want = std_map.get(&key);
            let got = our_map.get(&key);
            assert_eq!(got.unwrap(), want.unwrap());
        }
    }

    #[test]
    /// 测试获取删除后的值
    fn test_get_after_delete() {
        let sk = Arc::new(LockFreeSkipList::<i32, i32>::new());
        let (key, val) = (3, 99);
        sk.insert(key, val);
        sk.delete(&key);
        let got = sk.get(&key);
        assert_eq!(got, None)
    }

    #[test]
    // hashmap 范围获取demo
    fn test_range_hashmap() {
        let mu = Mutex::new(BTreeMap::new());
        for i in 0..100 {
            mu.lock().insert(i, i);
        }
        unsafe {
            let v = {
                let mut v = vec![];
                let lo = mu.lock();
                for x in lo.range(0..100) {
                    v.push(
                        //NonNull::new
                        (x.1) as *const i32,
                    );
                }
                v
            };
            for x in v {
                print!("{},", (*x));
            }
        }
        println!();
    }

    // TODO 多个线程同时插入（每个线程key不冲突），验证结果
    // TODO 多个线程同时删除（每个线程key不冲突），验证结果
    // TODO 待补充

    #[test]
    fn single_thread() {
        let map = Arc::new(LockFreeSkipList::<i32, i32>::new());
        for i in 1..2 {
            let map_ = map.clone();
            thread::spawn(move || {
                for j in i * 1000..(i + 1) * 1000 {
                    map_.insert(j, j);
                }
                for j in i * 1000..(i + 1) * 1000 {
                    let v = map_.get(&j);
                    assert_eq!(*v.unwrap(), j);
                }
            });
        }
    }
    #[test]
    fn test_multithread() {
        let map = Arc::new(LockFreeSkipList::<i32, i32>::new());
        let mut v = vec![];
        for i in 0..28 {
            let map_ = map.clone();
            v.push(thread::spawn(move || {
                let time = 10000;
                for j in i * time..(i + 1) * time {
                    map_.insert(j, j);
                }
                for j in i * time..(i + 1) * time {
                    let v = map_.get(&j);
                    assert_eq!(*v.unwrap(), j);
                }
            }));
        }
        for u in v {
            u.join().unwrap();
        }
    }
}
