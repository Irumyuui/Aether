# aether

一个 kv 存储引擎，基于 [bitcask](https://github.com/basho/bitcask) 存储模型，使用 Rust 语言实现。

基于 bitcask 存储模型，内存中存储 `key-index` 索引，由 `index` 索引找到实际存储位置。

## 内存 index 数据结构

数据结构位置：``aether/src/memory_index/data_structures`

目前实现了下面的数据结构：

- B 树（来自 rust 标准库）
- 无锁跳表
- 跳表（crossbeam-skiplist）
- B+ 树 (jammdb)

## IO

加载文件的时候可以使用 `mmap` ，启动时候快一点，如果文件比较大的话，构建索引可能会比较慢。

每个数据都是可持久化的，添加每一条新记录都是追加写，不会覆盖已有数据，顺序读写的效率也很高。

## 整理

merge 操作，因为 bitcask 模型的特点，数据都是可持久化的，因此需要整理一下之前的无效数据。

## Rust

rust 写数据结构很头疼，约等于直接指针。

## Bug && Feature

- [ ] 提供一个封装的服务端
- [ ] 尝试融入协程（tokio）
- [ ] 替换事务从串行化为 MVCC
- [ ] 优化内存数据结构

文件读取的地方用到了 `read_at` ，查找了很多关于异步的文件 api ，只发现在 FreeBDS 还有 MacOS 系统上的 `tokio-fs` 中有，因此在思考到有无更好的 api 以及是否需要使用协程后，再思考是否重写一个。
