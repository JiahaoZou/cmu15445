//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  auto bucket = std::make_shared<Bucket>(bucket_size_, 0);
  dir_.resize(2);
  dir_[0] = bucket;
  dir_[1] = bucket;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  //先从dir中找到bucket，再调用bucket的find方法。
  //需要加锁，按道理是加读锁即可，先直接加大锁
  std::lock_guard<std::mutex> guard(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  //加大锁
  std::lock_guard<std::mutex> guard(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  //加大锁
  std::lock_guard<std::mutex> guard(latch_);
  //桶满
  // indexof返回的是hash后的key所对应的下标，查找dir时需要转换后的key，而实际存在hash表中是原始key
  while (!dir_[IndexOf(key)]->Insert(key, value)) {
    int id = IndexOf(key);
    auto target_bucket = dir_[id];
    //局部深度等于全局深度，全局深度需要加1，即dir_扩大一倍
    // dir_扩容
    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      global_depth_++;
      int capacity = dir_.size();
      dir_.resize(capacity << 1);
      for (int idx = 0; idx < capacity; ++idx) {
        dir_[idx + capacity] = dir_[idx];
      }
    }
    //分裂
    //桶深度加1后的掩码
    int mask = 1 << target_bucket->GetDepth();
    //创建两个新bucket的shared_ptr
    auto zero_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    auto one_bucket = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    for (const auto &item : target_bucket->GetItems()) {
      //用c++标准库的hash函数将key做一次hash，生成固定格式的数据。
      //不一定非得用这个函数，也可以自己实现。
      size_t hashkey = std::hash<K>()(item.first);
      if ((hashkey & mask) != 0U) {
        one_bucket->Insert(item.first, item.second);
      } else {
        zero_bucket->Insert(item.first, item.second);
      }
    }
    //如果两个桶均非空，即两个桶均非满，则可以停止扩容，实际的bucket数量在此轮扩容中增加了1
    num_buckets_++;
    //注意：这里如果直接算下标会很麻烦，仔细体会，可以进行优化，但暂时先就这样
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = one_bucket;
        } else {
          dir_[i] = zero_bucket;
        }
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}
// bucket中的这些方法就不需要加锁了，因为在对目录进行操作时就已经加了锁
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &item : list_) {
    if (key == item.first) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto iter = list_.begin();
  bool flag = false;
  for (; iter != list_.end(); iter++) {
    if (iter->first == key) {
      flag = true;
      break;
    }
  }
  if (flag) {
    list_.erase(iter);
    return true;
  }
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //需要判断重复key
  for (auto &item : list_) {
    if (key == item.first) {
      item.second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.push_back({key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
