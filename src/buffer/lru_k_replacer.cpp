//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
// 淘汰,将被驱逐的frameid通过指针传回去。返回成功驱逐与否
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  bool has_find = false;
  // 先从history_list中找。
  if (!history_list_.empty()) {
    for (auto &id : history_list_) {
      if (is_evictable_[id]) {
        has_find = true;
        *frame_id = id;
      }
    }
    if (has_find) {
      history_list_.erase(history_map_[*frame_id]);
      history_map_.erase(*frame_id);
      curr_size_--;
    }
  }
  // history中没有找到，到cachelist中找
  if (!has_find) {
    for (auto &id : cache_list_) {
      if (is_evictable_[id]) {
        has_find = true;
        *frame_id = id;
      }
    }
    if (has_find) {
      cache_list_.erase(cache_map_[*frame_id]);
      cache_map_.erase(*frame_id);
      curr_size_--;
    }
  }
  if (has_find) {
    // 从访问计数中移除
    use_count_.erase(*frame_id);
    is_evictable_.erase(*frame_id);
  }
  return has_find;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  use_count_[frame_id]++;
  if (use_count_[frame_id] == 1) {
    // 第一次访问
    is_evictable_[frame_id] = false;
  }
  if (use_count_[frame_id] == k_) {
    if (history_map_.count(frame_id) != 0U) {
      auto it = history_map_[frame_id];
      history_list_.erase(it);
    }
    history_map_.erase(frame_id);
    // 将frame_id加入cachelist；
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else if (use_count_[frame_id] > k_) {
    // 从原位置移除
    if (cache_map_.count(frame_id) != 0U) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
    // 加入链表头
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else {
    // <k_
    if (history_map_.count(frame_id) == 0U) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
    // history列表中相对位置不需要改变
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (use_count_.find(frame_id) == use_count_.end()) {
    return;
  }
  if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  } else if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  is_evictable_[frame_id] = set_evictable;
}

// 指定某一页驱逐
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // 做一个判断，判断被remove的项是否已经被驱逐
  // 查两个队列中是否有该frame
  if (history_map_.count(frame_id) != 0U || cache_map_.count(frame_id) != 0U) {
    return;
  }
  // 判断该frame是否为可驱逐的
  if (!is_evictable_[frame_id]) {
    return;
  }
  // 驱逐
  // 查history_map
  if (history_map_.count(frame_id) != 0U) {
    auto iter = history_map_[frame_id];
    history_list_.erase(iter);
    history_map_.erase(frame_id);
  } else if (cache_map_.count(frame_id) != 0U) {
    auto iter = cache_map_[frame_id];
    cache_list_.erase(iter);
    cache_map_.erase(frame_id);
  }
  use_count_.erase(frame_id);
  is_evictable_.erase(frame_id);
}

// size返回可以被驱逐的数量
auto LRUKReplacer::Size() -> size_t {
  // 加锁，防止在并发下有程序正在修改
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
