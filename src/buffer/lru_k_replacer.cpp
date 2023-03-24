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
LRUKReplacer::Node::Node(int frame_t) : frame_id_(frame_t) {}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames),
      k_(k),
      history_head_(new Node),
      history_tail_(new Node),
      cache_head_(new Node),
      cache_tail_(new Node) {
  history_head_->next_ = history_tail_;
  history_tail_->next_ = history_head_;
  history_head_->pre_ = history_tail_;
  history_tail_->pre_ = history_head_;

  cache_head_->next_ = cache_tail_;
  cache_tail_->next_ = cache_head_;
  cache_head_->pre_ = cache_tail_;
  cache_tail_->pre_ = cache_head_;
}
LRUKReplacer::~LRUKReplacer() {
  delete history_head_;
  delete history_tail_;
  delete cache_head_;
  delete cache_tail_;
  mp1_.clear();
  mp2_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (evict_size_ != 0) {
    if (!mp1_.empty()) {
      auto p = history_tail_->pre_;
      while (!p->evictable_ && p != history_head_) {
        p = p->pre_;
      }
      if (p->evictable_ && p != history_head_) {
        p->pre_->next_ = p->next_;
        p->next_->pre_ = p->pre_;
        if (frame_id != nullptr) {
          *frame_id = p->frame_id_;
        }
        mp1_.erase(p->frame_id_);
        evict_size_--;
        curr_size_--;
        return true;
      }
    }
    if (!mp2_.empty()) {
      auto p = cache_tail_->pre_;
      size_t m = UINT64_MAX;
      frame_id_t f = -1;
      while (p != cache_head_) {
        if (p->evictable_) {
          size_t k = p->timestamp_[p->timestamp_.size() - k_];
          if (m > k) {
            m = k;
            f = p->frame_id_;
          }
        }
        p = p->pre_;
      }
      if (f != -1) {
        p = mp2_[f].get();

        p->pre_->next_ = p->next_;
        p->next_->pre_ = p->pre_;
        if (frame_id != nullptr) {
          *frame_id = p->frame_id_;
        }
        mp2_.erase(f);
        evict_size_--;
        curr_size_--;
        return true;
      }
    }
  }
  if (frame_id != nullptr) {
    *frame_id = -1;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  latch_.lock();
  if (mp1_.find(frame_id) != mp1_.end()) {
    auto p = mp1_[frame_id];
    p->timestamp_.push_back(current_timestamp_++);
    if (p->timestamp_.size() >= k_) {
      p->pre_->next_ = p->next_;
      p->next_->pre_ = p->pre_;

      p->next_ = cache_head_->next_;
      p->pre_ = cache_head_;
      cache_head_->next_->pre_ = p.get();
      cache_head_->next_ = p.get();
      mp2_[frame_id] = p;
      mp1_.erase(frame_id);
    }

  } else if (mp2_.find(frame_id) != mp2_.end()) {
    auto p = mp2_[frame_id];
    p->timestamp_.push_back(current_timestamp_++);

  } else {
    if (curr_size_ < replacer_size_) {
      std::shared_ptr<Node> p(new Node(frame_id));

      p->timestamp_.push_back(current_timestamp_++);
      if (p->timestamp_.size() < k_) {
        p->next_ = history_head_->next_;
        p->pre_ = history_head_;
        history_head_->next_->pre_ = p.get();
        history_head_->next_ = p.get();

        mp1_[frame_id] = p;
      } else {
        p->next_ = cache_head_->next_;
        p->pre_ = cache_head_;
        cache_head_->next_->pre_ = p.get();
        cache_head_->next_ = p.get();
        mp2_[frame_id] = p;
      }
      curr_size_++;

    } else {
      latch_.unlock();
      if (Evict(nullptr)) {
        latch_.lock();
        std::shared_ptr<Node> p(new Node(frame_id));
        p->timestamp_.push_back(current_timestamp_++);
        if (p->timestamp_.size() < k_) {
          p->next_ = history_head_->next_;
          p->pre_ = history_head_;
          history_head_->next_->pre_ = p.get();
          history_head_->next_ = p.get();

          mp1_[frame_id] = p;
        } else {
          p->next_ = cache_head_->next_;
          p->pre_ = cache_head_;
          cache_head_->next_->pre_ = p.get();
          cache_head_->next_ = p.get();
          mp2_[frame_id] = p;
        }
        curr_size_++;
      }
    }
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  assert(frame_id >= 0);
  assert(static_cast<int>(frame_id) < static_cast<int>(replacer_size_));
  if (mp1_.find(frame_id) != mp1_.end()) {
    if (set_evictable && !mp1_[frame_id]->evictable_) {
      evict_size_++;
    } else if (!set_evictable && mp1_[frame_id]->evictable_) {
      evict_size_--;
    }
    mp1_[frame_id]->evictable_ = set_evictable;

  } else if (mp2_.find(frame_id) != mp2_.end()) {
    if (set_evictable && !mp2_[frame_id]->evictable_) {
      evict_size_++;
    } else if (!set_evictable && mp2_[frame_id]->evictable_) {
      evict_size_--;
    }
    mp2_[frame_id]->evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (mp1_.find(frame_id) != mp1_.end()) {
    auto p = mp1_[frame_id];
    if (p->evictable_) {
      p->pre_->next_ = p->next_;
      p->next_->pre_ = p->pre_;
      mp1_.erase(frame_id);
      evict_size_--;
      curr_size_--;
      return;
    }
    abort();

  } else if (mp2_.find(frame_id) != mp2_.end()) {
    auto p = mp2_[frame_id];
    if (p->evictable_) {
      p->pre_->next_ = p->next_;
      p->next_->pre_ = p->pre_;
      mp2_.erase(frame_id);
      evict_size_--;
      curr_size_--;
      return;
    }
    abort();
  }
}

auto LRUKReplacer::Size() -> size_t { return evict_size_; }

}  // namespace bustub
