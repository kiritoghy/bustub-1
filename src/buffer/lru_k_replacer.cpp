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

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }

  for (auto iter = fifo_.begin(); iter != fifo_.end(); iter++) {
    if ((*iter)->evictable_) {
      *frame_id = (*iter)->frame_id_;
      items_.erase(*frame_id);
      fifo_.erase(iter);
      curr_size_--;
      return true;
    }
  }

  for (auto iter = k_distance_.begin(); iter != k_distance_.end(); iter++) {
    if ((*iter)->evictable_) {
      *frame_id = (*iter)->frame_id_;
      items_.erase(*frame_id);
      k_distance_.erase(iter);
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  current_timestamp_++;
  auto iter = items_.find(frame_id);

  // if frame_id is the first time add to replace
  if (iter == items_.end()) {
    auto new_item = std::make_shared<Item>(frame_id, current_timestamp_, false);
    items_.insert(std::make_pair(frame_id, new_item));
    if (new_item->access_count_ >= k_) {
      k_distance_.push_back(new_item);
    } else {
      fifo_.push_back(new_item);
    }
    return;
  }

  auto item = iter->second;
  item->access_count_++;
  item->access_time_ = current_timestamp_;
  if (item->access_count_ < k_) {
    // do nothing
  } else if (item->access_count_ == k_) {
    fifo_.remove(item);
    k_distance_.push_back(item);
  } else {
    k_distance_.remove(item);
    k_distance_.push_back(item);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = items_.find(frame_id);
  if (iter == items_.end()) {
    return;
  }
  // TODO(tyb)
  // BUSTUB_ENSURE(iter != items_.end(), "# Invalid frame_id");
  auto previous_evictable = iter->second->evictable_;
  iter->second->evictable_ = set_evictable;
  if (previous_evictable && !set_evictable) {
    curr_size_--;
  }
  if (!previous_evictable && set_evictable) {
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = items_.find(frame_id);
  if (iter == items_.end()) {
    return;
  }

  auto item = iter->second;
  BUSTUB_ENSURE(item->evictable_, "# Remove a non_evictable frame");
  if (item->access_count_ >= k_) {
    k_distance_.remove(item);
  } else {
    fifo_.remove(item);
  }
  items_.erase(iter);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
