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

  for (auto itor = history_.begin(); itor != history_.end(); ++itor) {
    if ((*itor)->is_evictable_) {
      *frame_id = (*itor)->frame_id_;
      frames_.erase(*frame_id);
      history_.erase(itor);
      curr_size_--;
      return true;
    }
  }

  for (auto itor = cache_.begin(); itor != history_.end(); ++itor) {
    if ((*itor)->is_evictable_) {
      *frame_id = (*itor)->frame_id_;
      frames_.erase(*frame_id);
      cache_.erase(itor);
      curr_size_--;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT(frame_id <= (frame_id_t)replacer_size_, "frame id is invalid");
  auto kv = frames_.find(frame_id);
  current_timestamp_++;
  // not found a frame
  if (kv == frames_.end()) {
    auto frame_meta = std::make_shared<FrameMeta>(frame_id, current_timestamp_, false);
    frames_.insert(std::make_pair(frame_id, frame_meta));
    if (frame_meta->cur_ >= k_) {
      cache_.push_back(frame_meta);
    } else {
      history_.push_back(frame_meta);
    }
    return;
  }

  auto frame_ptr = kv->second;
  frame_ptr->timestamps_ = current_timestamp_;
  frame_ptr->cur_++;

  // now access times reach k, move it in cache
  if (frame_ptr->cur_ == k_) {
    history_.remove(frame_ptr);
    cache_.push_back(frame_ptr);
  } else if (frame_ptr->cur_ > k_) {
    // lru rule
    cache_.remove(frame_ptr);
    cache_.push_back(frame_ptr);
  }
  // frame access times < k, FIFO rule
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto kv = frames_.find(frame_id);
  if (kv != frames_.end()) {
    auto frame_ptr = kv->second;
    if (frame_ptr->is_evictable_ && !set_evictable) {
      // set evictable to false
      curr_size_--;
      frame_ptr->is_evictable_ = set_evictable;
    } else if (!frame_ptr->is_evictable_ && set_evictable) {
      ++curr_size_;
      frame_ptr->is_evictable_ = set_evictable;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT(frame_id <= (frame_id_t)replacer_size_, "frame id is invalid");
  auto kv = frames_.find(frame_id);
  if (kv == frames_.end()) {
    return;
  }

  auto frame_ptr = kv->second;
  // if (!frame_ptr->is_evictable_) {
  //   return;
  // }
  BUSTUB_ASSERT(frame_ptr->is_evictable_ == true, "# Remove a non_evictable frame");

  if (frame_ptr->cur_ < k_) {
    history_.remove(frame_ptr);
  } else {
    cache_.remove(frame_ptr);
  }
  frames_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
