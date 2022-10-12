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
  if (history_.empty() && cache_.empty()) {
    LOG_INFO("Evict failed");
    LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
    return false;
  }
  curr_size_--;
  if (!history_.empty()) {
    auto &frame_meta = history_.front();
    *frame_id = frame_meta->FrameId();
    history_.pop_front();
    frames_.erase(*frame_id);
    LOG_INFO("Evict %d success", *frame_id);
    LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
    return true;
  }
  if (!cache_.empty()) {
    auto &frame_meta = cache_.front();
    *frame_id = frame_meta->FrameId();
    cache_.pop_front();
    frames_.erase(*frame_id);
    LOG_INFO("Evict %d success", *frame_id);
    LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
    return true;
  }
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= (frame_id_t)replacer_size_, "frame id is invalid");
  auto kv = frames_.find(frame_id);
  if (kv == frames_.end()) {
    // frame id has not been seen before
    // create new entry
    curr_size_++;
    auto frame_meta = std::make_unique<FrameMeta>(frame_id, current_timestamp_++);
    history_.emplace_back(std::move(frame_meta));
    frames_[frame_id] = prev(history_.end());
    LOG_INFO("RecordAccess %d finished, %zu times", frame_id, frames_[frame_id]->get()->AccessTimes());
    LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
    return;
  }

  // found frame id
  auto &frame_ptr = *(kv->second);
  if (!frame_ptr->Evictable()) {
    // now frame is in list unevictable_
    // frames in unevictable_ needn't to adjust its position
    frame_ptr->Access(current_timestamp_++);
    LOG_INFO("RecordAccess %d finished, %zu times", frame_id, frames_[frame_id]->get()->AccessTimes());
    LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
    return;
  }

  // now frame is evictable
  if (frame_ptr->AccessTimes() < k_) {
    // now frame is in list history_
    frame_ptr->Access(current_timestamp_++);
    if (frame_ptr->AccessTimes() >= k_) {
      // move this frame in lru_k_cache
      cache_.emplace_back(std::move(frame_ptr));
      history_.erase(kv->second);
      frames_[frame_id] = prev(cache_.end());
    }

    // history list is fifo
    // } else {
    //   // the front of history is always the first to evict
    //   history_.emplace_back(std::move(frame_ptr));
    //   history_.erase(kv->second);
    //   frames_[frame_id] = prev(history_.end());
    // }
    LOG_INFO("RecordAccess %d finished, %zu times", frame_id, frames_[frame_id]->get()->AccessTimes());
    LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
    return;
  }

  // now the frame is in lru_k_cache
  frame_ptr->Access(current_timestamp_++);
  cache_.emplace_back(std::move(frame_ptr));
  cache_.erase(kv->second);
  frames_[frame_id] = prev(cache_.end());
  LOG_INFO("RecordAccess %d finished, %zu times", frame_id, frames_[frame_id]->get()->AccessTimes());
  LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= (frame_id_t)replacer_size_, "frame id is invalid");
  auto kv = frames_.find(frame_id);
  if (kv != frames_.end()) {
    auto &ptr = *(kv->second);
    std::list<std::unique_ptr<FrameMeta>> *list_ptr = nullptr;
    if (ptr->AccessTimes() < k_) {
      list_ptr = &history_;
    } else {
      list_ptr = &cache_;
    }

    if (ptr->Evictable() && !set_evictable) {
      // move this frame from evictable to unevictable
      curr_size_--;
      ptr->SetEvictable(set_evictable);
      unevictable_.emplace_back(std::move(ptr));
      list_ptr->erase(kv->second);
      frames_[frame_id] = prev(unevictable_.end());
      LOG_INFO("move %d from evictable to unevictable", frame_id);
      LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
    } else if (!ptr->Evictable() && set_evictable) {
      // move this frame from unevictable to evicable
      curr_size_++;
      ptr->SetEvictable(set_evictable);
      if (list_ptr->empty()) {
        list_ptr->emplace_back(std::move(ptr));
        unevictable_.erase(kv->second);
        frames_[frame_id] = prev(list_ptr->end());
      } else {
        auto itor = list_ptr->begin();
        for (itor = list_ptr->begin(); itor != list_ptr->end(); ++itor) {
          if (current_timestamp_ - (*itor)->CurrTimestamp() > current_timestamp_ - ptr->CurrTimestamp()) {
            continue;
          }
          break;
        }
        auto it = list_ptr->emplace(itor, std::move(ptr));
        unevictable_.erase(kv->second);
        frames_[frame_id] = it;
      }
      LOG_INFO("move %d from unevictable to evictable", frame_id);
      LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id <= (frame_id_t)replacer_size_, "frame id is invalid");
  LOG_INFO("remove %d", frame_id);
  auto kv = frames_.find(frame_id);
  if (kv == frames_.end()) {
    return;
  }

  auto &ptr = *(kv->second);
  if (!ptr->Evictable()) {
    return;
  }

  curr_size_--;
  std::list<std::unique_ptr<FrameMeta>> *list_ptr = nullptr;
  if (ptr->AccessTimes() < k_) {
    list_ptr = &history_;
  } else {
    list_ptr = &cache_;
  }

  list_ptr->erase(kv->second);
  frames_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  LOG_INFO("%lu, %lu, %lu", history_.size(), cache_.size(), unevictable_.size());
  return curr_size_;
}

}  // namespace bustub
