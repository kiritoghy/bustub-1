//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;

  // fetch a frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  // fetch the page from frame
  auto &page = pages_[frame_id];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.page_id_, page.data_);
  }
  page_table_->Remove(page.GetPageId());
  page.ResetMemory();
  // allocate page
  *page_id = AllocatePage();
  page.pin_count_ = 1;
  page.page_id_ = *page_id;
  page.is_dirty_ = false;
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (page_table_->Find(page_id, frame_id)) {
    // found in page table
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  // not found, try to pick a frame in freelist or replacer
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  auto &page = pages_[frame_id];
  if (page.is_dirty_) {
    disk_manager_->WritePage(page.page_id_, page.data_);
  }
  page_table_->Remove(page.page_id_);
  // page.ResetMemory();
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;

  disk_manager_->ReadPage(page_id, page.data_);
  page_table_->Insert(page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  auto &page = pages_[frame_id];
  --page.pin_count_;
  if (is_dirty) {
    page.is_dirty_ = is_dirty;
  }
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, frame_id)) {
    return false;
  }

  auto &page = pages_[frame_id];
  disk_manager_->WritePage(page_id, page.data_);
  page.is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    auto &page = pages_[i];
    if (page.page_id_ != INVALID_PAGE_ID && page.is_dirty_) {
      disk_manager_->WritePage(page.page_id_, page.data_);
      page.is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  auto &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(frame_id);
  if (page.is_dirty_) {
    disk_manager_->WritePage(page_id, page.data_);
  }
  page.ResetMemory();
  page.is_dirty_ = false;
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
