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

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.resize(1);
  FillNewBucket(0, std::make_shared<Bucket>(bucket_size, 0));
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
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);

  while (dir_[index]->IsFull()) {
    auto local_depth = dir_[index]->GetDepth();

    // grow directory first
    if (local_depth == GetGlobalDepthInternal()) {
      GrowDirectory();
    }

    auto old_bucket = dir_[index];
    auto image_index = ImageBucketIndex(index);

    auto new_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    auto image_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    auto bucket_index_mask = (static_cast<size_t>(1) << new_bucket->GetDepth()) - 1;

    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == old_bucket) {
        if ((i & bucket_index_mask) == (index & bucket_index_mask)) {
          FillNewBucket(i, new_bucket);
        } else if ((i & bucket_index_mask) == (image_index & bucket_index_mask)) {
          FillNewBucket(i, image_bucket);
        } else {
          BUSTUB_ENSURE(false, "# In insert");
        }
      }
    }

    num_buckets_++;

    // can't pass dir_[index], because we have place a new bucket at index.
    RedistributeBucket(old_bucket);
    // update index
    index = IndexOf(key);
  }

  assert(!dir_[index]->IsFull());
  assert(dir_[index]->Insert(key, value));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::FillNewBucket(size_t index, std::shared_ptr<Bucket> bucket) -> void {
  // LOG_INFO("# index: %zu, num_buckets: %d", index, num_buckets_);
  dir_[index] = bucket;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ImageBucketIndex(size_t index) -> size_t {
  auto local_depth = dir_[index]->GetDepth();
  // auto low_index = index & ((static_cast<size_t>(1) << (local_depth + 1)) - 1);
  auto mask = static_cast<size_t>(1) << local_depth;
  return index ^ mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto items = bucket->GetItems();
  for (auto iter = items.begin(); iter != items.end(); iter++) {
    auto index = IndexOf(iter->first);
    dir_[index]->Insert(iter->first, iter->second);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GrowDirectory() -> void {
  global_depth_++;
  auto new_size = 1 << global_depth_;
  auto old_size = new_size >> 1;
  dir_.resize(new_size);
  for (int i = 0; i < old_size; i++) {
    dir_[i + old_size] = dir_[i];
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      value = iter->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      list_.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }

  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      iter->second = value;
      return true;
    }
  }

  list_.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
