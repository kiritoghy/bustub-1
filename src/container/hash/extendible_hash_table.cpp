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
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOfPair(int dir_index) -> size_t {
  auto local_depth = dir_[dir_index]->GetDepth();
  return dir_index ^ (1 << (local_depth - 1));
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
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) -> void {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  // auto index = IndexOf(key);
  // if (dir_[index]->Insert(key, value)) {
  //   // insert success
  //   return;
  // }
  // if (dir_[index]->IsFull()) {  // bucket is full, ready to spilt
  //   Spilt(index);
  //   Insert(key, value);
  // } else {
  //   Insert(key, value);
  // }
  auto index = IndexOf(key);
  while (!dir_[index]->Insert(key, value)) {
    Spilt(index);
    index = IndexOf(key);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Spilt(int index) -> void {
  auto bucket = dir_[index];
  bucket->IncrementDepth();
  if (bucket->GetDepth() > global_depth_) {
    Grow();
  }
  auto pair_index = IndexOfPair(index);
  dir_[pair_index] = std::make_shared<Bucket>(bucket_size_, dir_[index]->GetDepth());
  int diff = 1 << dir_[index]->GetDepth();

  for (int i = pair_index - diff; i >= 0; i -= diff) {
    dir_[i] = dir_[pair_index];
  }
  for (int i = pair_index + diff; i < 1 << global_depth_; i += diff) {
    dir_[i] = dir_[pair_index];
  }

  for (size_t i = 0; i < bucket_size_; ++i) {
    auto kv_pair = dir_[index]->GetItems().front();
    dir_[index]->GetItems().pop_front();
    auto new_index = IndexOf(kv_pair.first);
    dir_[new_index]->Insert(kv_pair.first, kv_pair.second);
  }

  num_buckets_++;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Grow() -> void {
  for (int i = 0; i < 1 << global_depth_; ++i) {
    dir_.push_back(dir_[i]);
  }
  global_depth_++;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (const auto &item : list_) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto itor = list_.begin(); itor != list_.end(); ++itor) {
    if (itor->first == key) {
      list_.erase(itor);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented");
  if (IsFull()) {
    return false;
  }
  for (auto &item : list_) {
    if (item.first == key) {
      item.second = value;
      return true;
    }
  }

  list_.emplace(list_.end(), std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
