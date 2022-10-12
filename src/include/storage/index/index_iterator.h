//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT

  IndexIterator(const IndexIterator &rhs) {
    buffer_pool_manager_ = rhs.buffer_pool_manager_;
    current_page_id_ = rhs.current_page_id_;
    current_leaf_page_ = rhs.current_leaf_page_;
    current_index_ = rhs.current_index_;
    is_end_ = rhs.is_end_;
    if (current_page_id_ != INVALID_PAGE_ID) {
      current_page_ = buffer_pool_manager_->FetchPage(current_page_id_);
    }
  }

  IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leftmost_page_id, int start_index);

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (is_end_ && itr.is_end_) {
      return true;
    }
    if (is_end_ != itr.is_end_ || current_page_ != itr.current_page_ || current_index_ != itr.current_index_) {
      return false;
    }

    return true;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  BufferPoolManager *buffer_pool_manager_;
  page_id_t current_page_id_;
  Page *current_page_;
  LeafPage *current_leaf_page_;
  int current_index_;
  bool is_end_;
};

}  // namespace bustub
