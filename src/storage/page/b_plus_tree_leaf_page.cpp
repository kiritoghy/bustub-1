//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKV(int index) -> const MappingType & {
  BUSTUB_ASSERT(index < GetSize(), "wrong index in leaf Get KV_pair");
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                                          const KeyComparator &keyComparator) -> bool {
  for (int i = 0; i < GetSize(); ++i) {
    if (keyComparator(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
    }
  }
  return !result->empty();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int i = 0;
  int size = GetSize();
  // 找到插入位置
  for (; i < size; ++i) {
    auto comp = comparator(key, array_[i].first);
    if (comp == 0) {
      // 重复key
      return false;
    }
    if (comp == -1) {
      break;
    }
  }
  // TODO(me): memcpy?
  std::move_backward(array_ + i, array_ + size, array_ + size + 1);
  array_[i].first = key;
  array_[i].second = value;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetDataCopy(std::vector<MappingType> &data_copy, const KeyType &key,
                                             const ValueType &value, const KeyComparator &comparator) -> bool {
  int i = 0;
  int size = GetSize();
  // 找到插入位置
  for (; i < size; ++i) {
    auto comp = comparator(key, array_[i].first);
    if (comp == 0) {
      // 重复key
      return false;
    }
    if (comp == -1) {
      break;
    }
    data_copy[i] = array_[i];
  }
  data_copy[i] = std::make_pair(key, value);
  for (; i < size; ++i) {
    data_copy[i + 1] = array_[i];
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyDataFrom(std::vector<MappingType> &data_copy, int first, int last) -> void {
  auto amount = last - first;
  for (int i = 0; i < amount; ++i) {
    array_[i] = data_copy[first + i];
  }
  IncreaseSize(amount);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
