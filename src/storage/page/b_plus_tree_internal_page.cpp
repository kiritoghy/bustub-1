//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  BUSTUB_ASSERT(index < GetSize(), "index >= size");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index < GetMaxSize(), "index >= max_size");
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index < GetSize(), "index >= size");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) -> void {
  BUSTUB_ASSERT(index == GetSize(), "index != size");
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  BUSTUB_ENSURE(GetSize() < GetMaxSize(), "no more space for insert");
  // mast start at 1
  int insert_index = 1;
  // 找出插入的位置，顺便确认没有重复的key
  for (; insert_index < GetSize(); insert_index++) {
    auto comp = comparator(key, array_[insert_index].first);
    if (comp == 0) {
      return false;
    } else if (comp < 0) {
      break;
    }
  }

  for (int i = GetSize() - 1; i >= insert_index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[insert_index] = std::make_pair(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsFull() const -> bool { return GetSize() == GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetDataWithNewKV(std::vector<MappingType> *result, const KeyType &key,
                                                      const ValueType &value, const KeyComparator &comparator) -> bool {
  bool inserted = false;
  result->push_back(array_[0]);
  for (int i = 1; i < GetSize(); i++) {
    auto comp = comparator(key, array_[i].first);
    if (comp == 0) {
      return false;
    } else if (comp < 0 && !inserted) {
      result->push_back(std::make_pair(key, value));
      inserted = true;
    }
    result->push_back(array_[i]);
  }
  if (!inserted) {
    result->push_back(std::make_pair(key, value));
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyDataFrom(std::vector<MappingType> &datas, int first, int end) -> void {
  BUSTUB_ENSURE(first <= end, "first > end");
  int amount = end - first;
  for (int i = 0; first < end; i++, first++) {
    array_[i] = datas[first];
  }
  IncreaseSize(amount);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
