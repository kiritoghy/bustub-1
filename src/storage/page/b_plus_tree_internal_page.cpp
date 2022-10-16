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
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator, int position) -> bool {
  int size = GetSize();
  if (position == -1) {
    // 按key插入
    int index = 1;
    for (; index < size; ++index) {
      auto comp = comparator(key, array_[index].first);
      if (comp == 0) {
        return false;
      }
      if (comp == -1) {
        break;
      }
    }

    // BaseAddr = array_+1
    std::move_backward(array_+1 + index, array_+1 + size, array_+1 + size + 1);
    array_[index] = std::make_pair(key, value);
    IncreaseSize(1);
    return true;
  }

  // 在末尾插入
  BUSTUB_ASSERT(position == size, "Wrong insert position In internal node insert.");
  array_[position] = std::make_pair(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetDataCopy(std::vector<MappingType> &data_copy, const KeyType &key,
                                             const ValueType &value, const KeyComparator &comparator) -> bool {
  data_copy[0] = array_[0];
  int i = 1;
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyDataFrom(std::vector<MappingType> &data_copy, int first, int last) -> void {
  auto amount = last - first;
  for (int i = 0; i < amount; ++i) {
    array_[i] = data_copy[first + i];
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
