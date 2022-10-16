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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveEntry(const KeyType &key, const KeyComparator &comparator) -> void {
  int index = 1;
  auto size = GetSize();
  for (; index < size; ++index) {
    if (comparator(key, array_[index].first) == 0) {
      break;
    }
  }
  if (index == size) {
    // key doesn't exits;
    return;
  } else if(index < size - 1) {
    std::move(array_ + index + 1, array_ + size, array_ + index);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  int index = 1;
  auto size = GetSize();
  int comp;
  for (; index < size; ++index) {
    comp = comparator(key, array_[index].first) <= 0;
    if (comp < 0) {
      return index - 1;
    }
    if (comp == 0) {
      return index;
    }
  }
  return index - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEnd(B_PLUS_TREE_INTERNAL_PAGE_TYPE *b_plus_leaf_page, const KeyType &key) -> void {
  auto size = b_plus_leaf_page->GetSize();
  b_plus_leaf_page->array_[size] = array_[0];
  b_plus_leaf_page->array_[size].first = key;
  b_plus_leaf_page->IncreaseSize(1);
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
}


INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFront(B_PLUS_TREE_INTERNAL_PAGE_TYPE *b, const KeyType &key) -> void {
  auto size = b->GetSize();
  std::move_backward(b->array_, b->array_+size, b->array_+size+1);
  b->array_[0] = array_[GetSize()-1];
  (b->array_+1)->first = key;
  b->IncreaseSize(1);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *left, const KeyType &key) -> void {
  auto size = GetSize();
  auto l_size = left->GetSize();
  for (int i = 0; i < size; ++i) {
    left->array_[l_size+i] = array_[i];
  }
  left->array_[l_size].first = key;
  left->IncreaseSize(size);
  IncreaseSize(-size);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
