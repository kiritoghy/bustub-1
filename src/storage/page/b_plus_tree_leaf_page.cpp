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
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
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
  BUSTUB_ASSERT(index < GetSize(), "index >= size");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::kvAt(int index) -> const MappingType & {
  BUSTUB_ASSERT(index < GetSize(), "index >= size");
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                                          const KeyComparator &comparator) -> bool {
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
    }
  }
  return !result->empty();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  BUSTUB_ENSURE(GetSize() < GetMaxSize(), "no more space for insert");
  int insert_index = 0;
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsFull() const -> bool { return GetSize() >= GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetDataWithNewKV(std::vector<MappingType> *result, const KeyType &key,
                                                  const ValueType &value, const KeyComparator &comparator) -> bool {
  bool inserted = false;
  for (int i = 0; i < GetSize(); i++) {
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::CopyDataFrom(std::vector<MappingType> &datas, int first, int end) -> void {
  BUSTUB_ENSURE(first <= end, "first > end");
  auto amount = end - first;
  for (int i = 0; first < end; i++, first++) {
    array_[i] = datas[first];
  }
  IncreaseSize(amount);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SearchIndexByKey(const KeyType &key, const KeyComparator &comparator) -> int {
  int index = 0;
  for (; index < GetSize(); index++) {
    if (comparator(key, array_[index].first) == 0) {
      break;
    }
  }

  return index;
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
