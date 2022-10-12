/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (current_page_ != nullptr && buffer_pool_manager_ != nullptr) {
    buffer_pool_manager_->UnpinPage(current_page_->GetPageId(), false);
    current_page_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t leftmost_page_id, int start_index)
    : buffer_pool_manager_(buffer_pool_manager),
      current_page_id_(leftmost_page_id),
      current_index_(start_index),
      is_end_(false) {
  if (current_page_id_ == INVALID_PAGE_ID) {
    current_page_ = nullptr;
    is_end_ = true;
  } else {
    current_page_ = buffer_pool_manager_->FetchPage(current_page_id_);
    current_leaf_page_ = reinterpret_cast<LeafPage *>(current_page_->GetData());
    if (current_index_ >= current_leaf_page_->GetSize()) {
      is_end_ = true;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS auto INDEXITERATOR_TYPE::IsEnd() -> bool { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (IsEnd()) {
    throw std::runtime_error("out of range");
  }
  return current_leaf_page_->kvAt(current_index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    throw std::runtime_error("out of range");
  }
  ++current_index_;
  if (current_index_ < current_leaf_page_->GetSize()) {
    return *this;
  }
  auto next_page_id = current_leaf_page_->GetNextPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    is_end_ = true;
  }
  buffer_pool_manager_->UnpinPage(current_page_->GetPageId(), false);
  current_page_ = buffer_pool_manager_->FetchPage(next_page_id);
  BUSTUB_ASSERT(current_page_ != nullptr, "fetch page failed");
  current_leaf_page_ = reinterpret_cast<LeafPage *>(current_page_->GetData());
  current_index_ = 0;
  if (current_leaf_page_->GetSize() == 0) {
    is_end_ = true;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
