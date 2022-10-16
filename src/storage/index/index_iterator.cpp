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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, LeafPage *current_leaf_page, int current_index, bool is_end)
 : buffer_pool_manager_(buffer_pool_manager), current_leaf_page_(current_leaf_page), current_index_(current_index), is_end_(is_end) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    BUSTUB_ASSERT(!IsEnd(), "Trying to access interator.end()");
    return current_leaf_page_->GetKV(current_index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    BUSTUB_ASSERT(!IsEnd(), "Trying to access interator.end()");
    current_index_++;
    if (current_index_ < current_leaf_page_->GetSize()) {
        return *this;
    }

    auto next_page_id = current_leaf_page_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
        is_end_ = true;
        buffer_pool_manager_->UnpinPage(current_leaf_page_->GetPageId(), false);
        current_leaf_page_ = nullptr;
        return *this;
    }

    buffer_pool_manager_->UnpinPage(current_leaf_page_->GetPageId(), false);
    auto page = buffer_pool_manager_->FetchPage(next_page_id);
    BUSTUB_ASSERT(page != nullptr, "Fetch page failed.");
    current_leaf_page_ = reinterpret_cast<LeafPage*>(page->GetData());
    current_index_ = 0;
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
    if (is_end_ && itr.is_end_) {
        return true;
    }
    if (is_end_ != itr.is_end_ || current_leaf_page_ != itr.current_leaf_page_ || current_index_ != itr.current_index_) {
        return false;
    }
    return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
    return !(*this == itr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
