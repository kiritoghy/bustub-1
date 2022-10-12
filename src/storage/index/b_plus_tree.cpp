#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeftmostLeafPageId() -> page_id_t {
  if (IsEmpty()) {
    return INVALID_PAGE_ID;
  }

  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BUSTUB_ENSURE(page != nullptr, "fetch page failed");
  auto b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // find leaf page
  while (!b_plus_tree_page->IsLeafPage()) {
    auto b_plus_tree_internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);
    auto value = b_plus_tree_internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(value);
    BUSTUB_ENSURE(page != nullptr, "fetch page failed");
    b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  auto page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> Page * {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BUSTUB_ENSURE(page != nullptr, "fetch page failed");
  auto b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // find leaf page
  while (!b_plus_tree_page->IsLeafPage()) {
    auto b_plus_tree_internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);
    int comp = 0;
    int i = 1;

    for (; i < b_plus_tree_internal_page->GetSize(); i++) {
      auto ki = b_plus_tree_internal_page->KeyAt(i);
      comp = comparator_(ki, key);
      if (comp >= 0) {
        break;
      }
    }
    if (i >= b_plus_tree_internal_page->GetSize()) {
      i = b_plus_tree_internal_page->GetSize() - 1;
    } else if (comp == 0) {
      // do nothing
    } else {
      i = i - 1;
    }
    auto value = b_plus_tree_internal_page->ValueAt(i);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(value);
    BUSTUB_ENSURE(page != nullptr, "fetch page failed");
    b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(Page *page1, const KeyType &key, Page *page2) -> bool {
  auto b_plus_tree_page1 = reinterpret_cast<BPlusTreePage *>(page1->GetData());
  if (b_plus_tree_page1->IsRootPage()) {
    auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ENSURE(root_page != nullptr, "new page failed");
    UpdateRootPageId(0);
    auto b_plus_tree_root_page = reinterpret_cast<InternalPage *>(root_page->GetData());
    b_plus_tree_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    b_plus_tree_root_page->InsertAt(0, KeyType{}, page1->GetPageId());
    b_plus_tree_root_page->InsertAt(1, key, page2->GetPageId());
    auto b_plus_tree_page2 = reinterpret_cast<BPlusTreePage *>(page2->GetData());
    b_plus_tree_page1->SetParentPageId(root_page_id_);
    b_plus_tree_page2->SetParentPageId(root_page_id_);
    return true;
  }
  auto parent_page_id = b_plus_tree_page1->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  BUSTUB_ENSURE(parent_page != nullptr, "fetch page failed");
  auto b_plus_tree_parent_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (!b_plus_tree_parent_page->IsFull()) {
    auto ret = b_plus_tree_parent_page->Insert(key, page2->GetPageId(), comparator_);
    auto b_plus_tree_page2 = reinterpret_cast<BPlusTreePage *>(page2->GetData());
    b_plus_tree_page2->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(b_plus_tree_parent_page->GetPageId(), ret);
    return ret;
  }

  // todo
  std::vector<std::pair<KeyType, page_id_t>> copy_of_datas;
  auto ret = b_plus_tree_parent_page->GetDataWithNewKV(&copy_of_datas, key, page2->GetPageId(), comparator_);
  if (!ret) {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    return ret;
  }

  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  BUSTUB_ENSURE(new_page != nullptr, "new page failed");
  auto b_plus_tree_parent_page2 = reinterpret_cast<InternalPage *>(new_page->GetData());
  b_plus_tree_parent_page2->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  b_plus_tree_parent_page->SetSize(0);
  b_plus_tree_parent_page2->SetSize(0);
  int data_size = copy_of_datas.size();
  b_plus_tree_parent_page->CopyDataFrom(copy_of_datas, 0, data_size / 2);
  b_plus_tree_parent_page2->CopyDataFrom(copy_of_datas, data_size / 2, data_size);
  auto smallest_key = b_plus_tree_parent_page2->KeyAt(0);
  ret = InsertInParent(parent_page, smallest_key, new_page);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  return ret;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }

  auto page = FindLeafPage(key);
  BUSTUB_ENSURE(page != nullptr, "fetch page failed");

  // get values
  auto b_plus_tree_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto ret = b_plus_tree_leaf_page->GetValue(key, result, comparator_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page *page = nullptr;
  LeafPage *b_plus_tree_leaf_page = nullptr;
  bool ret = false;
  if (IsEmpty()) {
    page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ENSURE(page != nullptr, "oom!");
    b_plus_tree_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    UpdateRootPageId(1);
    b_plus_tree_leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    b_plus_tree_leaf_page->SetNextPageId(INVALID_PAGE_ID);
    b_plus_tree_leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
    b_plus_tree_leaf_page->SetSize(0);
  } else {
    page = FindLeafPage(key);
    BUSTUB_ENSURE(page != nullptr, "oom!");
    b_plus_tree_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  }

  if (!b_plus_tree_leaf_page->IsFull()) {
    ret = b_plus_tree_leaf_page->Insert(key, value, comparator_);
    // todo opt
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return ret;
  }

  // get a copy of data first, because we need to detect if has dup key
  std::vector<MappingType> copy_of_datas;
  ret = b_plus_tree_leaf_page->GetDataWithNewKV(&copy_of_datas, key, value, comparator_);
  if (!ret) {
    // 有重复的k
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return ret;
  }

  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  BUSTUB_ENSURE(new_page != nullptr, "Oom!");
  auto new_b_plus_tree_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_b_plus_tree_leaf_page->Init(new_page_id, b_plus_tree_leaf_page->GetParentPageId(), leaf_max_size_);
  new_b_plus_tree_leaf_page->SetNextPageId(b_plus_tree_leaf_page->GetNextPageId());
  new_b_plus_tree_leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
  b_plus_tree_leaf_page->SetNextPageId(new_b_plus_tree_leaf_page->GetPageId());

  b_plus_tree_leaf_page->SetSize(0);
  new_b_plus_tree_leaf_page->SetSize(0);
  auto data_size = copy_of_datas.size();
  b_plus_tree_leaf_page->CopyDataFrom(copy_of_datas, 0, data_size / 2);
  new_b_plus_tree_leaf_page->CopyDataFrom(copy_of_datas, data_size / 2, data_size);
  auto smallest_key = new_b_plus_tree_leaf_page->KeyAt(0);
  ret = InsertInParent(page, smallest_key, new_page);
  // todo unpin page
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto leftmost_page_id = LeftmostLeafPageId();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leftmost_page_id, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t page_id = INVALID_PAGE_ID;
  int k_index = 0;
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, page_id, k_index);
  }
  auto page = FindLeafPage(key);
  if (page != nullptr) {
    page_id = page->GetPageId();
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  k_index = leaf_page->SearchIndexByKey(key, comparator_);
  if (page_id != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page_id, k_index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, 0); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
