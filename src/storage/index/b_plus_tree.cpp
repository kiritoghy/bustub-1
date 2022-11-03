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
    : root_latch_cnt_(0),
      index_name_(std::move(name)),
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
inline auto BPLUSTREE_TYPE::GetBPlusTreePage(page_id_t page_id, OperType op, page_id_t pre_page_id,
                                             Transaction *transaction) -> BPlusTreePage * {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  BUSTUB_ASSERT(page != nullptr, "Fetch page failed in BPlusTree.");
  Lock(page, op);
  auto b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // op为read，则可以释放之前的
  if (pre_page_id > 0 && (op == OperType::READ || b_plus_tree_page->IsSafe(op))) {
    FreePagesInTransaction(op, transaction, pre_page_id);
  }
  transaction->AddIntoPageSet(page);
  return b_plus_tree_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, OperType op, Transaction *transaction) -> LeafPage * {
  auto b_plus_tree_page = GetBPlusTreePage(root_page_id_, op, -1, transaction);

  // 非叶节点
  while (!b_plus_tree_page->IsLeafPage()) {
    auto b_plus_internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);
    int index = 1;
    int size = b_plus_internal_page->GetSize();
    int comp = 0;
    for (index = 1; index < size; ++index) {
      comp = comparator_(key, b_plus_internal_page->KeyAt(index));
      if (comp <= 0) {
        break;
      }
    }
    // key > k_m
    if (index == size) {
      index -= 1;
    } else {
      if (comp == -1) {
        // key < k_i
        index -= 1;
      }
      // key == k_i then find in P_i
    }
    auto page_id = b_plus_internal_page->ValueAt(index);
    auto pre_page_id = b_plus_internal_page->GetPageId();
    // buffer_pool_manager_->UnpinPage(b_plus_internal_page->GetPageId(), false);
    b_plus_tree_page = GetBPlusTreePage(page_id, op, pre_page_id, transaction);
    // free_page
  }

  // 叶子节点
  return reinterpret_cast<LeafPage *>(b_plus_tree_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *b_plus_tree_page, const KeyType &key,
                                    BPlusTreePage *new_b_plus_tree_page) -> bool {
  if (b_plus_tree_page->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(page != nullptr, "New page failed in InsertInParent.");
    auto root_page = reinterpret_cast<InternalPage *>(page->GetData());
    root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root_page->Insert(KeyType{}, b_plus_tree_page->GetPageId(), comparator_, 0);
    root_page->Insert(key, new_b_plus_tree_page->GetPageId(), comparator_, 1);
    UpdateRootPageId(0);
    b_plus_tree_page->SetParentPageId(root_page_id_);
    new_b_plus_tree_page->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  auto parent_page_id = b_plus_tree_page->GetParentPageId();
  // auto b_plus_parent = GetBPlusTreePage(parent_page_id);
  // 此时page在transaction中，不需要再加入
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  BUSTUB_ASSERT(page != nullptr, "Fetch page failed in BPlusTree.");
  auto b_plus_parent_page = reinterpret_cast<InternalPage *>(page->GetData());

  if (!b_plus_parent_page->IsFull()) {
    auto res = b_plus_parent_page->Insert(key, new_b_plus_tree_page->GetPageId(), comparator_);
    new_b_plus_tree_page->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return res;
  }

  // 父节点满 Spilt and Redistribute
  // std::vector<MappingType> data_copy(internal_max_size_+1);
  std::vector<std::pair<KeyType, page_id_t>> data_copy(internal_max_size_ + 1);
  auto res = b_plus_parent_page->GetDataCopy(data_copy, key, new_b_plus_tree_page->GetPageId(), comparator_);
  if (!res) {
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
    return res;
  }

  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  BUSTUB_ASSERT(new_page != nullptr, "create a page for B+tree failed.");
  Lock(new_page, OperType::INSERT);
  transaction->AddIntoPagesSet(new_page);
  auto new_b_plus_parent_page = reinterpret_cast<InternalPage *>(new_page->GetData());

  // 创建新的非叶节点并重新分配
  new_b_plus_parent_page->Init(new_page_id, b_plus_parent_page->GetParentPageId(), internal_max_size_);
  // ceil(A/B) = int((A+B-1)/B)
  auto max_size = data_copy.size();
  b_plus_parent_page->SetSize(0);
  b_plus_parent_page->CopyDataFrom(data_copy, 0, (max_size + 1) / 2);
  new_b_plus_parent_page->CopyDataFrom(data_copy, (max_size + 1) / 2, max_size);
  for (int i = 0; i < b_plus_parent_page->GetSize(); ++i) {
    // auto new_page = GetBPlusTreePage(b_plus_parent_page->ValueAt(i));
    // 不需要加锁，因为上层有锁，其他线程无法访问子节点
    page = buffer_pool_manager_->FetchPage(b_plus_parent_page->ValueAt(i));
    BUSTUB_ASSERT(page != nullptr, "Fetch page failed in BPlusTree.");
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    tree_page->SetParentPageId(b_plus_parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), true);
  }
  for (int i = 0; i < new_b_plus_parent_page->GetSize(); ++i) {
    // page = GetBPlusTreePage(new_b_plus_parent_page->ValueAt(i));
    page = buffer_pool_manager_->FetchPage(new_b_plus_parent_page->ValueAt(i));
    BUSTUB_ASSERT(page != nullptr, "Fetch page failed in BPlusTree.");
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    tree_page->SetParentPageId(new_b_plus_parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), true);
  }
  // auto size = b_plus_parent_page->GetSize();
  // b_plus_parent_page->SetSize(0);
  // b_plus_parent_page->CopyDataFrom(data_copy, 0, size / 2 + 1);
  // new_b_plus_parent_page->CopyDataFrom(data_copy, size / 2 + 1, size + 1);

  // 内部节点中，分配第二个节点时，本应当从key1分配，但是由于第一个key会被插入到上一层节点
  // 因此可以将其放在key0， 而key0本身不会被访问到
  auto smallest_key = new_b_plus_parent_page->KeyAt(0);
  res = InsertInParent(reinterpret_cast<BPlusTreePage *>(b_plus_parent_page), smallest_key,
                       reinterpret_cast<BPlusTreePage *>(new_b_plus_parent_page));
  // b_plus_parent_page本身在transaction中，此处fetch后需要自己unpin
  buffer_pool_manager_->UnpinPage(b_plus_parent_page->GetPageId(), true);
  // new_b_plus_parent_page初次获取，在transaction中unpin
  // buffer_pool_manager_->UnpinPage(new_b_plus_parent_page->GetPageId(), true);
  return res;
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
  // 当B+树为空
  LockRoot(OperType::READ);
  if (root_page_id_ == INVALID_PAGE_ID) {
    TryUnlockRoot(OperType::READ);
    return false;
  }

  // have read lock, parent have been released
  auto b_plus_leaf_page = FindLeafPage(key, OperType::READ, transaction);
  auto res = b_plus_leaf_page->GetValue(key, result, comparator_);
  FreePagesInTransaction(OperType::READ, transaction, b_plus_leaf_page->GetPageId);
  // buffer_pool_manager_->UnpinPage(b_plus_leaf_page->GetPageId(), false);
  return res;
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
  LeafPage *b_plus_leaf_page = nullptr;
  LockRoot(OperType::INSERT);
  if (IsEmpty()) {
    // B+树为空，创建一个新的叶子节点，同时也是给根节点
    // TODO(me): resource management, ensure unpinpage
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(page != nullptr, "create a page for B+tree failed.");
    b_plus_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    b_plus_leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId(1);
    // 新节点一定不会满
    b_plus_leaf_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(b_plus_leaf_page->GetPageId(), true);
    TryUnlockRoot(OperType::INSERT);
    return true;
  }

  // now b_plus_leaf_page有写锁，有可能分裂的节点及其父节点有写锁
  b_plus_leaf_page = FindLeafPage(key, OperType::INSERT, transaction);
  if (!b_plus_leaf_page->IsFull()) {
    auto res = b_plus_leaf_page->Insert(key, value, comparator_);
    if (res && b_plus_leaf_page->IsFull()) {
      // 叶子节点满了，Spilt and Redistribute
      // 创建一个临时节点，使用shared_ptr避免手动delete
      // auto data_copy = std::make_shared<MappingType[]>(b_plus_leaf_page->GetSize()+1);
      // std::shared_ptr<MappingType[]> data_copy(new MappingType[b_plus_leaf_page->GetSize() + 1]());
      std::vector<MappingType> data_copy(b_plus_leaf_page->GetSize());
      auto res = b_plus_leaf_page->GetDataCopy(data_copy, comparator_);
      if (!res) {
        // duplicate key exists
        FreePagesInTransaction(op, transaction, b_plus_leaf_page->GetPageId());
        // buffer_pool_manager_->UnpinPage(b_plus_leaf_page->GetPageId(), false);
        return false;
      }
      page_id_t new_page_id;
      auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
      BUSTUB_ASSERT(new_page != nullptr, "create a page for B+tree failed.");
      Lock(new_page, OperType::INSERT);
      transaction->AddIntoPageSet(new_page);
      auto new_b_plus_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());

      // 创建新的叶子节点并重新分配
      new_b_plus_leaf_page->Init(new_page_id, b_plus_leaf_page->GetParentPageId(), leaf_max_size_);
      new_b_plus_leaf_page->SetNextPageId(b_plus_leaf_page->GetNextPageId());
      b_plus_leaf_page->SetNextPageId(new_page_id);
      auto max_size = data_copy.size();
      b_plus_leaf_page->SetSize(0);
      b_plus_leaf_page->CopyDataFrom(data_copy, 0, (max_size + 1) / 2);
      new_b_plus_leaf_page->CopyDataFrom(data_copy, (max_size + 1) / 2, max_size);

      auto smallest_key = new_b_plus_leaf_page->KeyAt(0);

      res = InsertInParent(reinterpret_cast<BPlusTreePage *>(b_plus_leaf_page), smallest_key,
                           reinterpret_cast<BPlusTreePage *>(new_b_plus_leaf_page));
      // buffer_pool_manager_->UnpinPage(b_plus_leaf_page->GetPageId(), true);
      // buffer_pool_manager_->UnpinPage(new_b_plus_leaf_page->GetPageId(), true);
      FreePagesInTransaction(OperType::INSERT, transaction, b_plus_leaf_page->GetPageId());
      return res;
    }
    // 插入失败
    FreePagesInTransaction(OperType::INSERT, transaction, b_plus_leaf_page->GetPgeId());
    // buffer_pool_manager_->UnpinPage(b_plus_leaf_page->GetPageId(), true);
    return res;
  }

  return false;
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto b_plus_leaf_page = FindLeafPage(key);
  RemoveEntry<LeafPage>(reinterpret_cast<BPlusTreePage *>(b_plus_leaf_page), key);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename BPlusTreePageType>
void BPLUSTREE_TYPE::RemoveEntry(BPlusTreePage *b_plus_tree_page, const KeyType &key) {
  auto b_plus_page = reinterpret_cast<BPlusTreePageType *>(b_plus_tree_page);
  b_plus_page->RemoveEntry(key, comparator_);

  if (b_plus_page->IsRootPage()) {
    // 非叶节点 将其子节点提上来作为根节点
    if (!b_plus_page->IsLeafPage() && b_plus_page->GetSize() == 1) {
      auto page = GetBPlusTreePage(reinterpret_cast<InternalPage *>(b_plus_page)->ValueAt(0));
      root_page_id_ = page->GetPageId();
      page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(0);
      auto old_page_id = b_plus_page->GetPageId();
      buffer_pool_manager_->UnpinPage(old_page_id, true);
      buffer_pool_manager_->DeletePage(old_page_id);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    } else if (b_plus_page->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      auto old_page_id = b_plus_page->GetPageId();
      buffer_pool_manager_->UnpinPage(old_page_id, true);
      buffer_pool_manager_->DeletePage(old_page_id);
    } else {
      // 若是叶节点，则说明叶节点即是根节点，不用管。
      buffer_pool_manager_->UnpinPage(b_plus_page->GetPageId(), true);
    }
    return;
  }
  if (b_plus_page->GetSize() < b_plus_page->GetMinSize()) {
    // 非根节点并且数量少于最小数量
    // coalesce_or_redistribute
    // parent_page一定是InternalPage
    auto page = GetBPlusTreePage(b_plus_page->GetParentPageId());
    auto b_plus_parent_page = reinterpret_cast<InternalPage *>(page);
    // 虽然子节点中删除了key，但是还能在父结点中找到key所在的pointer
    int index = b_plus_parent_page->KeyIndex(key, comparator_);
    page_id_t neighbor_page_id;
    if (index == 0) {
      neighbor_page_id = b_plus_parent_page->ValueAt(index + 1);
    } else {
      neighbor_page_id = b_plus_parent_page->ValueAt(index - 1);
    }
    page = GetBPlusTreePage(neighbor_page_id);
    auto nei_b_plus_page = reinterpret_cast<BPlusTreePageType *>(page);
    if (b_plus_page->GetSize() + nei_b_plus_page->GetSize() > b_plus_page->GetMaxSize()) {
      Redistribute<BPlusTreePageType>(reinterpret_cast<BPlusTreePage *>(b_plus_page),
                                      reinterpret_cast<BPlusTreePage *>(nei_b_plus_page), b_plus_parent_page, index);
    } else {
      Coalesce<BPlusTreePageType>(reinterpret_cast<BPlusTreePage *>(b_plus_page),
                                  reinterpret_cast<BPlusTreePage *>(nei_b_plus_page), b_plus_parent_page, index);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename BPlusTreePageType>
auto BPLUSTREE_TYPE::Redistribute(BPlusTreePage *b_plus_tree_page, BPlusTreePage *nei_b_plus_tree_page,
                                  InternalPage *b_plus_parent_page, int index) -> void {
  BUSTUB_ASSERT(b_plus_tree_page->GetSize() < nei_b_plus_tree_page->GetSize(), "got invalid neighbor.");
  auto b_plus_page = reinterpret_cast<BPlusTreePageType *>(b_plus_tree_page);
  auto nei_b_plus_page = reinterpret_cast<BPlusTreePageType *>(nei_b_plus_tree_page);
  if (index == 0) {
    // neibor is at right
    nei_b_plus_page->MoveFirstToEnd(b_plus_page, b_plus_parent_page->KeyAt(index + 1));
    if (!b_plus_page->IsLeafPage()) {
      auto page = GetBPlusTreePage(reinterpret_cast<InternalPage *>(b_plus_page)->ValueAt(b_plus_page->GetSize() - 1));
      page->SetParentPageId(b_plus_page->GetPageId());
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
    b_plus_parent_page->SetKeyAt(index + 1, nei_b_plus_page->KeyAt(0));
  } else {
    nei_b_plus_page->MoveLastToFront(b_plus_page, b_plus_parent_page->KeyAt(index));
    if (!b_plus_page->IsLeafPage()) {
      auto page = GetBPlusTreePage(reinterpret_cast<InternalPage *>(b_plus_page)->ValueAt(0));
      page->SetParentPageId(b_plus_page->GetPageId());
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
    b_plus_parent_page->SetKeyAt(index, b_plus_page->KeyAt(0));
  }

  buffer_pool_manager_->UnpinPage(b_plus_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(nei_b_plus_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(b_plus_parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename BPlusTreePageType>
auto BPLUSTREE_TYPE::Coalesce(BPlusTreePage *b_plus_tree_page, BPlusTreePage *nei_b_plus_tree_page,
                              InternalPage *b_plus_parent_page, int index) -> void {
  BPlusTreePageType *left = nullptr;
  BPlusTreePageType *right = nullptr;

  // move all right to left
  if (index == 0) {
    left = reinterpret_cast<BPlusTreePageType *>(b_plus_tree_page);
    right = reinterpret_cast<BPlusTreePageType *>(nei_b_plus_tree_page);
    index++;
  } else {
    left = reinterpret_cast<BPlusTreePageType *>(nei_b_plus_tree_page);
    right = reinterpret_cast<BPlusTreePageType *>(b_plus_tree_page);
  }

  auto key = b_plus_parent_page->KeyAt(index);
  right->MoveTo(left, key);
  if (left->IsLeafPage()) {
    reinterpret_cast<LeafPage *>(left)->SetNextPageId(reinterpret_cast<LeafPage *>(right)->GetNextPageId());
  }

  if (!left->IsLeafPage()) {
    for (int i = 0; i < left->GetSize(); ++i) {
      auto page = GetBPlusTreePage(reinterpret_cast<InternalPage *>(left)->ValueAt(i));
      page->SetParentPageId(left->GetPageId());
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }

  auto right_page_id = right->GetPageId();
  buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  buffer_pool_manager_->DeletePage(right_page_id);
  RemoveEntry<InternalPage>(reinterpret_cast<BPlusTreePage *>(b_plus_parent_page), key);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindSmallestLeafPage(Transaction *transaction) -> LeafPage * {
  auto b_plus_tree_page = GetBPlusTreePage(root_page_id_);

  while (!b_plus_tree_page->IsLeafPage()) {
    auto b_plus_internal_page = reinterpret_cast<InternalPage *>(b_plus_tree_page);
    auto page_id = b_plus_internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(b_plus_internal_page->GetPageId(), false);
    b_plus_tree_page = GetBPlusTreePage(page_id);
  }

  return reinterpret_cast<LeafPage *>(b_plus_tree_page);
}

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
  if (IsEmpty()) {
    return End();
  }
  auto b_plus_leaf_page = FindSmallestLeafPage();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, b_plus_leaf_page);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  auto b_plus_leaf_page = FindLeafPage(key);
  int index = 0;
  auto size = b_plus_leaf_page->GetSize();
  for (; index < size; ++index) {
    if (comparator_(key, b_plus_leaf_page->KeyAt(index)) == 0) {
      break;
    }
  }
  // key不存在
  if (index == size) {
    return End();
  }
  return INDEXITERATOR_TYPE(buffer_pool_manager_, b_plus_leaf_page, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0, true); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

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
  out.flush();
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

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LockRoot(OperType op) -> void {
  if (op == OperType::READ) {
    root_latch_.RLock();
  } else {
    root_latch_.WLock();
  }
  root_latch_cnt_++;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryUnlockRoot(OperType op) -> void {
  if (root_latch_cnt_ > 0) {
    if (op == OperType::READ) {
      root_latch_.RUnlock();
    } else {
      root_latch_.WUnlock();
    }
    root_latch_cnt_--;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Lock(Page *page, OperType op) -> void {
  if (op == OperType::READ) {
    page->RLatch();
  } else {
    page->WLatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Unlock(Page *page, OperType op) -> void {
  if (op == OperType::READ) {
    page->RUnlatch();
  } else {
    page->WUnlatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FreePagesInTransaction(OperType op, Transaction *transaction, page_id_t cur) -> void {
  TryUnlockRoot(op);
  for (auto page : *transaction->GetPageSet()) {
    auto page_id = page->GetPageId();
    Unlock(page, op);
    buffer_pool_manager_->UnpinPage(page_id, op != OperType::READ);
    if (transaction->GetDeletedPageSet()->find(page_id) != transaction->GetDeletedPageSet()->end()) {
      buffer_pool_manager_->DeletePage(page_id);
      transaction->GetDeletedPageSet()->erase(page_id);
    }
  }
}

// INDEX_TEMPLATE_ARGUMENTS
// void BPLUSTREE_TYPE::RemoveEntry(BPlusTreePage *b_plus_tree_page, const KeyType &key) {
//   if (b_plus_tree_page->IsLeafPage()) {
//     auto b_plus_leaf_page = reinterpret_cast<LeafPage*>(b_plus_tree_page);
//     b_plus_leaf_page->RemoveEntry(key, comparator_);
//   } else {
//     auto b_plus_internal_page = reinterpret_cast<InternalPage*>(b_plus_tree_page);
//     b_plus_internal_page->RemoveEntry(key, comparator_);
//   }

//   if (b_plus_tree_page->IsRootPage() && b_plus_tree_page->GetSize() == 1) {
//     // 非叶节点 将其子节点提上来作为根节点
//     if (!b_plus_tree_page->IsLeafPage()) {
//       auto b_plus_internal_page = reinterpret_cast<InternalPage*>(b_plus_tree_page);
//       auto page = GetBPlusTreePage(b_plus_internal_page->ValueAt(0));
//       root_page_id_ = page->GetPageId();
//       page->SetParentPageId(INVALID_PAGE_ID);
//       UpdateRootPageId(0);
//       buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
//     }
//     // 若是叶节点，则说明叶节点即是根节点，不用管。
//     buffer_pool_manager_->UnpinPage(b_plus_tree_page->GetPageId(), true);
//     return;
//   } else if (b_plus_tree_page->GetSize() < b_plus_tree_page->GetMinSize()) {
//     // 非根节点并且数量少于最小数量
//     // coalesce_or_redistribute
//     // parent_page一定是InternalPage
//     auto page = GetBPlusTreePage(b_plus_tree_page->GetParentPageId());
//     auto b_plus_parent_page = reinterpret_cast<InternalPage*>(page);
//     // 虽然子节点中删除了key，但是还能在父结点中找到key所在的pointer
//     int index = b_plus_parent_page->KeyIndex(key, comparator_);
//     page_id_t neighbor_page_id;
//     if (index == 0) {
//       neighbor_page_id = b_plus_parent_page->ValueAt(index + 1);
//     } else {
//       neighbor_page_id = b_plus_parent_page->ValueAt(index - 1);
//     }
//     if (b_plus_tree_page->IsLeafPage()) {
//       page = GetBPlusTreePage(neighbor_page_id);
//       auto nei_b_plus_leaf_page = reinterpret_cast<LeafPage*>(page);
//       auto b_plus_leaf_page = reinterpret_cast<LeafPage*>(b_plus_tree_page);
//       if (b_plus_leaf_page->GetSize() + nei_b_plus_leaf_page->GetSize() > b_plus_leaf_page->GetMaxSize()) {
//         Redistribute(reinterpret_cast<BPlusTreePage*>(b_plus_leaf_page),
//         reinterpret_cast<BPlusTreePage*>(nei_b_plus_leaf_page), b_plus_parent_page, index);
//       } else {
//         Coalesce(reinterpret_cast<BPlusTreePage*>(b_plus_leaf_page),
//         reinterpret_cast<BPlusTreePage*>(nei_b_plus_leaf_page), b_plus_parent_page, index);
//       }
//     } else {
//       page = GetBPlusTreePage(neighbor_page_id);
//       auto nei_b_plus_leaf_page = reinterpret_cast<InternalPage*>(page);
//       auto b_plus_leaf_page = reinterpret_cast<InternalPage*>(b_plus_tree_page);
//       if (b_plus_leaf_page->GetSize() + nei_b_plus_leaf_page->GetSize() > b_plus_leaf_page->GetMaxSize()) {
//         Redistribute<LeafPage>(reinterpret_cast<BPlusTreePage*>(b_plus_leaf_page),
//         reinterpret_cast<BPlusTreePage*>(nei_b_plus_leaf_page), b_plus_parent_page, index);
//       } else {
//         Coalesce(reinterpret_casst<BPlusTreePage*>(b_plus_leaf_page),
//         reinterpret_cast<BPlusTreePage*>(nei_b_plus_leaf_page), b_plus_parent_page, index);
//       }
//     }
//   }
// }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
