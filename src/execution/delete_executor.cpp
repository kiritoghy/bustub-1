//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      is_deleted_(false) {}

void DeleteExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  auto txn = exec_ctx_->GetTransaction();
  if (!txn->IsTableIntentionExclusiveLocked(table_info_->oid_)) {
    auto lock_mgr = exec_ctx_->GetLockManager();
    try {
      lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
    } catch (TransactionAbortException &e) {
      txn->SetState(TransactionState::ABORTED);
      throw e;
    }
  }
  is_deleted_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_mgr = exec_ctx_->GetLockManager();
  if (is_deleted_) {
    // if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    //   try {
    //     lock_mgr->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
    //   } catch (TransactionAbortException &e) {
    //     txn->SetState(TransactionState::ABORTED);
    //     throw e;
    //   }
    // }
    return false;
  }
  int rows = 0;
  while (child_executor_->Next(tuple, rid)) {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        lock_mgr->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
      } catch (TransactionAbortException &e) {
        txn->SetState(TransactionState::ABORTED);
        throw e;
      }
    }
    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      rows++;
      auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        index_info->index_->DeleteEntry(
            tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
        IndexWriteRecord index_write_record(*rid, table_info_->oid_, WType::DELETE, *tuple, index_info->index_oid_,
                                            exec_ctx_->GetCatalog());
        txn->AppendIndexWriteRecord(index_write_record);
      }
      // if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      //   try {
      //     lock_mgr->UnlockRow(txn, table_info_->oid_, *rid);
      //   } catch (TransactionAbortException &e) {
      //     txn->SetState(TransactionState::ABORTED);
      //     throw e;
      //   }
      // }
    } else {
      // if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      //   try {
      //     lock_mgr->UnlockRow(txn, table_info_->oid_, *rid);
      //     lock_mgr->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
      //   } catch (TransactionAbortException &e) {
      //     txn->SetState(TransactionState::ABORTED);
      //     throw e;
      //   }
      // }
      return false;
    }
  }
  Value value(TypeId::INTEGER, rows);
  *tuple = Tuple({value}, &GetOutputSchema());
  is_deleted_ = true;
  // if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
  //   try {
  //     lock_mgr->UnlockTable(txn, table_info_->oid_);
  //   } catch (TransactionAbortException &e) {
  //     txn->SetState(TransactionState::ABORTED);
  //     throw e;
  //   }
  // }
  return true;
}

}  // namespace bustub
