//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      table_iter_(table_info_->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
  auto txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!txn->IsTableExclusiveLocked(table_info_->oid_) && !txn->IsTableSharedLocked(table_info_->oid_) &&
        !txn->IsTableIntentionSharedLocked(table_info_->oid_) &&
        !txn->IsTableIntentionExclusiveLocked(table_info_->oid_) &&
        !txn->IsTableSharedIntentionExclusiveLocked(table_info_->oid_)) {
      auto lock_mgr = exec_ctx_->GetLockManager();
      try {
        lock_mgr->LockTable(txn, LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      } catch (TransactionAbortException &e) {
        txn->SetState(TransactionState::ABORTED);
        throw e;
      }
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_mgr = exec_ctx_->GetLockManager();
  auto table_iter_end = table_info_->table_->End();
  while (table_iter_ != table_iter_end) {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        lock_mgr->LockRow(txn, LockManager::LockMode::SHARED, table_info_->oid_, table_iter_->GetRid());
      } catch (TransactionAbortException &e) {
        txn->SetState(TransactionState::ABORTED);
        throw e;
      }
    }
    *tuple = *table_iter_;
    table_iter_++;
    *rid = tuple->GetRid();
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      try {
        lock_mgr->UnlockRow(txn, table_info_->oid_, *rid);
      } catch (TransactionAbortException &e) {
        txn->SetState(TransactionState::ABORTED);
        throw e;
      }
    }
    return true;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    try {
      lock_mgr->UnlockTable(txn, table_info_->oid_);
    } catch (TransactionAbortException &e) {
      txn->SetState(TransactionState::ABORTED);
      throw e;
    }
  }
  return false;
}

}  // namespace bustub
