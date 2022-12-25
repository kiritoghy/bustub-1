//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())),
      table_info_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())),
      index_iter_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto index_iter_end = tree_->GetEndIterator();
  while (index_iter_ != index_iter_end) {
    *rid = (*index_iter_).second;
    ++index_iter_;
    table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
    return true;
  }
  return false;
}

}  // namespace bustub
