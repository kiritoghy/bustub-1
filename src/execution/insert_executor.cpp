//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)),
      is_inserted_(false) {}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
  is_inserted_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  int rows = 0;
  while (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
      rows++;
      auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        index_info->index_->InsertEntry(
            tuple->KeyFromTuple(GetOutputSchema(), index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
      }
    } else {
      Value value(TypeId::INTEGER, 0);
      *tuple = Tuple({value}, &GetOutputSchema());
      return false;
    }
  }
  Value value(TypeId::INTEGER, rows);
  *tuple = Tuple({value}, &GetOutputSchema());
  if (is_inserted_) {
    return false;
  } else {
    is_inserted_ = true;
    return true;
  }
}

}  // namespace bustub
