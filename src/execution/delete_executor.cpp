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

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_deleted_) {
    return false;
  }
  int rows = 0;
  while (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      rows++;
      auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        index_info->index_->DeleteEntry(
            tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
      }
    } else {
      return false;
    }
  }
  Value value(TypeId::INTEGER, rows);
  *tuple = Tuple({value}, &GetOutputSchema());
  is_deleted_ = true;
  return true;
}

}  // namespace bustub
