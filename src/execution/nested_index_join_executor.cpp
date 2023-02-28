//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid())),
      index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // Note: We will never insert duplicate rows into tables with indexes.
  Tuple outer_tuple;
  while (child_executor_->Next(&outer_tuple, rid)) {
    std::vector<RID> right_rids;
    // Tuple key_tuple;
    auto value = plan_->KeyPredicate()->EvaluateJoin(&outer_tuple, child_executor_->GetOutputSchema(), nullptr,
                                                     table_info_->schema_);
    // LOG_DEBUG("type:%d, value:%d", value.GetTypeId(), value.GetAs<int32_t>());
    Tuple key_tuple({value}, &index_info_->key_schema_);
    index_info_->index_->ScanKey(key_tuple, &right_rids, exec_ctx_->GetTransaction());
    std::vector<Value> res;
    if (!right_rids.empty()) {
      Tuple inner_tuple;
      RID inner_rid = right_rids[0];
      table_info_->table_->GetTuple(inner_rid, &inner_tuple, exec_ctx_->GetTransaction());
      res.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        res.emplace_back(outer_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); ++i) {
        res.emplace_back(inner_tuple.GetValue(&table_info_->schema_, i));
      }
      *tuple = Tuple(res, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      res.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        res.emplace_back(outer_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); ++i) {
        res.emplace_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(i).GetType()));
      }
      *tuple = Tuple(res, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
