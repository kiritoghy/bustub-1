//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  RID rid;
  Tuple tuple;
  child_->Init();
  int rows = 0;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    rows++;
  }
  if (rows == 0 && plan_->GetGroupBys().size() == 0) {
    aht_.InitWithEmptyTable(MakeAggregateKey(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto aht_itertor_end = aht_.End();
  while (aht_iterator_ != aht_itertor_end) {
    std::vector<Value> res;
    res.reserve(GetOutputSchema().GetColumnCount());
    for (auto &key_val : aht_iterator_.Key().group_bys_) {
      res.emplace_back(key_val);
    }
    for (auto &val : aht_iterator_.Val().aggregates_) {
      res.emplace_back(val);
    }
    *tuple = Tuple(res, &GetOutputSchema());
    ++aht_iterator_;
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
