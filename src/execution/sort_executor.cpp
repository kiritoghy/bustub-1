#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &t1, const Tuple &t2) -> bool {
    for (auto &order : plan_->GetOrderBy()) {
      auto order_type = order.first;
      auto val1 = order.second->Evaluate(&t1, GetOutputSchema());
      auto val2 = order.second->Evaluate(&t2, GetOutputSchema());
      auto res = val1.CompareEquals(val2);
      if (res == CmpBool::CmpTrue) {
        continue;
      }
      if (res == CmpBool::CmpFalse) {
        if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
          return val1.CompareLessThan(val2) == CmpBool::CmpTrue;
        }
        return val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
      }
      return false;
    }
    return false;
  });
  it_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.end()) {
    return false;
  }
  *tuple = *it_;
  it_++;
  return true;
}

}  // namespace bustub
