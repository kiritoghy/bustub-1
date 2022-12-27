#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  auto comp = [this](const Tuple &t1, const Tuple &t2) -> bool {
    for (auto &order : plan_->GetOrderBy()) {
      auto order_type = order.first;
      auto val1 = order.second->Evaluate(&t1, GetOutputSchema());
      auto val2 = order.second->Evaluate(&t2, GetOutputSchema());
      auto res = val1.CompareEquals(val2);
      if (res == CmpBool::CmpTrue) {
        continue;
      } else if (res == CmpBool::CmpFalse) {
        if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
          return val1.CompareLessThan(val2) == CmpBool::CmpTrue;
        } else {
          return val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
        }
      } else {
        return false;
      }
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> pq(comp);
  while (child_executor_->Next(&tuple, &rid)) {
    if (pq.size() < plan_->GetN()) {
      pq.push(tuple);
    } else {
      if (comp(tuple, pq.top())) {
        pq.pop();
        pq.push(tuple);
      }
    }
  }
  while (!pq.empty()) {
    tuple_stack_.push(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuple_stack_.empty()) {
    return false;
  }
  *tuple = tuple_stack_.top();
  *rid = tuple->GetRid();
  tuple_stack_.pop();
  return true;
}

}  // namespace bustub
