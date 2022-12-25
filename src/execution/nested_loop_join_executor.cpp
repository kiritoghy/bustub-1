//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      right_tuples_(),
      right_tuple_index_(0),
      left_tuple_(),
      is_joined_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }
  // right_tuple_index_ = 0;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (right_tuple_index_ == 0) {
    if (!left_executor_->Next(&left_tuple_, rid)) {
      return false;
    }
    // LOG_DEBUG("fetch left tuple");
  }
  // LOG_DEBUG("right tuple index:%d", right_tuple_index_);
  do {
    // LOG_DEBUG("right tuple size:%zu", right_tuples_.size());
    for (; right_tuple_index_ < right_tuples_.size(); ++right_tuple_index_) {
      // LOG_DEBUG("right tuple index:%d", right_tuple_index_);
      // LOG_DEBUG("left tuple:%s", left_tuple_.ToString(&left_executor_->GetOutputSchema()).c_str());
      // LOG_DEBUG("right tuple:%s",
      //           right_tuples_[right_tuple_index_].ToString(&right_executor_->GetOutputSchema()).c_str());
      auto res =
          plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                          &right_tuples_[right_tuple_index_], right_executor_->GetOutputSchema());
      if (!res.IsNull() && res.GetAs<bool>()) {
        std::vector<Value> res;
        res.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          res.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          res.emplace_back(right_tuples_[right_tuple_index_].GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = Tuple(res, &GetOutputSchema());
        *rid = tuple->GetRid();
        // LOG_DEBUG("successful combine");
        ++right_tuple_index_;
        is_joined_ = true;
        return true;
      }
    }
    if (!is_joined_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> res;
      res.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        res.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        // Value value(Value(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        // value = value.OperateNull(value);
        res.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(res, &GetOutputSchema());
      *rid = tuple->GetRid();
      right_tuple_index_ = 0;
      is_joined_ = false;
      return true;
    }
    right_tuple_index_ = 0;
    is_joined_ = false;
  } while (left_executor_->Next(&left_tuple_, rid));
  return false;
}

}  // namespace bustub
