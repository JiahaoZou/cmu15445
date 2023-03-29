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

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  if (plan_->GetJoinType() == JoinType::INNER) {
    is_ineer_ = true;
  }
}

void NestedLoopJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();
  // 先把右孩子的所有tuple全取得
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 右边数据为空
  if (right_tuples_.empty()) {
    if (is_ineer_) {
      return false;
    }
    // leftjoin
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
      return false;
    }
    std::vector<Value> value;
    for (uint32_t i = 0; i < left_schema_.GetColumns().size(); i++) {
      value.push_back(left_tuple_.GetValue(&left_schema_, i));
    }
    for (uint32_t i = 0; i < right_schema_.GetColumns().size(); i++) {
      value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
    }
    *tuple = {value, &GetOutputSchema()};
    return true;
  }
  // index_为0的情况只有可能是第一次调用next
  if (index_ == 0) {
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
      return false;
    }
  }
  while (true) {
    // 查看index是否到了最后
    if (index_ == right_tuples_.size()) {
      index_ = 0;
      is_match_ = false;
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        return false;
      }
    }
    // 没有到最后，或是取得了新的tuple
    for (; index_ < right_tuples_.size(); ++index_) {
      auto cmp_value =
          plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[index_], right_schema_);
      if (!cmp_value.IsNull() && cmp_value.GetAs<bool>()) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumns().size(); ++i) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumns().size(); ++i) {
          value.push_back(right_tuples_[index_].GetValue(&right_schema_, i));
        }
        *tuple = {value, &GetOutputSchema()};
        is_match_ = true;
        ++index_;
        return true;
      }
    }
    // 上一个for没有匹配到,此时index必定为right_tuples_.size()
    // 有两种情况，inner和leftjoin
    // 1.leftjoin,此时left相应的项应该为null
    // std::cout << "index" << index_ << right_tuples_.size() <<std::endl;
    if (!is_ineer_) {
      // 如果此条之前已经匹配成功过，则不需再发射
      if (is_match_) {
        is_match_ = false;
        continue;
      }
      // std::cout << "444444" <<std::endl;
      std::vector<Value> value;
      for (uint32_t i = 0; i < left_schema_.GetColumns().size(); i++) {
        value.push_back(left_tuple_.GetValue(&left_schema_, i));
      }
      for (uint32_t i = 0; i < right_schema_.GetColumns().size(); i++) {
        value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
      }
      *tuple = {value, &GetOutputSchema()};
      return true;
    }
    // 2.innerjoin, 没有匹配到，此条不应该发射
    // 直接返回while最前端重新从leftchild处获取一条
  }
}

}  // namespace bustub
