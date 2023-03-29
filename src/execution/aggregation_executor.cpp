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
      aht_iterator_(aht_.Begin()),
      cnt_(0) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  // 我们可以从plan中拿到groupby信息，如果在sql中没有指定groupby，则就是默认所有数据
  // 此时hashmap中只会有一项
  // 这里的key是groupby项的组合，是一个vector，value是统计项的组合，也是一个value
  while (child_->Next(&tuple, &rid)) {
    auto aggregate_key = MakeAggregateKey(&tuple);
    auto aggregate_value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggregate_key, aggregate_value);
  }
  aht_iterator_ = aht_.Begin();
}

// 有可能child传上来的数据为空值，此时我们的aht迭代器一开始就为END
// 这个时候应该向aht中加入一条空的值
// 目前还有问题，当没有groupby时，且表为空表时，count（*）会出错
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Schema s(*plan_->output_schema_);
  if (aht_iterator_ == aht_.End()) {
    /*
    if (cnt_ == 0){
      Tuple tmp_tuple;
      auto aggregate_key = MakeAggregateKey(&tmp_tuple);
      std::vector<Value> v(aht_iterator_.Key().group_bys_);
      for (const auto &agg_type : plan_->agg_types_) {
        switch (agg_type) {
          case AggregationType::CountStarAggregate:
            // Count start starts at zero.
            v.emplace_back(ValueFactory::GetIntegerValue(0));
            break;
          case AggregationType::CountAggregate:
          case AggregationType::SumAggregate:
          case AggregationType::MinAggregate:
          case AggregationType::MaxAggregate:
            // Others starts at null.
            v.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            break;
        }
      }
      *tuple = {v, &s};
      ++cnt_;
      return true;
    }
    */
    return false;
  }
  // 返回的tuple的形式为group-bys + aggregates
  std::vector<Value> v(aht_iterator_.Key().group_bys_);
  for (const auto &i : aht_iterator_.Val().aggregates_) {
    v.push_back(i);
  }
  *tuple = {v, &s};
  ++cnt_;
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
