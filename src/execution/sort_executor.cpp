#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_{std::move(child_executor)} {}

void SortExecutor::Init() {
  child_->Init();
  Tuple tuple{};
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  // 将tuples排序
  std::sort(
      tuples_.begin(), tuples_.end(),
      [order_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &tuple_a, const Tuple &tuple_b) {
        for (const auto &order_key : order_bys) {
          switch (order_key.first) {
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              break;
            case OrderByType::DESC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              break;
          }
        }
        return false;
      });

  iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }

  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;

  return true;
}

}  // namespace bustub
