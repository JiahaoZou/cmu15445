//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  index_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  iter_ = std::make_unique<BPlusTreeIndexIteratorForOneIntegerColumn>(index_->GetBeginIterator());
  table_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*iter_ != index_->GetEndIterator()) {
    *rid = (*(*iter_)).second;
    if (table_->GetTuple(*rid, tuple, this->GetExecutorContext()->GetTransaction())) {
      iter_->operator++();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
