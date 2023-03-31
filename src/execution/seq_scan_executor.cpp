//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // 获得被操作的表或索引
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // 实际存储数据的地方在tableheap中，table是管理tableheap的智能指针，用get可以得到裸指针
  table_ = table_info->table_.get();
  // 给迭代器初始化
  iterator_ = std::make_unique<TableIterator>(table_->Begin(exec_ctx_->GetTransaction()));
  // 该算子是对表进行扫描，所以应该锁全表
  try {
    // 因为实现锁时是会抛异常的，这里就应该在try中
    // 
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                table_info->oid_)) {
      throw ExecutionException("lock table share failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  try {
    if (!exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty()) {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !exec_ctx_->GetLockManager()->UnlockRow(
              exec_ctx_->GetTransaction(), exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_, *rid)) {
        throw ExecutionException("unlock row share failed");
      }
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("seq scan TransactionAbort");
  }
  if (*iterator_ != table_->End()) {
    try {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
          !exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_,
                                                (*(*iterator_)).GetRid())) {
        throw ExecutionException("lock row  intention share failed");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("seq scan TransactionAbort");
    }
    *tuple = *((*iterator_)++);
    *rid = tuple->GetRid();

    return true;
  }
  return false;
}

}  // namespace bustub
