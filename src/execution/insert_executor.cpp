//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}
// insertexecuter不会是一个叶节点，其必定有子节点，其数据来源于子节点
void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_name_ = table_info_->name_;
  table_ = table_info_->table_.get();
  iterator_ = std::make_unique<TableIterator>(table_->Begin(this->GetExecutorContext()->GetTransaction()));
  child_executor_->Init();
  try {
    // std::cout<<exec_ctx_->GetTransaction()->GetTransactionId()<<" insert satrt"<<std::endl;
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                table_info_->oid_)) {
      throw ExecutionException("lock table intention exclusive failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("insert TransactionAbort");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (issucessful_) {
    return false;
  }
  Tuple tup;
  RID r;
  std::vector<Value> num;
  int s = 0;
  std::vector<Column> c;

  c.emplace_back("", INTEGER);
  Schema schema(c);
  while (child_executor_->Next(&tup, &r)) {
    // 循环从子节点处获得数据，获得了数据后根据oid将整个表给锁起来
    if (table_->InsertTuple(tup, &r, this->GetExecutorContext()->GetTransaction())) {
      try {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                  table_info_->oid_, r)) {
          throw ExecutionException("lock row  exclusive failed");
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("insert TransactionAbort");
      }
      auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_name_);
      for (auto i : indexs) {
        auto key = tup.KeyFromTuple(table_info_->schema_, i->key_schema_, i->index_->GetKeyAttrs());
        i->index_->InsertEntry(key, r, this->GetExecutorContext()->GetTransaction());
      }
      s++;
    }
  }
  Value v(INTEGER, s);
  num.push_back(v);
  Tuple t(num, &schema);
  *tuple = t;
  issucessful_ = true;
  return true;
}

}  // namespace bustub
