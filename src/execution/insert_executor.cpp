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

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_name_ = table_info_->name_;
  table_heap_ = table_info_->table_.get();
  iterator_ = std::make_unique<TableIterator>(table_heap_->Begin(exec_ctx_->GetTransaction()));
  child_executor_->Init();
  try {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                table_info_->oid_)) {
      throw ExecutionException("lock table intention exclusive failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("insert TransactionAbort");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (successful_) {
    return false;
  }
  int count = 0;
  while (child_executor_->Next(tuple, rid)) {
    if (table_heap_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
      try {
        if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                  table_info_->oid_, *rid)) {
          throw ExecutionException("lock row  exclusive failed");
        }
      } catch (TransactionAbortException &e) {
        throw ExecutionException("insert TransactionAbort");
      }
      auto indexs = exec_ctx_->GetCatalog()->GetTableIndexes(table_name_);
      for (auto index : indexs) {
        auto key = (*tuple).KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      }
      count++;
    }
  }
  std::vector<Value> value;
  value.emplace_back(INTEGER, count);
  Schema schema(plan_->OutputSchema());
  *tuple = Tuple(value, &schema);
  successful_ = true;
  return true;
}

}  // namespace bustub
