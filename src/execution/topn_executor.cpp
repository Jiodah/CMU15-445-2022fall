#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      bool default_order_by = (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC);
      if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
              .CompareLessThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
        return default_order_by;
      }
      if (expr->Evaluate(&a, child_executor_->GetOutputSchema())
              .CompareGreaterThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())) == CmpBool::CmpTrue) {
        return !default_order_by;
      }
    }
    return true;
  });
  iterator_ = sorted_tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ < plan_->GetN() && iterator_ != sorted_tuples_.end()) {
    index_++;
    *tuple = *iterator_;
    iterator_++;
    return true;
  }
  return false;
}

}  // namespace bustub
