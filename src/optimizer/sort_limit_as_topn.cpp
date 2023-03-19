#include <memory>
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(limit_plan.children_.size() == 1, "SLAT should have exactly 1 children.");
    if (limit_plan.GetChildAt(0)->GetType() == PlanType::Sort) {
      const auto &child = dynamic_cast<const SortPlanNode &>(*limit_plan.GetChildAt(0));
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, child.GetChildPlan(), child.GetOrderBy(),
                                            limit_plan.GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
