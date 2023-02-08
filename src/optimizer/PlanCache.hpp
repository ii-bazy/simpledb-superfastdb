#pragma once

#include <map>
#include <set>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "src/execution/logical_plan/logical_plan.hpp"

class PlanCache {
   public:
    PlanCache(uint64_t max_size) { best_cost_cards_.resize(max_size); }

    void addPlan(uint64_t s, CostCard card) {
        best_cost_cards_[s] = std::move(card);
    }

    CostCard& getCostCard(uint64_t s) { return best_cost_cards_[s]; }

   private:
    std::vector<CostCard> best_cost_cards_;
};