#pragma once

#include <map>
#include <set>
#include "src/execution/logical_plan/logical_plan.hpp"

class PlanCache {
public:
    void addPlan(std::set<JoinNode>& s, double cost, int card, std::vector<JoinNode>& order) {
        best_orders_[s] = order;
        bestCosts_[s] = cost;
        bestCardinalities_[s] = card;
    }

    std::vector<JoinNode> getOrder(std::set<JoinNode>& s) {
        return best_orders_[s];
    }

    double getCost(std::set<JoinNode> s) {
        return bestCosts_[s];
    }

    int getCard(std::set<JoinNode>& s) {
        return bestCardinalities_[s];
    }

private:
    std::map<std::set<JoinNode>, std::vector<JoinNode>> best_orders_;
    std::map<std::set<JoinNode>, double> bestCosts_;
    std::map<std::set<JoinNode>, int> bestCardinalities_;
};