#pragma once

#include <vector>

#include "src/execution/logical_plan/join_node.hpp"

struct CostCard {
    double cost;
    double card;
    std::vector<JoinNode> plan;
};