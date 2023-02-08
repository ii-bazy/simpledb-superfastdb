#pragma once

#include <vector>

#include "src/execution/logical_plan/join_node.hpp"

struct CostCard {
    double cost = 0.0;
    double card = 0.0;
    std::vector<JoinNode> plan;
};