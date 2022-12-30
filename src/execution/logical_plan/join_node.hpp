#pragma once

#include "src/execution/OpType.hpp"
#include "src/execution/logical_plan/column_ref.hpp"

class LogicalPlan;

class JoinNode {
   public:
    enum Type {
        COL_COL,
        COL_SUB,
    };

    ColumnRef lref;
    ColumnRef rref;
    std::unique_ptr<LogicalPlan> subplan;
    OpType op;
    Type type;

    JoinNode(ColumnRef lref, OpType op, ColumnRef rref)
        : lref(lref), rref(rref), op(op), type(COL_COL) {}

    JoinNode(ColumnRef lref, OpType op, std::unique_ptr<LogicalPlan> subplan)
        : lref(lref), subplan(std::move(subplan)), op(op), type(COL_SUB) {}
};