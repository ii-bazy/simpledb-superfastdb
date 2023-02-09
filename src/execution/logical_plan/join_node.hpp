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
    std::shared_ptr<LogicalPlan> subplan;
    OpType op;
    Type type;

    JoinNode(ColumnRef lref, OpType op, ColumnRef rref)
        : lref(lref), rref(rref), subplan(nullptr), op(op), type(COL_COL) {}

    JoinNode(ColumnRef lref, OpType op, std::shared_ptr<LogicalPlan> subplan)
        : lref(lref), subplan(std::move(subplan)), op(op), type(COL_SUB) {}

    JoinNode swapInnerOuter() {
        OpType new_op = op;

        if (op == OpType::GREATER_THAN)
            new_op = OpType::LESS_THAN;
        else if (op == OpType::GREATER_THAN_OR_EQ)
            new_op = OpType::LESS_THAN_OR_EQ;
        else if (op == OpType::LESS_THAN)
            new_op = OpType::GREATER_THAN;
        else if (op == OpType::LESS_THAN_OR_EQ)
            new_op = OpType::GREATER_THAN_OR_EQ;

        auto ret_val = JoinNode(rref, new_op, lref);
        ret_val.subplan = subplan;
        return ret_val;
    }

    operator std::string() const {
        return std::string(lref) + " " + to_string(op) + " " + std::string(rref);
    }

    bool operator==(const JoinNode& other) const {
        return lref == other.lref && rref == other.rref &&
               subplan == other.subplan && op == other.op && type == other.type;
    }

    template <typename H>
    friend H AbslHashValue(H h, const JoinNode& ref) {
        return H::combine(std::move(h), ref.lref, ref.rref, ref.subplan, ref.op,
                          ref.type);
    }
};