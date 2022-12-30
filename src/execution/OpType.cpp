#include "src/execution/OpType.hpp"

std::string to_string(const OpType op) {
    using enum OpType;

    switch (op) {
        case EQUALS:
            return "=";
        case GREATER_THAN:
            return ">";
        case LESS_THAN:
            return "<";
        case LESS_THAN_OR_EQ:
            return "<=";
        case GREATER_THAN_OR_EQ:
            return ">=";
        case LIKE:
            return "~";
        case NOT_EQUALS:
            return "!=";
    }
}

OpType flip_op_type(const OpType op) {
    using enum OpType;
    
    switch (op) {
        case LESS_THAN:
            return GREATER_THAN;
        case GREATER_THAN:
            return LESS_THAN;
        case GREATER_THAN_OR_EQ:
            return LESS_THAN_OR_EQ;
        case LESS_THAN_OR_EQ:
            return GREATER_THAN_OR_EQ;
        default:
            return op;
    }
}