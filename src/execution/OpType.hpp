#pragma once

#include <string>

enum class OpType {
    EQUALS,
    GREATER_THAN,
    LESS_THAN,
    LESS_THAN_OR_EQ,
    GREATER_THAN_OR_EQ,
    LIKE,
    NOT_EQUALS
};

std::string to_string(const OpType op);

OpType flip_op_type(const OpType op);