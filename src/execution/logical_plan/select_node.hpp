#pragma once

#include "src/execution/logical_plan/column_ref.hpp"

class SelectNode {
   public:
    ColumnRef ref;

    SelectNode(ColumnRef ref) : ref(ref) {}

    operator std::string() const { return static_cast<std::string>(ref); }
};
