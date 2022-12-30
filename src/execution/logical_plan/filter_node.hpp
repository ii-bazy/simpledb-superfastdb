#pragma once

#include "src/storage/Field.hpp"
#include "src/execution/logical_plan/column_ref.hpp"
#include "src/execution/OpType.hpp"

class FilterNode {
   public:
    enum FilterType {
        TWO_COLUMNS,
        COLUMN_CONSTANT,
    };

    ColumnRef lcol;
    ColumnRef rcol;
    std::unique_ptr<Field> constant;
    OpType op;
    FilterType type;

    FilterNode(ColumnRef lcol, OpType op, ColumnRef rcol)
        : lcol(lcol), rcol(rcol), op(op), type(TWO_COLUMNS) {}

    FilterNode(ColumnRef lcol, OpType op, std::unique_ptr<Field> constant)
        : lcol(lcol),
          rcol(ColumnRef("", "")),
          constant(std::move(constant)),
          op(op),
          type(COLUMN_CONSTANT) {}

    operator std::string() const {
        switch (type) {
            case TWO_COLUMNS:
                return static_cast<std::string>(lcol) + " " +
                       to_string(op) + " " +
                       static_cast<std::string>(rcol);
            case COLUMN_CONSTANT:
                return static_cast<std::string>(lcol) + " " +
                       to_string(op) + " " +
                       constant->to_string();
        }
    }
};