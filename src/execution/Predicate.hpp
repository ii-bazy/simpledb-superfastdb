#pragma once

#include "src/execution/OpType.hpp"
#include "src/storage/Field.hpp"
#include "src/storage/Tuple.hpp"

class Predicate {
   public:
    Predicate(const int field_index, const OpType op, const Field* operand)
        : field_index_{field_index}, op_{op}, operand_{operand} {}

    int get_field_index() const { return field_index_; }

    OpType get_op_type() const { return op_; }

    const Field* get_operand() const { return operand_; }

    bool apply(const Tuple* t) const {
        return t->get_field(field_index_)->compare(op_, operand_);
    }

    std::string to_string() const {
        return absl::StrCat("col= ", field_index_, " ", ::to_string(op_), " ",
                            operand_->to_string());
    }

   private:
    const int field_index_;
    const OpType op_;
    const Field* operand_;
};