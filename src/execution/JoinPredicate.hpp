#pragma once

#include "src/execution/OpType.hpp"
#include "src/storage/Tuple.hpp"

class JoinPredicate {
   public:
    JoinPredicate(int field1_index, OpType op, int field2_index)
        : field1_index_(field1_index), op_(op), field2_index_(field2_index) {}

    bool apply(const Tuple* t1, const Tuple* t2) const {
        return t1->get_field(field1_index_)
            ->compare(op_, t2->get_field(field2_index_).get());
    }

    int get_field1_index() const { return field1_index_; }

    int get_field2_index() const { return field1_index_; }

    OpType get_op_type() const { return op_; }

    std::string to_string() const {
        return absl::StrCat("col= ", field1_index_, " ", ::to_string(op_),
                            " col= ", field2_index_);
    }

   private:
    const int field1_index_;
    const OpType op_;
    const int field2_index_;
};