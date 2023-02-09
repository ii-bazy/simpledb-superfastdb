#pragma once
#include "src/execution/OpIterator.hpp"

class Aggregator : public OpIterator {
   public:
    constexpr static int NO_GROUPING = -1;

    virtual std::shared_ptr<TupleDesc> get_tuple_desc() override {
        throw std::invalid_argument("SHOULD NOT ASK AGGREGATOR FOR TUPLE_DESC");
    }

    virtual void merge_tuple_into_group(std::shared_ptr<Tuple> tuple) = 0;
};