#pragma once

#include <algorithm>
#include <memory>

#include "src/execution/OpIterator.hpp"

class OrderBy : public OpIterator {
   public:
    enum Order {
        ASCENDING,
        DESCENDING,
    };

    OrderBy(const int order_by_field, Order order,
            std::unique_ptr<OpIterator>& child)
        : child_{std::move(child)},
          order_{order},
          order_by_field_{order_by_field} {
        for (const auto& tuple : *child_) {
            child_tuples_.push_back(tuple);
        }

        OpType op = order == Order::ASCENDING ? OpType::LESS_THAN_OR_EQ
                                              : OpType::GREATER_THAN_OR_EQ;

        std::sort(child_tuples_.begin(), child_tuples_.end(),
                  [this, op](const std::shared_ptr<Tuple>& c_1,
                             const std::shared_ptr<Tuple>& c_2) {
                      return c_1->get_field(order_by_field_)
                          ->compare(op, c_2->get_field(order_by_field_).get());
                  });
    }

    std::shared_ptr<Tuple> next() { return child_tuples_[child_index_++]; }

    void rewind() { child_index_ = 0; }

    bool has_next() {
        return child_index_ < static_cast<int>(child_tuples_.size());
    }

    std::shared_ptr<TupleDesc> get_tuple_desc() {
        return child_->get_tuple_desc();
    }

   private:
    std::unique_ptr<OpIterator> child_;
    const Order order_;
    const int order_by_field_;

    std::vector<std::shared_ptr<Tuple>> child_tuples_;

    int child_index_ = 0;
};