r#pragma once

#include <memory>

#include "src/execution/Aggregator.hpp"
#include "src/execution/AggregatorOp.hpp"
#include "src/execution/Operator.hpp"

#include <glog/logging.h>
#include <absl/strings/str_cat.h>

class Aggregate : public OpIterator {
   public:
    Aggregate(std::unique_ptr<OpIterator> child, int agg_field, int group_field,
              AggregatorOp op)
        : child_{std::move(child)},
          agg_field_{agg_field},
          group_field_{group_field},
          op_{op} {
        std::string aggregate_field_name =
            to_string(op_) + "(" +
            child_->get_tuple_desc()->get_field_name(agg_field_) + ")";

        if (group_field_ == Aggregator::NO_GROUPING) {
            td_ = std::make_shared<TupleDesc>(
                std::vector<const Type*>{Type::INT_TYPE()},
                std::vector<std::string>{aggregate_field_name});
        } else {
            td_ = std::make_shared<TupleDesc>(
                std::vector<const Type*>{
                    child_->get_tuple_desc()->get_field_type(group_field_),
                    Type::INT_TYPE()},
                std::vector<std::string>{
                    child_->get_tuple_desc()->get_field_name(group_field_),
                    aggregate_field_name});
        }

        const Type* group_type = (group_field_ == Aggregator::NO_GROUPING ? nullptr : child_->get_tuple_desc()->get_field_type(group_field_));

        if (child_->get_tuple_desc()->get_field_type(agg_field_) == Type::INT_TYPE()) {
            aggregator_ = std::make_unique<IntegerAggregator>(group_field_, group_type, agg_field_, op);
        } else {
            aggregator_ = std::make_unique<StringAggregator>(group_field_, group_type, agg_field_, op);
        }

        for (auto t : *child_) {
            aggregator_->merge_tuple_into_group(std::move(t));
        }
    }

    int group_field() const { return group_field_; }

    std::string group_field_name() const {
        if (group_field_ != Aggregator::NO_GROUPING) {
            return child_->get_tuple_desc()->get_field_name(group_field_);
        }

        return "";
    }

    std::shared_ptr<Tuple> next() override { return aggregator_->next(); }

    void rewind() override { aggregator_->rewind(); }

    bool has_next() override { return aggregator_->has_next(); }

    std::shared_ptr<TupleDesc> get_tuple_desc() override { return td_; }

   private:
    std::unique_ptr<OpIterator> child_ = nullptr;
    const int agg_field_;
    const int group_field_;
    const AggregatorOp op_;
    std::unique_ptr<Aggregator> aggregator_ = nullptr;
    std::shared_ptr<TupleDesc> td_ = nullptr;
};