#pragma once

#include <unordered_map>

#include "src/common/Type.hpp"
#include "src/execution/Aggregator.hpp"
#include "src/execution/AggregatorOp.hpp"
#include "src/storage/IntField.hpp"

class StringAggregator : public Aggregator {
   public:
    StringAggregator(int gb_field, const Type* gb_field_type, int acum_field,
                     const AggregatorOp op)
        : gb_field_(gb_field),
          gb_field_type_(gb_field_type),
          acum_field_(acum_field),
          op_(op),
          count_(0) {
        if (gb_field != NO_GROUPING) {
            td_ = std::make_shared<TupleDesc>(
                std::vector<const Type*>{gb_field_type, Type::INT_TYPE()});
        } else {
            td_ = std::make_shared<TupleDesc>(
                std::vector<const Type*>{Type::INT_TYPE()});
        }
    }

    std::shared_ptr<Tuple> next() override {
        auto res = tuples_[idx_];
        ++idx_;
        return res;
    }

    void rewind() override {
        if (idx_ == -1) {
            gen_tuples();
        }
        idx_ = 0;
    }

    bool has_next() override { return idx_ < tuples_.size(); }

    std::shared_ptr<TupleDesc> get_tuple_desc() override { return td_; }

    void merge_tuple_into_group(std::shared_ptr<Tuple> tuple) override {
        if (gb_field_ == NO_GROUPING) {
            ++count_;
        } else {
            ++count_map_[tuple->get_field(gb_field_)];
        }
    }

   private:
    void gen_tuples() {
        if (gb_field_ == NO_GROUPING) {
            auto t = std::make_shared<Tuple>(td_);
            t->set_field(0, std::make_shared<IntField>(count_));
            tuples_.push_back(std::move(t));
            return;
        }

        tuples_.reserve(count_map_.size());
        for (auto& [f, count] : count_map_) {
            auto t = std::make_shared<Tuple>(td_);
            t->set_field(0, f);
            t->set_field(1, std::make_shared<IntField>(count));
            tuples_.push_back(std::move(t));
        }
    }

    const int gb_field_, acum_field_;
    const AggregatorOp op_;
    const Type* gb_field_type_;

    std::unordered_map<std::shared_ptr<Field>, int> count_map_;
    int count_;
    std::shared_ptr<TupleDesc> td_;
    std::vector<std::shared_ptr<Tuple>> tuples_;
    int idx_ = -1;
};