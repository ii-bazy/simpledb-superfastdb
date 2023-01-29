#pragma once

#include <glog/logging.h>

#include <limits>
#include <unordered_map>

#include "src/common/Type.hpp"
#include "src/execution/Aggregator.hpp"
#include "src/execution/AggregatorOp.hpp"
#include "src/storage/IntField.hpp"

class IntegerAggregator : public Aggregator {
   public:
    IntegerAggregator(const int gb_field, const Type* gb_field_type,
                      const int acum_field, const AggregatorOp op)
        : gb_field_{gb_field},
          acum_field_{acum_field},
          op_{op} {
        switch (op_) {
            case AggregatorOp::NONE:
                throw std::invalid_argument("Int Aggregation over NONE operator");
            case AggregatorOp::MIN:
                accumulator_value_ = std::numeric_limits<int>::max();
                break;
            case AggregatorOp::MAX:
                accumulator_value_ = std::numeric_limits<int>::min();
                break;
            case AggregatorOp::SUM:
            case AggregatorOp::AVG:
            case AggregatorOp::COUNT:
            case AggregatorOp::SUM_COUNT:
            case AggregatorOp::SC_AVG:
                accumulator_value_ = 0;
                break;
        }

        if (gb_field != NO_GROUPING) {
            td_ = std::make_shared<TupleDesc>(
                std::vector<const Type*>{gb_field_type, Type::INT_TYPE()});
        } else {
            td_ = std::make_shared<TupleDesc>(
                std::vector<const Type*>{Type::INT_TYPE()});
        }
    }

    void merge_tuple_into_group(std::shared_ptr<Tuple> t) override {
        const auto field_ptr =
            dynamic_cast<IntField*>(t->get_field(acum_field_).get());

        if (field_ptr == nullptr) {
            throw std::invalid_argument(
                "IntegerAggregator: wrong field type to acumulate.");
        }

        const int field_value = field_ptr->get_value();

        int& acumulator = (gb_field_ != NO_GROUPING
                               ? accumulator_map_[t->get_field(gb_field_)]
                               : accumulator_value_);
        int& acumulator_helper =
            (gb_field_ != NO_GROUPING
                 ? accumulator_helper_map_[t->get_field(gb_field_)]
                 : accumulator_helper_);

        switch (op_) {
            case AggregatorOp::NONE:
                throw std::invalid_argument("String Aggregation over NONE operator");
            case AggregatorOp::MIN:
                accumulator_value_ = std::min(acumulator, field_value);
                break;
            case AggregatorOp::MAX:
                accumulator_value_ = std::max(acumulator, field_value);
                break;
            case AggregatorOp::SUM:
                accumulator_value_ = acumulator += field_value;
                break;
            case AggregatorOp::AVG:
                acumulator += field_value;
                acumulator_helper += 1;
                break;
            case AggregatorOp::COUNT:
                acumulator += 1;
                break;
            default:
                throw std::invalid_argument("siema");
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

    bool has_next() override { return idx_ < (int)tuples_.size(); }

   private:
    const int gb_field_, acum_field_;
    const AggregatorOp op_;

    std::shared_ptr<TupleDesc> td_;
    std::vector<std::shared_ptr<Tuple>> tuples_;

    std::unordered_map<std::shared_ptr<Field>, int> accumulator_map_;
    std::unordered_map<std::shared_ptr<Field>, int> accumulator_helper_map_;
    int accumulator_value_ = 0;
    int accumulator_helper_ = 0;
    int idx_ = -1;

    void gen_tuples() {
        LOG(INFO) << "Gen tuples Int";

        if (gb_field_ == NO_GROUPING) {
            auto t = std::make_shared<Tuple>(td_);
            t->set_field(
                0, std::make_shared<IntField>(op_ != AggregatorOp::AVG
                                                  ? accumulator_value_
                                                  : accumulator_value_ /
                                                        accumulator_helper_));

            tuples_.push_back(t);
            LOG(INFO) << "tuples_" << tuples_.size() << "\n";
            return;
        }

        tuples_.reserve(accumulator_map_.size());

        for (auto& [f, val] : accumulator_map_) {
            int value = 0;
            if (op_ != AggregatorOp::AVG) {
                value = val;
            } else {
                value = val / accumulator_helper_map_[f];
            }

            auto t = std::make_shared<Tuple>(td_);
            t->set_field(0, f);
            t->set_field(1, std::make_shared<IntField>(value));
            tuples_.push_back(std::move(t));
        }
    }
};