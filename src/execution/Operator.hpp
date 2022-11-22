#pragma once

#include <glog/logging.h>

#include <memory>

#include "src/execution/OpIterator.hpp"

class Operator : public OpIterator {
   public:
    virtual absl::StatusOr<bool> has_next() override {
        if (next_tuple_ == nullptr) {
            ASSIGN_OR_RETURN(next_tuple_, fetch_next());
        }

        return next_tuple_ != nullptr;
    }

    virtual absl::StatusOr<std::shared_ptr<Tuple>> next() override {
        if (next_tuple_ == nullptr) {
            return absl::InvalidArgumentError(
                "Could not fetch next tuple in next() call");
        }

        std::shared_ptr<Tuple> result = std::move(next_tuple_);
        next_tuple_ = nullptr;
        return result;
    }

    virtual absl::StatusOr<std::shared_ptr<Tuple>> fetch_next() = 0;
    virtual absl::Status rewind() override = 0;
    virtual std::shared_ptr<TupleDesc> get_tuple_desc() const override = 0;

   protected:
    std::shared_ptr<Tuple> next_tuple_ = nullptr;
};