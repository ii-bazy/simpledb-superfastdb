#pragma once

#include <glog/logging.h>

#include <memory>

#include "src/execution/OpIterator.hpp"

class Operator : public OpIterator {
   public:
    virtual bool has_next() override {
        if (next_tuple_ == nullptr) {
            next_tuple_ = fetch_next();
        }

        return next_tuple_ != nullptr;
    }

    virtual std::shared_ptr<Tuple> next() override {
        if (next_tuple_ == nullptr) {
            throw std::invalid_argument(
                "Could not fetch next tuple in next() call");
        }

        std::shared_ptr<Tuple> result = std::move(next_tuple_);
        next_tuple_ = nullptr;
        return result;
    }

    virtual std::shared_ptr<Tuple> fetch_next() = 0;

    virtual ~Operator() = default;

   protected:
    std::shared_ptr<Tuple> next_tuple_ = nullptr;
};