#pragma once

#include <glog/logging.h>

#include <memory>

#include "src/storage/Tuple.hpp"
#include "src/utils/status_macros.hpp"

class OpIterator {
   public:
    // virtual void open() = 0;
    // virtual void close() = 0;
    virtual absl::StatusOr<std::shared_ptr<Tuple>> next() = 0;
    virtual absl::Status rewind() = 0;
    virtual absl::StatusOr<bool> has_next() = 0;
    virtual std::shared_ptr<TupleDesc> get_tuple_desc() const = 0;

    struct Sentinel {};

    class Iterator {
       public:
        Iterator(OpIterator* op) : op_(op) {
            auto status = op_->rewind();
            if (!status.ok()) {
                // I have to crash here :(
                LOG(ERROR) << "rewind() failed in iterator: " << status;
                abort();
            }
        }

        Iterator& operator++() {
            auto nxt = op_->has_next();
            if (!nxt.ok()) {
                // I have to crash here :(
                LOG(ERROR) << "has_next() failed in iterator: " << nxt.status();
                std::abort();
            }
            return *this;
        }

        void operator++(int) { ++(*this); }

        absl::StatusOr<std::shared_ptr<Tuple>> operator*() const {
            return op_->next();
        }

        bool operator==(const Sentinel&) {
            auto nxt = op_->has_next();
            if (!nxt.ok()) {
                // I have to crash here :(
                LOG(ERROR) << "has_next() failed in iterator: " << nxt.status();
                std::abort();
            }
            return !*nxt;
        }
        bool operator!=(const Sentinel&) {
            auto nxt = op_->has_next();
            if (!nxt.ok()) {
                // I have to crash here :(
                LOG(ERROR) << "has_next() failed in iterator: " << nxt.status();
                std::abort();
            }
            return *nxt;
        }

       private:
        OpIterator* op_;
    };

    auto begin() { return Iterator(this); }

    auto end() { return Sentinel(); };

    virtual ~OpIterator() = default;
};