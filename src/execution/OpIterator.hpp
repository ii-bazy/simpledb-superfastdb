#pragma once

#include <memory>

#include "src/storage/Tuple.hpp"

class OpIterator {
   public:
    // virtual void open() = 0;
    // virtual void close() = 0;
    virtual std::shared_ptr<Tuple> next() = 0;
    virtual void rewind() = 0;
    virtual bool has_next() = 0;
    virtual std::shared_ptr<TupleDesc> get_tuple_desc() = 0;

    struct Sentinel {};

    class Iterator {
       public:
        Iterator(OpIterator* op) : op_(op) { op_->rewind(); }

        Iterator& operator++() {
            op_->has_next();
            return *this;
        }

        void operator++(int) { ++(*this); }

        std::shared_ptr<Tuple> operator*() const { return op_->next(); }

        bool operator==(const Sentinel&) { return !op_->has_next(); }
        bool operator!=(const Sentinel&) { return op_->has_next(); }

       private:
        OpIterator* op_;
    };

    auto begin() { return Iterator(this); }
    auto end() { return Sentinel(); };
    virtual ~OpIterator() = default;
};