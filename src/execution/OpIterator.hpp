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
};