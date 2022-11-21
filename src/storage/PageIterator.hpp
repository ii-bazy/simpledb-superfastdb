#pragma once

#include <memory>

#include "src/storage/Tuple.hpp"

class PageIterator {
   public:
    virtual bool has_next() = 0;
    virtual std::shared_ptr<Tuple> next() = 0;
    virtual void rewind() = 0;
    virtual ~PageIterator() = default;
};