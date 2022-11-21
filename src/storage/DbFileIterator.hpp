#pragma once

#include <memory>

#include "src/storage/Tuple.hpp"
#include "src/transaction/TransactionId.hpp"

class DbFileIterator {
   public:
    virtual bool has_next(TransactionId tid) = 0;
    virtual std::shared_ptr<Tuple> next() = 0;
    virtual void rewind(TransactionId tid) = 0;
    virtual ~DbFileIterator() = default;
};