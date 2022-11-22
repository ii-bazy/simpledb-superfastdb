#pragma once

#include <memory>

#include "src/storage/Tuple.hpp"
#include "src/transaction/TransactionId.hpp"
#include "src/utils/status_macros.hpp"

class DbFileIterator {
   public:
    virtual absl::StatusOr<bool> has_next(TransactionId tid) = 0;
    virtual absl::StatusOr<std::shared_ptr<Tuple>> next() = 0;
    virtual absl::Status rewind(TransactionId tid) = 0;
    virtual ~DbFileIterator() = default;
};