#pragma once

#include <memory>

#include "src/storage/Tuple.hpp"
#include "src/utils/status_macros.hpp"

class PageIterator {
   public:
    virtual absl::StatusOr<bool> has_next() = 0;
    virtual absl::StatusOr<std::shared_ptr<Tuple>> next() = 0;
    virtual absl::Status rewind() = 0;
    virtual ~PageIterator() = default;
};