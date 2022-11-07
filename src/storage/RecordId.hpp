#pragma once

#include <memory>

#include "src/storage/PageId.hpp"

class RecordId {
   public:
    RecordId(const std::shared_ptr<PageId>& pid, int tupleno)
        : pid_(pid.get()), tupleno_(tupleno) {}

    int get_tuple_number() const { return tupleno_; }

    const PageId* get_page_id() const { return pid_; }

   private:
    // TODO: czy to musi byc shared_ptr?
    const PageId* pid_;
    const int tupleno_;
};