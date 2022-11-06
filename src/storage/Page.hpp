#pragma once

#include "src/storage/PageId.hpp"
#include "src/storage/Tuple.hpp"
#include "src/transaction/TransactionId.hpp"

class Page {
   public:
    virtual std::shared_ptr<PageId> get_id() const = 0;
    // virtual TransactionId* isDirty() = 0;
    // virtual void markDirty(bool dirty, TransactionId* tid) = 0;
    // virtual std::vector<char> get_page_data() = 0;
    // virtual Page* get_before_image() = 0;
    // virtual void set_before_image() = 0;

    virtual bool has_next() = 0;
    virtual std::shared_ptr<Tuple> next() = 0;
    virtual void rewind() = 0;
};