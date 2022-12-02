#pragma once

#include "src/storage/PageId.hpp"
#include "src/storage/PageIterator.hpp"
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

    virtual int get_num_unused_slots() const = 0;
    virtual std::shared_ptr<PageId> get_id() const = 0;
    virtual void insert_tuple(std::shared_ptr<Tuple> t) = 0;
    virtual void delete_tuple(std::shared_ptr<Tuple> t) = 0;
    virtual static std::vector<char> create_empty_page_data() = 0;

    virtual std::unique_ptr<PageIterator> iterator() override;

    virtual std::unique_ptr<PageIterator> iterator() = 0;
    virtual ~Page() = default;
};