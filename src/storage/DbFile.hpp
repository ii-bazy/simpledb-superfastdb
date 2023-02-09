#pragma once

#include <memory>

#include "src/storage/DbFileIterator.hpp"
#include "src/storage/Page.hpp"
#include "src/storage/PageId.hpp"
#include "src/storage/TupleDesc.hpp"

class DbFile {
   public:
    virtual std::shared_ptr<Page> read_page(std::shared_ptr<PageId> pid) = 0;
    // virtual void write_page(std::unique_ptr<Page> page) = 0;
    virtual int get_id() const = 0;
    virtual const std::shared_ptr<TupleDesc>& get_tuple_desc() const = 0;
    virtual std::vector<std::shared_ptr<Page>> insert_tuple(
        const TransactionId& tid, std::shared_ptr<Tuple> t) = 0;
    virtual std::vector<std::shared_ptr<Page>> delete_tuple(
        const TransactionId& tid, std::shared_ptr<Tuple> t) = 0;
    virtual std::unique_ptr<DbFileIterator> iterator() = 0;
    virtual ~DbFile() = default;

   private:
};