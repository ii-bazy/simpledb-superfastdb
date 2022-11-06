#pragma once

#include <memory>

#include "src/storage/Page.hpp"
#include "src/storage/PageId.hpp"
#include "src/storage/TupleDesc.hpp"

class DbFile {
   public:
    // TODO: read page nie moze zwracac unique
    virtual std::shared_ptr<Page> read_page(std::shared_ptr<PageId> pid) = 0;
    // virtual void write_page(std::unique_ptr<Page> page) = 0;
    virtual int get_id() const = 0;
    virtual const std::shared_ptr<TupleDesc>& get_tuple_desc() const = 0;

   virtual bool has_next(TransactionId tid) = 0;
   virtual std::shared_ptr<Tuple> next() = 0;
   virtual void rewind(TransactionId tid) = 0;

   private:
};