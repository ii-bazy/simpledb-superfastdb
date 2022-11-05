#pragma once

#include <memory>

#include "src/storage/Page.hpp"
#include "src/storage/PageId.hpp"
#include "src/storage/TupleDesc.hpp"

class DbFile {
   public:
    // TODO: read page nie moze zwracac unique
    virtual std::shared_ptr<Page> read_page(const PageId* id) = 0;
    // virtual void write_page(std::unique_ptr<Page> page) = 0;
    virtual int get_id() = 0;
    virtual const std::shared_ptr<TupleDesc>& get_tuple_desc() = 0;

   private:
};