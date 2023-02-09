#pragma once

#include <glog/logging.h>

#include "src/storage/DbFileIterator.hpp"
#include "src/storage/HeapFile.hpp"
#include "src/storage/HeapPageIterator.hpp"

class HeapFile;

class HeapFileIterator : public DbFileIterator {
   public:
    HeapFileIterator(HeapFile* file);
    bool has_next(TransactionId tid) override;

    std::shared_ptr<Tuple> next() override;
    void rewind(TransactionId tid) override;

   private:
    HeapFile* file_;
    int it_page_index_;
    std::unique_ptr<PageIterator> current_page_it_;
};