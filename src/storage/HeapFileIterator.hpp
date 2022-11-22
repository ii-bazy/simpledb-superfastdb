#pragma once

#include <glog/logging.h>

#include "src/storage/DbFileIterator.hpp"
#include "src/storage/HeapFile.hpp"
#include "src/storage/HeapPageIterator.hpp"

class HeapFile;

class HeapFileIterator : public DbFileIterator {
   public:
    HeapFileIterator(HeapFile* file);
    absl::StatusOr<bool> has_next(TransactionId tid) override;
    absl::StatusOr<std::shared_ptr<Tuple>> next() override;
    absl::Status rewind(TransactionId tid) override;

   private:
    HeapFile* file_;
    int it_page_index_;
    std::unique_ptr<PageIterator> current_page_it_;
};