#pragma once

#include "src/storage/HeapPage.hpp"
#include "src/storage/PageIterator.hpp"

class HeapPage;
class HeapPageIterator : public PageIterator {
   public:
    HeapPageIterator(HeapPage* page);

    absl::StatusOr<bool> has_next() override;

    absl::StatusOr<std::shared_ptr<Tuple>> next() override;

    absl::Status rewind() override {
        it_index_ = 0;
        return absl::OkStatus();
    }

   private:
    HeapPage* page_;
    int it_index_;
};