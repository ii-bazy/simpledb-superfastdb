#pragma once

#include "src/storage/HeapPage.hpp"
#include "src/storage/PageIterator.hpp"

class HeapPage;
class HeapPageIterator : public PageIterator {
   public:
    HeapPageIterator(HeapPage* page);

    bool has_next() override;

    std::shared_ptr<Tuple> next() override;

    void rewind() override { it_index_ = 0; }

   private:
    HeapPage* page_;
    int it_index_;
};