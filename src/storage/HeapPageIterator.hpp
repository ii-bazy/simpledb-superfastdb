#pragma once

#include <memory>

#include "src/storage/HeapPage.hpp"
#include "src/storage/PageIterator.hpp"

class HeapPage;
class HeapPageIterator : public PageIterator {
   public:
    HeapPageIterator(std::shared_ptr<HeapPage> page);

    bool has_next() override;

    std::shared_ptr<Tuple> next() override;

    void rewind() override { it_index_ = 0; }

   private:
    std::shared_ptr<HeapPage> page_;
    int it_index_;
};