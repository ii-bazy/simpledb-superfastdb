#include "src/storage/HeapPageIterator.hpp"
#include <iostream>
#include <cassert>

HeapPageIterator::HeapPageIterator(std::shared_ptr<HeapPage> page)
    : page_(std::move(page)), it_index_(0) {}

bool HeapPageIterator::has_next() {
    for (int i = it_index_; i < page_->get_num_tuples(); ++i) {
        if (page_->is_slot_used(i)) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<Tuple> HeapPageIterator::next() {
    for (; it_index_ < page_->get_num_tuples(); ++it_index_) {
        if (page_->is_slot_used(it_index_)) {
            it_index_ += 1;
            return page_->tuples_[it_index_ - 1];
        }
    }

    throw std::runtime_error("What?");
}