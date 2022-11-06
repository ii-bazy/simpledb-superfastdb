#pragma once

#include <sstream>
#include <vector>

#include "src/common/Database.hpp"
#include "src/storage/HeapPageId.hpp"
#include "src/storage/Page.hpp"
#include "src/storage/RecordId.hpp"
#include "src/storage/Tuple.hpp"

class HeapPage : public Page {
   public:
    HeapPage(std::shared_ptr<PageId> id, const std::vector<char>& data);

    std::shared_ptr<PageId> get_id() const { return pid_; }

    static std::vector<char> create_empty_page_data();

    int getNumUnusedSlots() const {
        int unused = 0;

        for (const auto byte : header_) {
            for (int i = 0; i < 8; ++i) {
                unused += (byte & (1 << i)) ? 1 : 0;
            }
        }

        return unused;
    }

    virtual bool has_next() {
        for (int i = it_index_; i < get_num_tuples(); ++i) {
            if (is_slot_used(i)) {
                return true;
            }
        }

        return false;
    }

    virtual std::shared_ptr<Tuple> next() override {
        for (; it_index_ < get_num_tuples(); ++it_index_) {
            if (is_slot_used(it_index_)) {
                it_index_ += 1;
                return tuples_[it_index_ - 1];
            }
        }

        throw std::runtime_error("What?");
    }

    virtual void rewind() override { it_index_ = 0; }

    // TODO:
    // /**
    //  * @return an iterator over all tuples on this page (calling remove on
    //  this iterator throws an UnsupportedOperationException)
    //  *         (note that this iterator shouldn't return tuples in empty
    //  slots!)
    //  */
    // public Iterator<Tuple> iterator() {
    //     // TODO: some code goes here
    //     return null;
    // }

    // TODO:
    // public byte[] getPageData() {}

   private:
    int it_index_ = 0;

    std::shared_ptr<PageId> pid_;
    const std::shared_ptr<TupleDesc> td_;

    int num_slots_;
    std::vector<char> header_;
    std::vector<std::shared_ptr<Tuple>> tuples_;

    std::vector<char> old_data;
    // const char old_data_lock = 0;

    int get_num_tuples();

    int get_header_size() { return (get_num_tuples() + 7) / 8; }

    std::shared_ptr<Tuple> read_next_tuple(std::istringstream& it,
                                           int slot_id) {
        std::shared_ptr<Tuple> t = std::make_shared<Tuple>(td_);

        std::shared_ptr<RecordId> rid =
            std::make_shared<RecordId>(pid_, slot_id);
        t->set_record_id(rid);

        for (int i = 0; i < td_->num_fields(); ++i) {
            t->set_field(i, td_->get_field_type(i)->parse(it));
        }

        return t;
    }

    bool is_slot_used(int slot_id) {
        const int byte_index = slot_id / 8;
        const int bit_position = slot_id % 8;
        return header_[byte_index] & (1 << bit_position);
    }
};