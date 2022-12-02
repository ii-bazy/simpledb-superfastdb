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

    std::shared_ptr<PageId> get_id() const override { return pid_; }

    static std::vector<char> create_empty_page_data();

    int get_num_unused_slots() const {
        int unused = 0;

        for (const auto byte : header_) {
            for (int i = 0; i < 8; ++i) {
                unused += (byte & (1 << i)) ? 1 : 0;
            }
        }

        return unused;
    }

    virtual std::unique_ptr<PageIterator> iterator() override;

    void insert_tuple(std::shared_ptr<Tuple> t) {
        // TODO: check desciptor mismatch? ?? 
        // TODO: Need to make diry?

        for (int i = 0; i < get_num_tuples; ++i) {
            if (not is_slot_used(i)) {
                tuples_[i] = t;
                t->set_record_id = std::make_shared<RecordId>(pid_, i);
                return ;
            }
        }

        throw std::invalid_argument("No free slot on this page");
    }

    void delete_tuple(std::shared_ptr<Tuple> t) {
        // TODO: Need to make diry?
        const int slot_index = t->get_record_id()->get_tuple_number();

        if (not t->get_record_id()->get_page_id()->equals(pid_)) {
            throw std::invalid_argument("Trying to delete tuple from wrong page.");
        }

        if (not is_slot_used(slot_index)) {
            throw std::invalid_argument("Trying to delete tuple from empty slot.");
        }

        mark_slot_unused(slot_index);
        tuples_[slot_index] = nullptr;
    }

    // TODO:
    // public byte[] getPageData() {}

   private:
    friend class HeapPageIterator;
    std::shared_ptr<PageId> pid_;
    const std::shared_ptr<TupleDesc> td_;

    int num_slots_;
    std::vector<char> header_;
    std::vector<std::shared_ptr<Tuple>> tuples_;

    std::vector<char> old_data;
    // const char old_data_lock = 0;

    int get_num_tuples() const;

    int get_header_size() const { return (get_num_tuples() + 7) / 8; }

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

    bool is_slot_used(int slot_id) const {
        const int byte_index = slot_id / 8;
        const int bit_position = slot_id % 8;
        return header_[byte_index] & (1 << bit_position);
    }

    void mark_slot_unused(const int slot_index) {
        const int byte_index = slot_id / 8;
        const int bit_position = slot_id % 8;
        return header_[byte_index] ^ header_[byte_index];
    }
};