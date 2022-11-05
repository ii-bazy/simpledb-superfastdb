#pragma once

#include <vector>
#include <sstream>

#include "src/common/Database.hpp"
#include "src/storage/Page.hpp"
#include "src/storage/HeapPageId.hpp"
#include "src/storage/Tuple.hpp"
#include "src/storage/RecordId.hpp"

class HeapPage : Page {
public:
    HeapPage(std::shared_ptr<HeapPageId> id, const std::vector<char>& data) 
    : pid_{std::move(id)}, 
      td_{Database::get_catalog().get_tuple_desc(id->get_table_id())}
    {
        num_slots_ = get_num_tuples();

        const int header_size = get_header_size();
        header_ = std::vector(data.begin(), data.begin() + header_size);


        std::istringstream it{std::string(data.begin(), data.end())};
        it.ignore(header_size);


        tuples_.resize(num_slots_);
        for (int slot_id = 0; slot_id < num_slots_; ++slot_id) {
            if (!is_slot_used(slot_id)) {
                it.ignore(td_->get_size());
            }
            else {
                tuples_[slot_id] = read_next_tuple(it, slot_id);
            }
        }        
    }

    std::shared_ptr<HeapPageId> get_id() const {
        return pid_;
    }

    static std::vector<char> create_empty_page_data() const {
        int len = BufferPool::get_page_size();
        return std::vector<char>(len, 0);
    }

    int getNumUnusedSlots() const {
        int unused = 0;

        for (const byte : header_) {
            for (int i = 0; i < 8; ++i) {
                unused += (byte & (1 << i)) ? 1 : 0;
            }
        }

        return unused;
    }

    // TODO:
    // public byte[] getPageData() {}


private:
    std::shared_ptr<HeapPageId> pid_;
    const std::shared_ptr<TupleDesc> td_;

    int num_slots_;
    std::vector<char> header_;
    std::vector<Tuple> tuples_;

    std::vector<char> old_data;
    // const char old_data_lock = 0;

    int get_num_tuples() {
        return (BufferPool::get_page_size() * 8) / (td_->get_size() * 8 + 1);
    }

    int get_header_size() {
        return (get_num_tuples() + 7) / 8;
    }

    Tuple read_next_tuple(std::istringstream& it, int slot_id) {
        Tuple t(td_);

        
        std::shared_ptr<RecordId> rid(pid_.get(), slot_id);
        t.set_record_id(rid);

        for (int i = 0; i < td_->num_fields(); ++i) {
            t.set_field(i, td_->get_field_type(i)->parse(it));
        }

        return t;
    }

    bool is_slot_used(int slot_id) {
        const int byte_index = slot_id / 8;
        const int bit_position = slot_id % 8;
        return header_[byte_index] & (1 << bit_position);
    }

};