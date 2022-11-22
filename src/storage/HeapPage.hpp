#pragma once

#include <sstream>
#include <vector>

#include "src/common/Database.hpp"
#include "src/storage/HeapPageId.hpp"
#include "src/storage/Page.hpp"
#include "src/storage/RecordId.hpp"
#include "src/storage/Tuple.hpp"
#include "src/utils/status_macros.hpp"

class HeapPage : public Page {
   public:
    // https://abseil.io/tips/42
    static absl::StatusOr<std::shared_ptr<Page>> Create(
        std::shared_ptr<PageId> pid, const std::vector<char>& data);

    static std::vector<char> create_empty_page_data();

    std::shared_ptr<PageId> get_id() const override { return pid_; }

    int getNumUnusedSlots() const {
        int unused = 0;

        for (const auto byte : header_) {
            for (int i = 0; i < 8; ++i) {
                unused += (byte & (1 << i)) ? 1 : 0;
            }
        }

        return unused;
    }

    virtual std::unique_ptr<PageIterator> iterator() override;

    // TODO:
    // public byte[] getPageData() {}

    HeapPage(std::shared_ptr<PageId> id, const std::vector<char>& data,
             std::shared_ptr<TupleDesc> td);

   private:
    friend class HeapPageIterator;

    int get_num_tuples() const;

    int get_header_size() const { return (get_num_tuples() + 7) / 8; }

    std::shared_ptr<Tuple> read_next_tuple(std::istringstream& it,
                                           int slot_id) {
        std::shared_ptr<Tuple> t = std::make_shared<Tuple>(td_);

        std::shared_ptr<RecordId> rid =
            std::make_shared<RecordId>(pid_.get(), slot_id);
        t->set_record_id(std::move(rid));

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

    std::shared_ptr<PageId> pid_;
    std::shared_ptr<TupleDesc> td_;

    int num_slots_;
    std::vector<char> header_;
    std::vector<std::shared_ptr<Tuple>> tuples_;

    std::vector<char> old_data;
    // const char old_data_lock = 0;
};