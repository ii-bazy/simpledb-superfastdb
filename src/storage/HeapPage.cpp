#include "src/storage/HeapPage.hpp"

HeapPage::HeapPage(std::shared_ptr<PageId> id, const std::vector<char>& data)
    : pid_{std::move(id)},
      td_{Database::get_catalog().get_tuple_desc(pid_->get_table_id())} {
    num_slots_ = get_num_tuples();

    const int header_size = get_header_size();
    header_ = std::vector(data.begin(), data.begin() + header_size);

    std::istringstream it{std::string(data.begin(), data.end())};
    it.ignore(header_size);

    tuples_.resize(num_slots_);
    for (int slot_id = 0; slot_id < num_slots_; ++slot_id) {
        if (!is_slot_used(slot_id)) {
            it.ignore(td_->get_size());
        } else {
            tuples_[slot_id] = read_next_tuple(it, slot_id);
        }
    }
}

std::vector<char> HeapPage::create_empty_page_data() {
    int len = BufferPool::get_page_size();
    return std::vector<char>(len, 0);
}

int HeapPage::get_num_tuples() {
    return (BufferPool::get_page_size() * 8) / (td_->get_size() * 8 + 1);
}