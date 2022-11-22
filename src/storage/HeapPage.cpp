#include "src/storage/HeapPage.hpp"

absl::StatusOr<std::shared_ptr<Page>> HeapPage::Create(
    std::shared_ptr<PageId> pid, const std::vector<char>& data) {
    ASSIGN_OR_RETURN(
        std::shared_ptr<TupleDesc> td,
        Database::get_catalog().get_tuple_desc(pid->get_table_id()));
    // return std::make_shared<HeapPage>(std::move(pid), data, std::move(td));
    return std::unique_ptr<HeapPage>(
        new HeapPage(std::move(pid), data, std::move(td)));
}

std::vector<char> HeapPage::create_empty_page_data() {
    const int len = BufferPool::get_page_size();
    return std::vector<char>(len, 0);
}

std::unique_ptr<PageIterator> HeapPage::iterator() {
    return std::make_unique<HeapPageIterator>(this);
}

int HeapPage::get_num_tuples() const {
    return (BufferPool::get_page_size() * 8) / (td_->get_size() * 8 + 1);
}

HeapPage::HeapPage(std::shared_ptr<PageId> id, const std::vector<char>& data,
                   std::shared_ptr<TupleDesc> td)
    : pid_(std::move(id)), td_(std::move(td)) {
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