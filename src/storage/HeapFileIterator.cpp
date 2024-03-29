#include "src/storage/HeapFileIterator.hpp"

HeapFileIterator::HeapFileIterator(HeapFile* file)
    : file_(file), it_page_index_(0) {}

bool HeapFileIterator::has_next(TransactionId tid) {
    while (true) {
        if (current_page_it_->has_next()) {
            return true;
        }

        it_page_index_ += 1;
        // LOG(INFO) << "Jumping to next page: " << it_page_index_ << "\tAll pages: " << file_->num_pages();

        if (it_page_index_ >= file_->num_pages()) {
            // LOG(INFO) << "Next page doesn't exist!";
            return false;
        }

        std::shared_ptr<PageId> hpid =
            std::make_shared<HeapPageId>(file_->get_id(), it_page_index_);
        current_page_it_ = Database::get_buffer_pool()
                               .get_page(&tid, hpid, Permissions::READ_ONLY)
                               ->iterator();
        current_page_it_->rewind();
    }

    return false;
}

std::shared_ptr<Tuple> HeapFileIterator::next() {
    return current_page_it_->next();
}

void HeapFileIterator::rewind(TransactionId tid) {
    it_page_index_ = 0;

    std::shared_ptr<PageId> hpid =
        std::make_shared<HeapPageId>(file_->get_id(), it_page_index_);

    current_page_it_ = Database::get_buffer_pool()
                           .get_page(&tid, hpid, Permissions::READ_ONLY)
                           ->iterator();
    current_page_it_->rewind();
}