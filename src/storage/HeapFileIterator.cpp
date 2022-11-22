#include "src/storage/HeapFileIterator.hpp"

HeapFileIterator::HeapFileIterator(HeapFile* file)
    : file_(file), it_page_index_(0) {}

absl::StatusOr<bool> HeapFileIterator::has_next(TransactionId tid) {
    while (true) {
        ASSIGN_OR_RETURN(bool has_next, current_page_it_->has_next());
        if (has_next) {
            return true;
        }

        it_page_index_ += 1;
        LOG(INFO) << absl::StrCat("Jumping to next page: ", it_page_index_);

        if (it_page_index_ >= file_->num_pages()) {
            LOG(INFO) << "Next page doesn't exist!";
            return false;
        }

        std::shared_ptr<PageId> hpid =
            std::make_shared<HeapPageId>(file_->get_id(), it_page_index_);
        ASSIGN_OR_RETURN(auto page, Database::get_buffer_pool().get_page(
                                        &tid, hpid, Permissions::READ_ONLY));
        current_page_it_ = page->iterator();
        RETURN_IF_ERROR(current_page_it_->rewind());
    }

    return false;
}

absl::StatusOr<std::shared_ptr<Tuple>> HeapFileIterator::next() {
    return current_page_it_->next();
}

absl::Status HeapFileIterator::rewind(TransactionId tid) {
    it_page_index_ = 0;

    std::shared_ptr<PageId> hpid =
        std::make_shared<HeapPageId>(file_->get_id(), it_page_index_);
    ASSIGN_OR_RETURN(auto page, Database::get_buffer_pool().get_page(
                                    &tid, hpid, Permissions::READ_ONLY));
    current_page_it_ = page->iterator();
    RETURN_IF_ERROR(current_page_it_->rewind());
    return absl::OkStatus();
}