#include "src/storage/BufferPool.hpp"

#include <glog/logging.h>

BufferPool::BufferPool(int num_pages) : num_pages_(num_pages), page_count_(0) {
    // TODO: pewnie jakis timestep na LRU
}

absl::StatusOr<std::shared_ptr<Page>> BufferPool::get_page(
    TransactionId* tid, std::shared_ptr<PageId> pid, Permissions perm) {
    LOG(INFO) << absl::StrCat("Get page(tid, pid): (", pid->get_table_id(), ",",
                              pid->get_page_number(), ")");
    auto it = pages_.find({pid->get_table_id(), pid->get_page_number()});
    if (it != pages_.end()) {
        LOG(INFO) << "Page found in buffer pool.";
        return it->second;
    }

    if ((int)pages_.size() >= num_pages_) {
        return absl::InternalError("For now we have limit for pages.");
    }

    LOG(INFO) << "Get db_file from catalog.";
    ASSIGN_OR_RETURN(auto db_file,
                     Database::get_catalog().get_db_file(pid->get_table_id()));
    
    ASSIGN_OR_RETURN(auto new_page, db_file->read_page(pid));
    pages_[{pid->get_table_id(), pid->get_page_number()}] = new_page;
    return new_page;
}