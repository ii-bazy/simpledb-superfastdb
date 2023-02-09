#include "src/storage/BufferPool.hpp"

#include <glog/logging.h>

BufferPool::BufferPool(int num_pages) : num_pages_(num_pages) {
    // TODO: pewnie jakis timestep na LRU
    page_count_ = 0;
}

std::shared_ptr<Page> BufferPool::get_page(const TransactionId* tid,
                                           std::shared_ptr<PageId> pid,
                                           Permissions perm) {
    // LOG(INFO) << "Get page(table_id, page_number): (" << pid->get_table_id()
    //           << "," << pid->get_page_number() << ")";
    auto it = pages_.find({pid->get_table_id(), pid->get_page_number()});

    if (it != pages_.end()) {
        recently_used_it_ = it;
        // LOG(INFO) << "Page found in buffer pool.";
        // LOG(WARNING) << "HIT";
        return it->second;
    }

    // LOG(WARNING) << "MISS";
    if (static_cast<int>(pages_.size()) >= num_pages_) {
        if (recently_used_it_ == pages_.end()) {
            throw std::logic_error("Page limit equal to 0?");
        }
        // LOG(WARNING) << "Removing " << recently_used_it_->first.first << " "
        //              << recently_used_it_->first.second;
        recently_used_it_->second = nullptr;
        pages_.erase(recently_used_it_);
    }

    // LOG(INFO) << "Get db_file from catalog.";
    auto db_file = Database::get_catalog().get_db_file(pid->get_table_id());

    if (db_file == nullptr) {
        throw std::runtime_error("Db_file is null.");
    }

    auto new_page = db_file->read_page(pid);
    recently_used_it_ =
        pages_.insert({{pid->get_table_id(), pid->get_page_number()}, new_page})
            .first;

    return new_page;
}

void BufferPool::insert_tuple(TransactionId tid, int table_id,
                              std::shared_ptr<Tuple> t) {
    auto db_file = Database::get_catalog().get_db_file(table_id);
    db_file->insert_tuple(tid, t);
}

void BufferPool::delete_tuple(TransactionId tid, std::shared_ptr<Tuple> t) {
    auto db_file = Database::get_catalog().get_db_file(
        t->get_record_id()->get_page_id()->get_table_id());
    db_file->delete_tuple(tid, t);
}