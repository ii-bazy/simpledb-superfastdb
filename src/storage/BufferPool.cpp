#include "src/storage/BufferPool.hpp"

#include <glog/logging.h>

BufferPool::BufferPool(int num_pages) : num_pages_(num_pages) {
    // TODO: pewnie jakis timestep na LRU
    page_count_ = 0;
}

std::shared_ptr<Page> BufferPool::get_page(TransactionId* tid,
                                           std::shared_ptr<PageId> pid,
                                           Permissions perm) {
    std::cerr << "Get page\n";
    auto it = pages_.find({pid->get_table_id(), pid->get_page_number()});

    if (it != pages_.end()) {
        return it->second;
    }

    if (static_cast<int>(pages_.size()) >= num_pages_) {
        throw std::logic_error(":)");
    }

    std::cerr << "PYTAM O STRONE\n";
    LOG(INFO) << "Pytam o strone!\n";

    auto db_file = Database::get_catalog()
                        .get_db_file(pid->get_table_id());

    
    LOG(INFO) << "Mam db file! :" << (db_file == nullptr);

    auto new_page = db_file->read_page(pid);
    std::cerr << "DZIALA\n";
    pages_[{pid->get_table_id(), pid->get_page_number()}] = new_page;
    return new_page;
}