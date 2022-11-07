#include "src/storage/HeapFile.hpp"

#include <glog/logging.h>

#include "src/storage/HeapPageId.hpp"

HeapFile::HeapFile(std::ifstream file, std::shared_ptr<TupleDesc> td,
                   const std::string file_name)
    : td_{std::move(td)},
      file_{std::move(file)},
      id_{std::hash<std::string>()(file_name)} {
    const auto current_offset = file_.tellg();
    file_.seekg(0, std::ios_base::end);
    num_pages_ = file_.tellg() / BufferPool::get_page_size();
    LOG(INFO) << "NUM PAGES\t" << num_pages_;
    file_.seekg(current_offset);
}
std::shared_ptr<Page> HeapFile::read_page(std::shared_ptr<PageId> pid) {
    if (pid->get_page_number() > num_pages()) {
        throw std::invalid_argument("No such page.");
    }

    const int page_size = BufferPool::get_page_size();
    const int page_offset = pid->get_page_number() * page_size;

    LOG(INFO) << "PAGE NUM " << pid->get_page_number() << " PAGE ID "
              << pid->get_table_id() << "\n";
    LOG(INFO) << "PAGE SIZE " << page_size << " OFFSET " << page_offset << "\n";

    file_.seekg(page_offset);

    std::vector<char> bytes(page_size);

    file_.read(bytes.data(), page_size);

    auto ptr = std::make_shared<HeapPage>(pid, bytes);

    return ptr;
}

bool HeapFile::has_next(TransactionId tid) {
    while (true) {
        if (current_page_->has_next()) {
            return true;
        }

        it_page_index_ += 1;
        LOG(INFO) << "Jumping to next page: " << it_page_index_;

        if (it_page_index_ >= num_pages()) {
            LOG(INFO) << "Next page doesn't exist!";
            return false;
        }

        std::shared_ptr<PageId> hpid =
            std::make_shared<HeapPageId>(get_id(), it_page_index_);
        current_page_ = Database::get_buffer_pool().get_page(
            &tid, hpid, Permissions::READ_ONLY);
        current_page_->rewind();
    }

    return false;
}

std::shared_ptr<Tuple> HeapFile::next() { return current_page_->next(); }

void HeapFile::rewind(TransactionId tid) {
    it_page_index_ = 0;

    std::shared_ptr<PageId> hpid =
        std::make_shared<HeapPageId>(get_id(), it_page_index_);

    current_page_ = Database::get_buffer_pool().get_page(
        &tid, hpid, Permissions::READ_ONLY);
}
