#include "src/storage/HeapFile.hpp"

#include <glog/logging.h>

#include "src/storage/HeapPageId.hpp"

HeapFile::HeapFile(std::fstream file, std::shared_ptr<TupleDesc> td,
                   const std::string file_name)
    : td_{std::move(td)},
      file_{std::move(file)},
      id_{std::hash<std::string>()(file_name)} {
    const auto current_offset = file_.tellg();
    file_.seekg(0, std::ios_base::end);
    num_pages_ = file_.tellg() / BufferPool::get_page_size();
    // LOG(INFO) << "NUM PAGES\t" << num_pages_;
    file_.seekg(current_offset);
}
std::shared_ptr<Page> HeapFile::read_page(std::shared_ptr<PageId> pid) {
    if (pid->get_page_number() > num_pages()) {
        throw std::invalid_argument("No such page.");
    }

    const int page_size = BufferPool::get_page_size();
    const int page_offset = pid->get_page_number() * page_size;

    // LOG(INFO) << "PAGE NUM " << pid->get_page_number() << " PAGE ID "
            //   << pid->get_table_id() << "\n";
    // LOG(INFO) << "PAGE SIZE " << page_size << " OFFSET " << page_offset << "\n";

    file_.seekg(page_offset);

    std::vector<char> bytes(page_size);

    file_.read(bytes.data(), page_size);

    auto ptr = std::make_shared<HeapPage>(pid, bytes);

    return ptr;
}

std::unique_ptr<DbFileIterator> HeapFile::iterator() {
    return std::make_unique<HeapFileIterator>(this);
}

std::vector<std::shared_ptr<Page>> HeapFile::insert_tuple(
    const TransactionId& tid,
    std::shared_ptr<Tuple> t) {

    auto& buffer_pool = Database::get_buffer_pool();
    for (int page_index = 0; page_index < num_pages(); ++page_index) {
        const std::shared_ptr<PageId> page_id = std::make_shared<HeapPageId>(get_id(), page_index);
        auto page = buffer_pool.get_page(&tid, page_id, Permissions::READ_WRITE);
        if (page->get_num_unused_slots()) {
            page->insert_tuple(t);

            return std::vector<std::shared_ptr<Page>> {page};
        }
    }

    // Page with empty slot not found
    const auto empty_page_data = HeapPage::create_empty_page_data();
    file_.seekg(0, std::ios::end);
    file_.write(empty_page_data.data(), empty_page_data.size());
    num_pages_++;

    auto new_page = buffer_pool.get_page(
        &tid,
        std::make_shared<HeapPageId>(get_id(), num_pages() - 1),
        Permissions::READ_WRITE
    );

    new_page->insert_tuple(t);

    return std::vector<std::shared_ptr<Page>> {new_page};
}


std::vector<std::shared_ptr<Page>> HeapFile::delete_tuple(
    const TransactionId& tid,
    std::shared_ptr<Tuple> t) {

    auto& buffer_pool = Database::get_buffer_pool();
    const std::shared_ptr<PageId> page_id = std::make_shared<HeapPageId>(
        get_id(),
        t->get_record_id()->get_page_id()->get_page_number()
    );

    auto page = buffer_pool.get_page(&tid, page_id, Permissions::READ_WRITE);
    page->delete_tuple(t);

    return std::vector<std::shared_ptr<Page>> {page};
}