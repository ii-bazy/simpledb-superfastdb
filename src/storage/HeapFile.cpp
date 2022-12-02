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

std::unique_ptr<DbFileIterator> HeapFile::iterator() {
    return std::make_unique<HeapFileIterator>(this);
}