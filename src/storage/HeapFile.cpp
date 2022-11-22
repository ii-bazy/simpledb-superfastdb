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
    file_.seekg(current_offset);
    LOG(INFO) << absl::StrCat("Loading HeapFile with ", num_pages_, " pages.");
}

absl::StatusOr<std::shared_ptr<Page>> HeapFile::read_page(
    std::shared_ptr<PageId> pid) {
    if (pid->get_page_number() > num_pages()) {
        return absl::InvalidArgumentError(
            absl::StrCat("Invalid page with number=", pid->get_page_number()));
    }

    const int page_size = BufferPool::get_page_size();
    const int page_offset = pid->get_page_number() * page_size;

    file_.seekg(page_offset);
    std::vector<char> bytes(page_size);
    file_.read(bytes.data(), page_size);

    return HeapPage::Create(std::move(pid), bytes);
}

std::unique_ptr<DbFileIterator> HeapFile::iterator() {
    return std::make_unique<HeapFileIterator>(this);
}