#pragma once

#include <fstream>
#include <memory>

#include "src/storage/BufferPool.hpp"
#include "src/storage/DbFile.hpp"
#include "src/storage/HeapFileIterator.hpp"
#include "src/storage/HeapPage.hpp"
#include "src/storage/TupleDesc.hpp"

class HeapFile : public DbFile {
   public:
    HeapFile(std::ifstream file, std::shared_ptr<TupleDesc> td,
             const std::string file_name);

    // std::ifstream get_file() {
    //     throw std::invalid_argument("Nie wiem po co to narazie");
    //     return std::ifstream(".", std::ifstream::in);
    // }

    int get_id() const override { return id_; }

    const std::shared_ptr<TupleDesc>& get_tuple_desc() const override {
        return td_;
    }

    int num_pages() const { return num_pages_; }

    absl::StatusOr<std::shared_ptr<Page>> read_page(
        std::shared_ptr<PageId> pid) override;

    std::unique_ptr<DbFileIterator> iterator() override;

   private:
    friend class HeapFileIterator;

    std::shared_ptr<TupleDesc> td_;
    std::ifstream file_;
    int num_pages_;
    size_t id_;
};