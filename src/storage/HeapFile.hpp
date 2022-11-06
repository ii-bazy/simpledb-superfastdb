#pragma once

#include <fstream>
#include <memory>

#include "src/storage/BufferPool.hpp"
#include "src/storage/DbFile.hpp"
#include "src/storage/HeapPage.hpp"
#include "src/storage/TupleDesc.hpp"

class HeapFile : public DbFile {
   public:
    HeapFile(std::ifstream file, std::shared_ptr<TupleDesc> td,
             const std::string file_name);

    std::ifstream get_file() {
        throw std::invalid_argument("Nie wiem po co to narazie");
        return std::ifstream(".", std::ifstream::in);
    }

    virtual int get_id() const { return id_; }

    const std::shared_ptr<TupleDesc>& get_tuple_desc() const { return td_; }

    int num_pages() const { return num_pages_; }

    std::shared_ptr<Page> read_page(std::shared_ptr<PageId> pid);

    bool has_next(TransactionId tid);

    std::shared_ptr<Tuple> next();

    void rewind(TransactionId tid);

    // TODO: zamknij plik

   private:
    std::shared_ptr<TupleDesc> td_;
    std::ifstream file_;

    int num_pages_;
    size_t id_;

    int it_page_index_ = 0;
    std::shared_ptr<Page> current_page_;
};