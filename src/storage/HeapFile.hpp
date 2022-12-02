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
    HeapFile(std::fstream file, std::shared_ptr<TupleDesc> td,
             const std::string file_name);

    std::fstream get_file() {
        throw std::invalid_argument("Nie wiem po co to narazie");
        return std::fstream(".", std::fstream::in);
    }

    int get_id() const override { return id_; }

    const std::shared_ptr<TupleDesc>& get_tuple_desc() const override {
        return td_;
    }

    int num_pages() const { return num_pages_; }

    std::shared_ptr<Page> read_page(std::shared_ptr<PageId> pid) override;

    std::unique_ptr<DbFileIterator> iterator() override;

    std::vector<std::shared_ptr<Page>> insert_tuple(
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

    std::vector<std::shared_ptr<Page>> delete_tuple(
        const TransactionId& tid,
        std::shared_ptr<Tuple> t) {

        auto& buffer_pool = Database::get_buffer_pool();
        const std::shared_ptr<PageId> page_id = std::make_shared<HeapPageId>(
            get_id(),
            page_index
        );

        auto page = buffer_pool.get_page(&tid, page_id, Permissions::READ_WRITE);
        page->delete_tuple(t);

        return std::vector<std::shared_ptr<Page>> {page};
    }

    // TODO: zamknij plik

   private:
    friend class HeapFileIterator;
    std::shared_ptr<TupleDesc> td_;
    std::fstream file_;

    int num_pages_;
    size_t id_;
};