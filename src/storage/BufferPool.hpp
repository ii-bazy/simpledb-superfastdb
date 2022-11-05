#pragma once

#include <exception>
#include <unordered_map>

#include "src/common/Permissions.hpp"
#include "src/storage/Page.hpp"
#include "src/storage/PageId.hpp"
#include "src/transaction/TransactionId.hpp"

class BufferPool {
   public:
    static int get_page_size() { return page_size_; }

    static int set_page_size(int page_size) { page_size_ = page_size; }

    static void reset_page_size() { page_size_ = c_default_page_size; }

    // TODO: Funkcje od lini 85 narazie nie potrzebne

    BufferPool(int num_pages = c_default_pages) : num_pages_(num_pages) {
        // TODO: pewnie jakis timestep na LRU
        page_count_ = 0;
    }

    std::shared_ptr<Page> get_page(TransactionId* tid, PageId* pid,
                                   Permissions perm) {
        auto it = pages_.find(pid);

        if (it != pages_.end()) {
            return it->second;
        }

        if (pages_.size() >= num_pages_) {
            throw std::logic_error(":)");
        }

        auto new_page = Database::get_catalog()
                            .get_db_file(pid->get_table_id())
                            ->read_page(pid);
        pages_[pid] = new_page;
        return new_page;
    }

   private:
    constexpr static inline int c_default_pages = 50;
    constexpr static inline int c_default_page_size = 4096;
    static inline int page_size_ = c_default_page_size;

    std::unordered_map<PageId*, std::shared_ptr<Page>> pages_;
    const int num_pages_;
    int page_count_;
};