#pragma once

#include <exception>
#include <map>
#include <memory>

#include "src/common/Database.hpp"
#include "src/common/Permissions.hpp"
#include "src/storage/Page.hpp"
#include "src/storage/PageId.hpp"
#include "src/transaction/TransactionId.hpp"

class BufferPool {
   public:
    static int get_page_size() { return page_size_; }

    static void set_page_size(int page_size) { page_size_ = page_size; }

    static void reset_page_size() { page_size_ = c_default_page_size; }

    // TODO: Funkcje od lini 85 narazie nie potrzebne

    BufferPool(int num_pages = c_default_pages);

    std::shared_ptr<Page> get_page(const TransactionId* tid,
                                   std::shared_ptr<PageId> pid,
                                   Permissions perm);

    void insert_tuple(TransactionId tid, int table_id, std::shared_ptr<Tuple> t);

    void delete_tuple(TransactionId tid, std::shared_ptr<Tuple> t);

   private:
    constexpr static inline int c_default_pages = 50;
    constexpr static inline int c_default_page_size = 4096;
    static inline int page_size_ = c_default_page_size;

    std::map<std::pair<int, int>, std::shared_ptr<Page>> pages_;
    const int num_pages_;
    int page_count_;
};