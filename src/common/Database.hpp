#pragma once

#include <mutex>

#include "src/common/Catalog.hpp"
#include "src/storage/BufferPool.hpp"

class Database {
   public:
    static Catalog& get_catalog() { return catalog_; }

    static BufferPool& get_buffer_pool() { return buffer_pool_; }

   private:
    Database() {}

    static Catalog catalog_;
    static BufferPool buffer_pool_;
};