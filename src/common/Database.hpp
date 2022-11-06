#pragma once

#include <mutex>

#include "src/common/Catalog.hpp"
#include "src/storage/BufferPool.hpp"

class Catalog;
class BufferPool;

class Database {
   public:
    static Catalog& get_catalog();

    static BufferPool& get_buffer_pool();

   private:
    Database();

    inline static std::unique_ptr<Catalog> catalog_;
    inline static std::unique_ptr<BufferPool> buffer_pool_;
};