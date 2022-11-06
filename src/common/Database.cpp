#include "src/common/Database.hpp"

Catalog& Database::get_catalog() {
    if (catalog_ == nullptr) {
        catalog_ = std::make_unique<Catalog>();
    }
    return *catalog_;
}

BufferPool& Database::get_buffer_pool() {
    if (buffer_pool_ == nullptr) {
        buffer_pool_ = std::make_unique<BufferPool>();
    }

    return *buffer_pool_;
}

Database::Database() {}