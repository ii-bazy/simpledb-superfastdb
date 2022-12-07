#include "src/storage/Page.hpp"

#include "src/storage/BufferPool.hpp"

std::vector<char> Page::create_empty_page_data() {
    int len = BufferPool::get_page_size();
    return std::vector<char>(len, 0);
}