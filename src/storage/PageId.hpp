#pragma once

#include <vector>
#include <memory>

class PageId {
public:
    virtual int get_table_id() const = 0;
    virtual int get_page_number() const = 0;

    virtual int hash_code() = 0;
    virtual bool equals(const std::shared_ptr<PageId>& other) = 0;
    virtual std::vector<int> serialize() = 0;
};