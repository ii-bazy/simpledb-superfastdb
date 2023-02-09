#pragma once

#include "src/storage/PageId.hpp"

class HeapPageId : public PageId {
   public:
    HeapPageId(int table_id, int page_no)
        : table_id_{table_id}, page_no_{page_no} {}

    virtual int get_table_id() const override { return table_id_; }

    virtual int get_page_number() const override { return page_no_; }

    virtual int hash_code() override {
        // TODO:
        return 0;
    }

    virtual bool equals(const std::shared_ptr<PageId>& other) const override {
        if (dynamic_cast<HeapPageId*>(other.get()) == nullptr) {
            return false;
        }

        return other->get_page_number() == get_page_number() and
               other->get_table_id() == get_table_id();
    }

    virtual std::vector<int> serialize() override {
        return std::vector<int>{get_table_id(), get_page_number()};
    }

   private:
    int table_id_, page_no_;
};