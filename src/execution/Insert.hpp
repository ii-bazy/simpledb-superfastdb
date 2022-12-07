#pragma once

#include "src/common/Database.hpp"
#include "src/execution/OpIterator.hpp"
#include "src/storage/IntField.hpp"
#include "src/transaction/TransactionId.hpp"

class Insert : public OpIterator {
   public:
    Insert(const TransactionId& t, std::unique_ptr<OpIterator> child,
           const int table_id)
        : child_{std::move(child)}, t_{t}, table_id_{table_id} {
        td_ = std::make_shared<TupleDesc>(
            std::vector<const Type*>{Type::INT_TYPE()},
            std::vector<std::string>{column_name_});
        int inserted = 0;
        for (auto t : *child_) {
            ++inserted;
            Database::get_buffer_pool().insert_tuple(t_, table_id_,
                                                     std::move(t));
        }
        inserted_ = std::make_shared<Tuple>(std::vector<const Type*>{Type::INT_TYPE()});
        inserted_->set_field(0, std::make_shared<IntField>(inserted));
    }

    std::shared_ptr<Tuple> next() override {
		emitted_result_ = true;
        return inserted_;
    }

    void rewind() override { emitted_result_ = false; }

    bool has_next() override { 
		return !emitted_result_;
	}

    std::shared_ptr<TupleDesc> get_tuple_desc() const { return td_; }

   private:
    const std::string column_name_ = "INSERTED";
    std::unique_ptr<OpIterator> child_;
    const TransactionId t_;
    const int table_id_;
    std::shared_ptr<TupleDesc> td_;
    bool emitted_result_ = false;
	std::shared_ptr<Tuple> inserted_;
};