#pragma once

#include <string>

#include "src/execution/OpIterator.hpp"
#include "src/transaction/TransactionId.hpp"
#include "src/common/Database.hpp"

class SeqScan : OpIterator {
public:
    SeqScan(TransactionId tid, const int table_id, const std::string& table_alias)
        : table_id_{table_id}, table_alias_{table_alias} {}

    SeqScan(TransactionId tid, const int table_id)
        : SeqScan(tid, table_id, Database::get_catalog().get_table_name(table_id_)) {}

    std::string get_table_name() const {
        return Database::get_catalog().get_table_name(table_id_);
    }

    std::string get_alias() const {
        return table_alias_;
    }

    std::shared_ptr<TupleDesc> get_tuple_desc() const {
        return Database::get_catalog().get_tuple_desc(table_id_);
    }




    virtual void open() = 0;
    virtual void close() = 0;
    virtual Tuple next() = 0;
    virtual void rewind() = 0;
    virtual bool has_next() = 0;
    virtual std::shared_ptr<TupleDesc> get_tuple_desc() = 0;
private:
    const int table_id_;
    const std::string table_alias_;
};