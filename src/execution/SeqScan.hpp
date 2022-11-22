#pragma once

#include <string>

#include "src/common/Database.hpp"
#include "src/execution/OpIterator.hpp"
#include "src/transaction/TransactionId.hpp"
#include "src/utils/status_macros.hpp"

class SeqScan : public OpIterator {
   public:
    static absl::StatusOr<std::unique_ptr<OpIterator>> Create(
        TransactionId tid, const int table_id, const std::string& table_alias) {
        ASSIGN_OR_RETURN(auto file,
                         Database::get_catalog().get_db_file(table_id));

        return std::unique_ptr<SeqScan>(new SeqScan(
            std::move(tid), table_id, table_alias, std::move(file)));
    }

    SeqScan(TransactionId tid, const int table_id,
            const std::string& table_alias, std::shared_ptr<DbFile> file)
        : tid_(std::move(tid)),
          table_id_(table_id),
          table_alias_(table_alias),
          file_(std::move(file)),
          file_it_(file_->iterator()) {}

    SeqScan(TransactionId tid, const int table_id, std::shared_ptr<DbFile> file)
        : SeqScan(std::move(tid), table_id, "", std::move(file)) {}

    absl::StatusOr<std::string> get_table_name() const {
        return Database::get_catalog().get_table_name(table_id_);
    }

    std::string get_alias() const { return table_alias_; }

    absl::StatusOr<std::shared_ptr<Tuple>> next() override {
        return file_it_->next();
    }

    absl::Status rewind() override { return file_it_->rewind(tid_); }

    absl::StatusOr<bool> has_next() override {
        return file_it_->has_next(tid_);
    };

    std::shared_ptr<TupleDesc> get_tuple_desc() const override {
        return file_->get_tuple_desc();
    }

   private:
    TransactionId tid_;
    const int table_id_;
    const std::string table_alias_;
    std::shared_ptr<DbFile> file_;
    std::unique_ptr<DbFileIterator> file_it_;
};