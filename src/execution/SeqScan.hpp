#pragma once

#include <string>
#include "absl/strings/str_cat.h"#include <absl/strings/str_cat.h>
#include <glog/logging.h>

#include "src/common/Database.hpp"
#include "src/execution/OpIterator.hpp"
#include "src/transaction/TransactionId.hpp"

class SeqScan : public OpIterator {
   public:
    SeqScan(TransactionId tid, const int table_id,
            const std::string& table_alias)
        : tid_{tid},
          table_id_{table_id},
          table_alias_{table_alias},
          file_{Database::get_catalog().get_db_file(table_id_)},
          file_it_{file_->iterator()} {

        std::vector<const Type*> types;
        std::vector<std::string> field_names;
        const auto& td = Database::get_catalog().get_tuple_desc(table_id_);

        for (int i = 0; i < td->num_fields(); ++i) {
            types.push_back(td->get_field_type(i));
            field_names.push_back(
                absl::StrCat(table_alias_ != "" ? table_alias_ : get_table_name(), ".", td->get_field_name(i))
            );

            LOG(WARNING) << "FIELD NAME\t" << field_names.back() << "\n";
        }
        
        td_ = std::make_shared<TupleDesc>(
            types, field_names
        );
    }

    SeqScan(TransactionId tid, const int table_id)
        : SeqScan(tid, table_id,
                  Database::get_catalog().get_table_name(table_id)) {}

    std::string get_table_name() const {
        return Database::get_catalog().get_table_name(table_id_);
    }

    std::string get_alias() const { return table_alias_; }

    std::shared_ptr<TupleDesc> get_tuple_desc() const {
        return Database::get_catalog().get_tuple_desc(table_id_);
    }

    std::shared_ptr<Tuple> next() { return file_it_->next(); }

    void rewind() { file_it_->rewind(tid_); }

    bool has_next() { return file_it_->has_next(tid_); };

    std::shared_ptr<TupleDesc> get_tuple_desc() {
        return file_->get_tuple_desc();
    }

   private:
    TransactionId tid_;
    const int table_id_;
    const std::string table_alias_;
    std::shared_ptr<DbFile> file_;
    std::unique_ptr<DbFileIterator> file_it_;
    std::shared_ptr<TupleDesc> td_;
};