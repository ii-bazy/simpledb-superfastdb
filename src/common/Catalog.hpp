#pragma once

#include <unordered_map>

#include "src/storage/DbFile.hpp"
#include "src/storage/TupleDesc.hpp"

class Catalog {
   public:
    Catalog() {
        // TODO(domiko):
    }

    void add_table(std::shared_ptr<DbFile> file, std::string name,
                   std::string pkey_field) {
        db_files_.emplace(file->get_id(), file);
        table_names_.emplace(file->get_id(), name);
        primary_keys_.emplace(file->get_id(), pkey_field);

        if (name.size() > 0u) {
            name_to_id_[name] = file->get_id();
            id_to_name_[file->get_id()] = name;
        }
    }

    void add_table(std::shared_ptr<DbFile> file, std::string name) {
        add_table(std::move(file), std::move(name), "");
    }

    int get_table_id(const std::string& name) const {
        const int table_id = name_to_id_.at(name);
        return table_id;
    }

    std::string get_table_name(const int id) const {
        return id_to_name_.at(id);
    }

    const std::shared_ptr<TupleDesc>& get_tuple_desc(int table_id) const {
        auto it = db_files_.find(table_id);
        if (it == db_files_.end()) {
            return nullptr;
        }
        return it->second->get_tuple_desc();
    }

    std::shared_ptr<DbFile> get_db_file(int table_id) const {
        auto it = db_files_.find(table_id);
        if (it == db_files_.end()) {
            return nullptr;
        }
        return it->second;
    }

    void load_schema(std::string catalog_file) {
        // TODO:
    }

   private:
    std::unordered_map<int, std::shared_ptr<DbFile>> db_files_;
    std::unordered_map<int, std::string> table_names_;
    std::unordered_map<int, std::string> primary_keys_;
    std::unordered_map<std::string, int> name_to_id_;
    std::unordered_map<int, std::string> id_to_name_;
};