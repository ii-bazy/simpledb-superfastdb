#pragma once

#include <filesystem>
#include <fstream>
#include <unordered_map>

#include "src/storage/DbFile.hpp"
#include "src/storage/HeapFile.hpp"
#include "src/storage/TupleDesc.hpp"
#include "src/utils/status_builder.hpp"

class Catalog {
   public:
    Catalog();

    void add_table(std::shared_ptr<DbFile> file, std::string name,
                   std::string pkey_field);

    void add_table(std::shared_ptr<DbFile> file, std::string name);

    absl::StatusOr<int> get_table_id(const std::string& name) const;

    absl::StatusOr<std::string> get_table_name(int id) const;

    absl::StatusOr<std::shared_ptr<TupleDesc>> get_tuple_desc(int id) const;

    absl::StatusOr<std::shared_ptr<DbFile>> get_db_file(int id) const;

    absl::Status load_schema(std::string catalog_file);

   private:
    std::unordered_map<int, std::shared_ptr<DbFile>> db_files_;
    std::unordered_map<int, std::string> table_names_;
    std::unordered_map<int, std::string> primary_keys_;
    std::unordered_map<std::string, int> name_to_id_;
    std::unordered_map<int, std::string> id_to_name_;
};