#pragma once

#include <filesystem>
#include <fstream>
#include <unordered_map>

#include "src/storage/DbFile.hpp"
#include "src/storage/HeapFile.hpp"
#include "src/storage/TupleDesc.hpp"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

class Catalog {
   public:
    Catalog();

    void add_table(std::shared_ptr<DbFile> file, std::string name,
                   std::string pkey_field);

    void add_table(std::shared_ptr<DbFile> file, std::string name);

    int get_table_id(const std::string& name) const;

    std::string get_table_name(const int id) const;

    const std::shared_ptr<TupleDesc>& get_tuple_desc(int table_id) const;

    std::shared_ptr<DbFile> get_db_file(int table_id) const;

    std::string get_primary_key(int table_id) const;

    void load_schema(std::string catalog_file);

   private:
    std::vector<std::string> split_line(std::string line, char delimeter = ',');

    absl::flat_hash_map<int, std::shared_ptr<DbFile>> db_files_;
    absl::flat_hash_map<int, std::string> table_names_;
    absl::flat_hash_map<int, std::string> primary_keys_;
    absl::flat_hash_map<std::string, int> name_to_id_;
    absl::flat_hash_map<int, std::string> id_to_name_;
};