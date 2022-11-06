#pragma once

#include <filesystem>
#include <fstream>
#include <unordered_map>

#include "src/storage/DbFile.hpp"
#include "src/storage/HeapFile.hpp"
#include "src/storage/TupleDesc.hpp"

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

    void load_schema(std::string catalog_file);

   private:
    std::vector<std::string> split_line(std::string line, char delimeter = ',');

    std::unordered_map<int, std::shared_ptr<DbFile>> db_files_;
    std::unordered_map<int, std::string> table_names_;
    std::unordered_map<int, std::string> primary_keys_;
    std::unordered_map<std::string, int> name_to_id_;
    std::unordered_map<int, std::string> id_to_name_;
};