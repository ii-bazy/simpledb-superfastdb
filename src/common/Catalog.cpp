#include "src/common/Catalog.hpp"

#include <absl/strings/str_cat.h>
#include <glog/logging.h>

Catalog::Catalog() {}

void Catalog::add_table(std::shared_ptr<DbFile> file, std::string name,
                        std::string pkey_field) {
    const auto file_id = file->get_id();
    LOG(INFO) << absl::StrCat("Adding new table(id, name, pkey): ", "(",
                              file_id, ",", name, ",", pkey_field, ")");

    db_files_[file_id] = std::move(file);
    name_to_id_[name] = file_id;
    id_to_name_[file_id] = name;
    primary_keys_[file_id] = std::move(pkey_field);
}

void Catalog::add_table(std::shared_ptr<DbFile> file, std::string name) {
    add_table(std::move(file), std::move(name), "");
}

int Catalog::get_table_id(const std::string& name) const {
    const int table_id = name_to_id_.at(name);
    return table_id;
}

std::string Catalog::get_table_name(const int id) const {
    return id_to_name_.at(id);
}

const std::shared_ptr<TupleDesc>& Catalog::get_tuple_desc(int table_id) const {
    LOG(INFO) << "Get tuple desc: " << db_files_.size();
    auto it = db_files_.find(table_id);
    if (it == db_files_.end()) {
        throw std::invalid_argument("Unknown table id");
    }
    return it->second->get_tuple_desc();
}

std::shared_ptr<DbFile> Catalog::get_db_file(int table_id) const {
    LOG(INFO) << "Db files size\t" << db_files_.size() << "\n";
    auto it = db_files_.find(table_id);
    if (it == db_files_.end()) {
        throw std::invalid_argument("No table with id: ");
        return nullptr;
    }
    return it->second;
}

void Catalog::load_schema(std::string catalog_file) {
    std::ifstream file(catalog_file);
    if (!file.is_open()) {
        throw std::invalid_argument("Catalog file not found.");
    }
    const auto directory =
        std::filesystem::path{catalog_file}.parent_path().string();

    std::string line;
    while (std::getline(file, line)) {
        const auto tokens = split_line(line);
        const auto table_name = tokens.at(0);
        std::vector<const Type*> types;
        std::vector<std::string> names;
        std::cerr << "Loading schema :";

        for (int i = 1; i + 1 < static_cast<int>(tokens.size()); i += 2) {
            names.push_back(tokens[i]);
            const auto type = tokens[i + 1];
            std::cerr << tokens[i] << ":" << type << ", ";
            if (type == "int") {
                types.push_back(Type::INT_TYPE());
            } else if (type == "string") {
                types.push_back(Type::STRING_TYPE());
            } else {
                throw std::invalid_argument("Unknown type: " + type);
            }
        }
        std::cerr << "\n";

        const std::string file_name = directory + "/" + table_name + ".dat";
        std::ifstream file(file_name, std::ios::binary | std::ios::in);

        if (not file.is_open()) {
            throw std::invalid_argument("Could not open file: " + file_name);
        }

        auto td = std::make_shared<TupleDesc>(types, names);
        std::shared_ptr<DbFile> tab = std::make_shared<HeapFile>(
            std::move(file), std::move(td), std::move(file_name));
        add_table(std::move(tab), table_name);
    }
}

std::vector<std::string> Catalog::split_line(std::string line, char delimeter) {
    std::stringstream ss(line);
    std::vector<std::string> res;
    while (ss.good()) {
        std::string sub;
        std::getline(ss, sub, delimeter);
        res.push_back(std::move(sub));
    }
    return res;
}
