#include "src/common/Catalog.hpp"

#include <absl/strings/str_cat.h>
#include <glog/logging.h>

#include "src/utils/split_line.hpp"

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

absl::StatusOr<int> Catalog::get_table_id(const std::string& name) const {
    if (auto it = name_to_id_.find(name); it != name_to_id_.end()) {
        return it->second;
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid table name=", name, "."));
}

absl::StatusOr<std::string> Catalog::get_table_name(const int id) const {
    if (auto it = id_to_name_.find(id); it != id_to_name_.end()) {
        return it->second;
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid table id=", id, "."));
}

absl::StatusOr<std::shared_ptr<TupleDesc>> Catalog::get_tuple_desc(
    const int id) const {
    if (auto it = db_files_.find(id); it != db_files_.end()) {
        return it->second->get_tuple_desc();
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid table id=", id, "."));
}

absl::StatusOr<std::shared_ptr<DbFile>> Catalog::get_db_file(int id) const {
    if (auto it = db_files_.find(id); it != db_files_.end()) {
        return it->second;
    }
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid table id=", id, "."));
}

absl::Status Catalog::load_schema(std::string catalog_file) {
    std::ifstream file(catalog_file);
    if (!file.is_open()) {
        return absl::InvalidArgumentError("Catalog not found catalog=" +
                                          catalog_file);
    }

    const auto directory =
        std::filesystem::path{catalog_file}.parent_path().string();

    std::string line;
    while (std::getline(file, line)) {
        const auto tokens = split_line(line, ',');
        const auto table_name = tokens.at(0);
        std::vector<const Type*> types;
        std::vector<std::string> names;
        LOG(INFO) << "Loading schema :";

        for (int i = 1; i + 1 < static_cast<int>(tokens.size()); i += 2) {
            names.push_back(tokens[i]);
            const auto type = tokens[i + 1];
            LOG(INFO) << tokens[i] << ":" << type << ", ";
            if (type == "int") {
                types.push_back(Type::INT_TYPE());
            } else if (type == "string") {
                types.push_back(Type::STRING_TYPE());
            } else {
                return absl::InvalidArgumentError("Unknown type: " + type);
            }
        }
        LOG(INFO) << "\n";

        const std::string file_name = directory + "/" + table_name + ".dat";
        std::ifstream file(file_name, std::ios::binary | std::ios::in);

        if (not file.is_open()) {
            return absl::InvalidArgumentError("Could not open file: " +
                                              file_name);
        }

        auto td = std::make_shared<TupleDesc>(types, names);
        std::shared_ptr<HeapFile> tab = std::make_shared<HeapFile>(
            std::move(file), std::move(td), std::move(file_name));
        add_table(std::move(tab), table_name);
    }

    return absl::OkStatus();
}
