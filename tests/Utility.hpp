#pragma once

#include <memory>

#include "src/common/Type.hpp"
#include "src/storage/TupleDesc.hpp"

namespace utility {

std::vector<const Type*> get_types(int len) {
    std::vector<const Type*> types(len);
    for (auto& type : types) {
        type = Type::INT_TYPE();
    }
    return types;
}

std::vector<std::string> get_strings(int len, std::string val) {
    std::vector<std::string> strings(len);
    for (int i = 0; i < len; ++i) {
        strings[i] = val + std::to_string(i);
    }
    return strings;
}

std::shared_ptr<TupleDesc> get_tuple_desc(int n) {
    return std::make_shared<TupleDesc>(get_types(n));
}

std::shared_ptr<TupleDesc> get_tuple_desc(int n, std::string name) {
    return std::make_shared<TupleDesc>(get_types(n), get_strings(n, name));
}

}  // namespace utility