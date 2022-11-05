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

std::shared_ptr<TupleDesc> get_tuple_desc(int n) {
    return std::make_shared<TupleDesc>(get_types(n));
}

}  // namespace utility