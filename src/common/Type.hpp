#pragma once

#include <iterator>
#include <memory>
#include <string_view>

#include "src/storage/Field.hpp"

class Field;

class Type {
   public:
    static const Type* INT_TYPE();
    static const Type* STRING_TYPE();

    inline constexpr static int STRING_LEN = 128;

    int get_len() const;
    std::unique_ptr<Field> parse(std::istream& is) const;
    std::string_view to_string() const;

   private:
    enum class TypeEnum { INT_TYPE, STRING_TYPE } type_;

    Type(TypeEnum type);
};