#pragma once

#include <string_view>

#include "src/storage/Field.hpp"

class StringField : public Field {
   public:
    StringField(std::string s, int max_size);
    std::string_view get_value() const;
    void serialize(std::ostream& os);
    const Type* get_type() const;
    std::string to_string() const;
    bool operator==(const std::shared_ptr<Field>& other) const;

   private:
    std::string value_;
    const int max_size_;
};
