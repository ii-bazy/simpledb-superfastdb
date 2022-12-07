#pragma once

#include <string_view>

#include "src/storage/Field.hpp"

class StringField : public Field {
   public:
    StringField(std::string s, int max_size);
    std::string_view get_value() const;

    virtual std::size_t hash() const override {
        return std::hash<std::string>{}(value_);
    }

    void serialize(std::ostream& os);
    const Type* get_type() const;
    std::string to_string() const;
    bool operator==(const std::shared_ptr<Field>& other) const;
    bool compare(const OpType op, const Field* other) const;
   private:
    std::string value_;
    const int max_size_;
};
