#pragma once

#include "src/storage/Field.hpp"

class IntField : public Field {
   public:
    IntField(int value);
    int get_value() const;

    void serialize(std::ostream& os);
    const Type* get_type() const;
    std::string to_string() const;
    bool operator==(const std::shared_ptr<Field>& other) const;
    bool compare(const OpType op, const Field* other) const;
   private:
    const int value_;
};
