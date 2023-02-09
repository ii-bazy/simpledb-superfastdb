#pragma once

#include "src/storage/Field.hpp"

class IntField : public Field {
   public:
    IntField(int value);
    int get_value() const;
    std::size_t hash() const override;

    void serialize(std::ostream& os) override;
    const Type* get_type() const override;
    std::string to_string() const override;
    bool operator==(const std::shared_ptr<Field>& other) const override;
    bool compare(const OpType op, const Field* other) const override;

   private:
    const int value_;
};
