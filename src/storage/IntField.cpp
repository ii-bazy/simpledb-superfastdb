#include "src/storage/IntField.hpp"

IntField::IntField(int value) : value_(value) {}

int IntField::get_value() const { return value_; }

void IntField::serialize(std::ostream& os) {
    os.write(reinterpret_cast<const char*>(&value_), sizeof(value_));
}

const Type* IntField::get_type() const { return Type::INT_TYPE(); }

std::string IntField::to_string() const { return std::to_string(value_); }

bool IntField::operator==(const std::shared_ptr<Field>& other) const {
    IntField* o = dynamic_cast<IntField*>(other.get());
    if (o == nullptr) {
        return false;
    }
    return value_ == o->value_;
}