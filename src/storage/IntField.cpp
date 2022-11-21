#include "src/storage/IntField.hpp"
#include <glog/logging.h>

IntField::IntField(int value) : value_(value) {}

int IntField::get_value() const { return value_; }

void IntField::serialize(std::ostream& os) {
    os.write(reinterpret_cast<const char*>(&value_), sizeof(value_));
}

const Type* IntField::get_type() const { return Type::INT_TYPE(); }

std::string IntField::to_string() const { return std::to_string(value_); }

bool IntField::compare(const OpType op, const Field* other) const {
    const auto* o = dynamic_cast<const IntField*>(other);
    if (o == nullptr) {
        return false;
    }
    
    switch (op) {
        case OpType::EQUALS:
        case OpType::LIKE:
            return value_ == o->value_;
        case OpType::GREATER_THAN:
            return value_ > o->value_; 
        case OpType::LESS_THAN:
            return value_ < o->value_; 
        case OpType::LESS_THAN_OR_EQ:
            return value_ <= o->value_; 
        case OpType::GREATER_THAN_OR_EQ:
            return value_ >= o->value_; 
        case OpType::NOT_EQUALS:
            return value_ != o->value_; 
    }
    return false;
}

bool IntField::operator==(const std::shared_ptr<Field>& other) const {
    IntField* o = dynamic_cast<IntField*>(other.get());
    if (o == nullptr) {
        return false;
    }
    return value_ == o->value_;
}