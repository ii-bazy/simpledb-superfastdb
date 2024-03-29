#include "src/storage/StringField.hpp"
#include <glog/logging.h>

#include <utility>

StringField::StringField(std::string s, int max_size)
    : value_(std::move(s)), max_size_(max_size) {
    if (static_cast<int>(value_.size()) > max_size_) {
        value_.resize(max_size_);
    }
}

std::string_view StringField::get_value() const { return value_; }

void StringField::serialize(std::ostream& os) {
    os.write(reinterpret_cast<const char*>(&value_), sizeof(value_));
}

const Type* StringField::get_type() const { return Type::STRING_TYPE(); }

std::string StringField::to_string() const { return value_; }

bool StringField::operator==(const std::shared_ptr<Field>& other) const {
    StringField* o = dynamic_cast<StringField*>(other.get());
    if (o == nullptr) {
        return false;
    }
    return value_ == o->value_;
}

bool StringField::compare(const OpType op, const Field* other) const {
    auto* field = dynamic_cast<const StringField*>(other);

    if (field == nullptr) {
        return false;
    }
    
    switch (op) {
        case OpType::EQUALS:
            return value_ == field->value_;
        case OpType::GREATER_THAN:
            return value_ > field->value_;
        case OpType::LESS_THAN:
            return value_ < field->value_;
        case OpType::LESS_THAN_OR_EQ:
            return value_ <= field->value_;
        case OpType::GREATER_THAN_OR_EQ:
            return value_ >= field->value_;
        case OpType::NOT_EQUALS:
            return value_ != field->value_;
        case OpType::LIKE:
            return value_.find(field->value_) != std::string::npos;
    }

    return false;
}