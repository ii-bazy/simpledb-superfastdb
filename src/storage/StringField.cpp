#include "src/storage/StringField.hpp"

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

const Type* StringField::get_type() const { return Type::INT_TYPE(); }

std::string StringField::to_string() const { return value_; }

bool StringField::operator==(const std::shared_ptr<Field>& other) const {
    StringField* o = dynamic_cast<StringField*>(other.get());
    if (o == nullptr) {
        return false;
    }
    return value_ == o->value_;
}