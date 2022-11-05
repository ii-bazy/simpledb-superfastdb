#include "src/common/Type.hpp"

#include "src/storage/IntField.hpp"
#include "src/storage/StringField.hpp"

const Type* Type::INT_TYPE() {
    static std::unique_ptr<Type> int_type(new Type(TypeEnum::INT_TYPE));
    return int_type.get();
}

const Type* Type::STRING_TYPE() {
    static std::unique_ptr<Type> string_type(new Type(TypeEnum::STRING_TYPE));
    return string_type.get();
}

int Type::get_len() const {
    switch (type_) {
        case TypeEnum::INT_TYPE: {
            return 4;
        }
        case TypeEnum::STRING_TYPE: {
            return STRING_LEN + 4;
        }
        default: {
            throw std::exception();
        }
    }
}

std::unique_ptr<Field> Type::parse(std::istream& is) const {
    switch (type_) {
        case TypeEnum::INT_TYPE: {
            int number;
            is.read(reinterpret_cast<char*>(&number), sizeof(number));
            return std::make_unique<IntField>(number);
        }
        case TypeEnum::STRING_TYPE: {
            int string_length = 0;
            is.read(reinterpret_cast<char*>(&string_length),
                    sizeof(string_length));

            std::string str(string_length, 0);
            is.read(reinterpret_cast<char*>(str.data()), string_length);

            is.ignore(STRING_LEN - string_length);

            return std::make_unique<StringField>(str, STRING_LEN);
        }
        default: {
            throw std::exception();
        }
    }
}

std::string Type::to_string() const {
    static std::string int_type_str = "INT_TYPE";
    static std::string string_type_str = "STRING_TYPE";

    switch (type_) {
        case TypeEnum::INT_TYPE: {
            return int_type_str;
        }
        case TypeEnum::STRING_TYPE: {
            return string_type_str;
        }
        default: {
            throw std::exception();
        }
    }
}

Type::Type(TypeEnum type) : type_{type} {}