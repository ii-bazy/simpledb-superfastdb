#pragma once

#include <iostream>
#include <memory>
#include <string>

#include "src/common/Type.hpp"

class Type;

class Field {
   public:
    virtual void serialize(std::ostream& os) = 0;
    // TODO: predicate
    // bool compare()
    virtual const Type* get_type() const = 0;
    virtual std::string to_string() const = 0;
    virtual bool operator==(const std::shared_ptr<Field>& other) const = 0;
    virtual ~Field() = default;
};