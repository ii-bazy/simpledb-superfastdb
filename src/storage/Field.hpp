#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <string>

#include "src/common/Type.hpp"
#include "src/execution/OpType.hpp"

class Type;

class Field {
   public:
    virtual void serialize(std::ostream& os) = 0;
    virtual std::size_t hash() const = 0;
    // TODO: predicate
    // bool compare()
    virtual const Type* get_type() const = 0;
    virtual std::string to_string() const = 0;
    virtual bool operator==(const std::shared_ptr<Field>& other) const = 0;
    virtual bool compare(const OpType op, const Field* other) const = 0;
    virtual ~Field() = default;
};

// ;)
template <>
struct std::hash<std::shared_ptr<Field>> {
    constexpr std::size_t operator()(
        const std::shared_ptr<Field>& field) const noexcept {
        return field->hash();
    }
};

template <>
struct std::equal_to<std::shared_ptr<Field>> {
    constexpr bool operator()(
        const std::shared_ptr<Field>& field,
        const std::shared_ptr<Field>& other) const noexcept {
        return field->operator==(other);
    }
};