#pragma once

#include <exception>
#include <memory>
#include <numeric>
#include <vector>

#include "src/common/Type.hpp"

class TupleDesc {
   public:
    class TDItem {
       public:
        const Type* field_type;
        const std::string field_name;

        TDItem(const Type* t, const std::string& s)
            : field_type{t}, field_name{s} {}

        std::string to_string() const {
            return field_name + "(" + field_type->to_string() + ")";
        }

       private:
        constexpr static inline long serial_version_UID = 1;
    };

    TupleDesc(const std::vector<const Type*>& types,
              const std::vector<std::string>& names) {
        // TODO: ASSERT SAME SIZES

        int size = types.size();
        items_.reserve(size);

        for (int i = 0; i < size; ++i) {
            items_.emplace_back(types[i], names[i]);
        }
    }

    TupleDesc(const std::vector<const Type*>& types) {
        // TODO: ASSERT SAME SIZES

        int size = types.size();
        items_.reserve(size);

        for (int i = 0; i < size; ++i) {
            items_.emplace_back(types[i], "");
        }
    }

    int num_fields() const { return items_.size(); }

    std::string get_field_name(int i) const { return items_.at(i).field_name; }

    const Type* get_field_type(int i) const { return items_.at(i).field_type; }

    int index_for_field_name(const std::string& name) const {
        for (int i = 0; i < static_cast<int>(items_.size()); ++i) {
            if (name == items_[i].field_name) {
                return i;
            }
        }

        throw std::invalid_argument("Field name not found.");
    }

    int get_size() const {
        return std::accumulate(items_.begin(), items_.end(), 0,
                               [](const int result, const auto& item) {
                                   return result + item.field_type->get_len();
                               });
    }

    static TupleDesc merge(const TupleDesc& td1, const TupleDesc td2) {
        const int size = td1.num_fields() + td2.num_fields();
        std::vector<TDItem> items;
        items.reserve(size);
        for (const auto& item : td1.items_) {
            items.emplace_back(item);
        }
        for (const auto& item : td2.items_) {
            items.emplace_back(item);
        }
        return TupleDesc(std::move(items));
    };

    bool equals(const TupleDesc& other) const {
        const int size = other.num_fields();
        if (size != num_fields()) {
            return false;
        }

        for (int i = 0; i < size; ++i) {
            if (get_field_name(i) != other.get_field_name(i)) {
                return false;
            }
            if (get_field_type(i) != other.get_field_type(i)) {
                return false;
            }
        }
        return true;
    }

    std::string to_string() const {
        std::string result = "[";
        for (int i = 0; i < static_cast<int>(items_.size()); ++i) {
            result += items_[i].to_string();
            result += ";";
        }
        result += "]";
        return result;
    }

   private:
    TupleDesc(std::vector<TDItem> items) : items_(std::move(items)) {}

    constexpr static inline long serial_version_UID = 1;
    std::vector<TDItem> items_;
};