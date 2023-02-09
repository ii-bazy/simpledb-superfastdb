#pragma once

#include <exception>
#include <memory>
#include <numeric>
#include <vector>

#include "absl/strings/str_cat.h"
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
    };

    TupleDesc(const std::vector<const Type*>& types,
              const std::vector<std::string>& names) {
        if (types.size() != names.size()) {
            throw std::invalid_argument(
                "Types size and names size must be equal.");
        }

        const int size = types.size();
        items_.reserve(size);
        for (int i = 0; i < size; ++i) {
            items_.emplace_back(types[i], names[i]);
        }

        calc_size();
    }

    TupleDesc(const std::vector<const Type*>& types) {
        const int size = types.size();
        items_.reserve(size);
        for (int i = 0; i < size; ++i) {
            items_.emplace_back(types[i], "");
        }

        calc_size();
    }

    int num_fields() const { return items_.size(); }

    std::string get_field_name(int i) const { return items_.at(i).field_name; }

    const Type* get_field_type(int i) const { return items_.at(i).field_type; }

    uint64_t get_size() const {
        assert(size_ != -1);
        return size_;
    }

    int index_for_field_name(const std::string& name) const {
        for (int i = 0; i < static_cast<int>(items_.size()); ++i) {
            if (name == items_[i].field_name) {
                return i;
            }
        }

        throw std::invalid_argument(
            absl::StrCat("Field ", name, " not found."));
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
    TupleDesc(std::vector<TDItem> items) : items_(std::move(items)) {
        calc_size();
    }

    void calc_size() {
        size_ = std::accumulate(items_.begin(), items_.end(), 0,
                                [](const int result, const auto& item) {
                                    return result + item.field_type->get_len();
                                });
    }

    std::vector<TDItem> items_;
    uint64_t size_ = -1;
};