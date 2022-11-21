#pragma once

#include <memory>
#include <vector>

#include "src/storage/RecordId.hpp"
#include "src/storage/TupleDesc.hpp"

class Tuple {
   public:
    Tuple(std::shared_ptr<TupleDesc> td) : td_(std::move(td)) {
        fields_.resize(td_->num_fields());
    }

    const std::shared_ptr<TupleDesc>& get_tuple_desc() const { return td_; }

    int num_fields() const { return fields_.size(); }

    void set_field(int i, std::shared_ptr<Field> f) {
        fields_.at(i) = std::move(f);
    }

    std::shared_ptr<Field> get_field(int i) const {
        return fields_.at(i);
    }

    void set_record_id(std::shared_ptr<RecordId> record_id) {
        record_id_ = std::move(record_id);
    }

    const std::shared_ptr<RecordId>& get_record_id() const {
        return record_id_;
    }

    std::string to_string() const {
        std::string res;
        for (int i = 0; i < num_fields(); ++i) {
            res += get_field(i)->to_string();
            if (i + 1 < num_fields()) {
                res += "\t";
            }
        }
        // res += "\n";
        return res;
    }

   private:
    std::shared_ptr<TupleDesc> td_;
    std::vector<std::shared_ptr<Field>> fields_;
    std::shared_ptr<RecordId> record_id_;
};