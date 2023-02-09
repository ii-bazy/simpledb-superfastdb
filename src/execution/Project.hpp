#pragma once

#include "src/execution/OpIterator.hpp"

class Project : public OpIterator {
   public:
    Project(const std::vector<int> fields, const std::vector<const Type*> types,
            std::unique_ptr<OpIterator> child)
        : child_{std::move(child)}, out_field_ids{fields} {
        std::vector<std::string> field_names;
        const auto child_td = child_->get_tuple_desc();

        for (const auto field_id : fields) {
            field_names.push_back(child_td->get_field_name(field_id));
        }

        td_ = std::make_shared<TupleDesc>(types, field_names);
    }

    std::shared_ptr<Tuple> next() {
        auto child_t = child_->next();

        auto t = std::make_shared<Tuple>(td_);
        t->set_record_id(child_t->get_record_id());

        for (int i = 0; i < static_cast<int>(out_field_ids.size()); ++i) {
            t->set_field(i, child_t->get_field(out_field_ids[i]));
        }

        return t;
    }

    void rewind() { child_->rewind(); }

    bool has_next() { return child_->has_next(); }

    std::shared_ptr<TupleDesc> get_tuple_desc() { return td_; }

    void explain(std::ostream& os, int indent) override {
        os << std::string(indent, ' ') + "-> Project over fields: ";
        for (const int idx : out_field_ids) {
            os << idx << " ";
        }
        os << "\n";
        os << std::string(indent + td_indent_, ' ')
           << "TD: " << td_->to_string() << "\n";
        child_->explain(os, indent + child_indent_);
    }

   private:
    std::unique_ptr<OpIterator> child_ = nullptr;
    std::shared_ptr<TupleDesc> td_ = nullptr;

    const std::vector<int> out_field_ids;
};