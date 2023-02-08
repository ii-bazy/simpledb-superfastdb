#pragma once

#include <memory>

#include "src/execution/JoinPredicate.hpp"
#include "src/execution/Operator.hpp"

class Join : public Operator {
   public:
    Join(JoinPredicate predicate, std::unique_ptr<OpIterator> child1,
         std::unique_ptr<OpIterator> child2)
        : predicate_(std::move(predicate)),
          child1_(std::move(child1)),
          child2_(std::move(child2)),
          td_(std::make_shared<TupleDesc>(TupleDesc::merge(
              *child1_->get_tuple_desc(), *child2_->get_tuple_desc()))) {}

    std::shared_ptr<Tuple> fetch_next() {
        if (current_tuple_ == nullptr) {
            if (!child1_->has_next()) {
                return nullptr;
            }
            current_tuple_ = child1_->next();
            child1_iters += 1;
        }
        while (true) {
            while (child2_->has_next()) {
                // if predicate
                auto nxt = child2_->next();
                if (!child2_rewind) {
                    child2_iters += 1;
                }
                if (predicate_.apply(current_tuple_.get(), nxt.get())) {
                    return join_tuples(current_tuple_, nxt);
                }
            }
            child2_->rewind();
            child2_rewind = true;
            if (child1_->has_next()) {
                current_tuple_ = child1_->next();
                child1_iters += 1;
            } else {
                current_tuple_ = nullptr;
                break;
            }
        }
        return nullptr;
    }

    void rewind() {
        child1_->rewind();
        child2_->rewind();
    }

    std::shared_ptr<TupleDesc> get_tuple_desc() { return td_; }

    void explain(std::ostream& os, int indent) override {
        os << absl::StrCat(std::string(indent, ' '),
                           "-> Join : ", predicate_.to_string(), "\n");
        os << std::string(indent + td_indent_, ' ')
           << "TD: " << td_->to_string() << "\n";
        child1_->explain(os, indent + child_indent_);
        child2_->explain(os, indent + child_indent_);
    }

    ~Join() {
        LOG(ERROR) << absl::StrCat("~Join(", child1_iters, ",", child2_iters,
                                   ")");
    }

   private:
    std::shared_ptr<Tuple> join_tuples(std::shared_ptr<Tuple> t1,
                                       std::shared_ptr<Tuple> t2) {
        std::shared_ptr<Tuple> t = std::make_shared<Tuple>(td_);
        for (int i = 0; i < t1->num_fields(); ++i) {
            t->set_field(i, t1->get_field(i));
        }
        for (int i = 0; i < t2->num_fields(); ++i) {
            t->set_field(t1->num_fields() + i, t2->get_field(i));
        }

        return t;
    }

    const JoinPredicate predicate_;
    std::unique_ptr<OpIterator> child1_;
    std::unique_ptr<OpIterator> child2_;
    std::shared_ptr<TupleDesc> td_;
    // current tuple from child1
    std::shared_ptr<Tuple> current_tuple_;
    int child1_iters = 0;
    int child2_iters = 0;
    bool child2_rewind = false;
};
