#pragma once

#include <glog/logging.h>

#include <memory>

#include "src/execution/Operator.hpp"
#include "src/execution/Predicate.hpp"
#include "src/storage/Field.hpp"

class Filter : public Operator {
   public:
    Filter(const Predicate p, std::unique_ptr<OpIterator> child)
        : p_(p), child_(std::move(child)), td_(child_->get_tuple_desc()) {}

    const Predicate get_predicate() const { return p_; }

    std::shared_ptr<TupleDesc> get_tuple_desc() { return td_; }

    void rewind() { child_->rewind(); }

    std::shared_ptr<Tuple> fetch_next() {
        LOG(INFO) << "Filter fetch next!";
        while (child_->has_next()) {
            auto next = child_->next();
            if (p_.apply(next.get())) {
                return next;
            }
        }

        return nullptr;
    }

   private:
    const Predicate p_;
    std::unique_ptr<OpIterator> child_;
    std::shared_ptr<TupleDesc> td_;
};