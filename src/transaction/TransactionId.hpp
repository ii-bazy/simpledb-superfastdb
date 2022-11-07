#pragma once

#include <atomic>

class TransactionId {
   public:
    TransactionId() : my_id_(counter++) {}

    long long get_id() const { return my_id_; }

   private:
    static inline std::atomic<int> counter{0};
    long long my_id_;
};