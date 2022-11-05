#pragma once

#include <atomic>

class TransactionId {
public:
    TransactionId() {
        my_id_ = counter++;
    }

    long long get_id() const {
        return my_id_;
    }

private:
    constexpr static inline long long serial_version_uid = 1;
    static inline std::atomic<int> counter{0};
    long long my_id_;
};