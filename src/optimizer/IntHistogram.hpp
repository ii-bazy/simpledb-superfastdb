#pragma once

#include "src/execution/Predicate.hpp"

class IntHistogram {
public:
    IntHistogram(int buckets, int min, int max)
        : min_ {min}, max_ {max}, bucket_count_ {buckets}, buckets_ {std::vector<int>(buckets, 0)},
        range_per_bucket_ {((max_ - min_) / bucket_count_)} {
        assert(min <= max);
    }

    void add_value(int value) {
        buckets_[value / ((max_ - min_) / bucket_count_)]++;
        all_count_++;
    }

    double estimate_selectivity(const OpType op, const int value) {
        double result = 0;

        switch (op) {
            case OpType::EQUALS: {
                const int target_bucket = bucket_for_value(value);
                result += (double)buckets_[target_bucket] / range_per_bucket_;
                break;
            }
            case OpType::GREATER_THAN: case OpType::GREATER_THAN_OR_EQ: {
                const int target_bucket = bucket_for_value(value);
                for (int i = target_bucket + 1; i < bucket_count_; ++i) {
                    result += buckets_[i];
                }

                result += (1.0 - (double)(value - target_bucket * range_per_bucket_)) * buckets_[target_bucket] / range_per_bucket_;
                break;
            }
            case OpType::LESS_THAN: case OpType::LESS_THAN_OR_EQ: {
                const int target_bucket = bucket_for_value(value);
                for (int i = 0; i < target_bucket; ++i) {
                    result += buckets_[i];
                }

                result += (double)(value - target_bucket * range_per_bucket_) * buckets_[target_bucket] / range_per_bucket_;
                break;
            }
            case OpType::NOT_EQUALS: {
                return 1.0 - estimate_selectivity(OpType::EQUALS, value);
            }
            default:
                throw std::invalid_argument("Unexpected operator in estimate selectivity IntHistogram");
        }

        return result / all_count_;
    }

    double avg_selectivity(OpType op) {
        switch (op) {
            case OpType::EQUALS: {
              double result = 0;
              int non_zero_buckets = 0;
              for (int cnt : buckets_) {
                if (cnt > 0) {
                  result += (double)cnt / all_count_;
                  non_zero_buckets += 1;
                }
              }
              return result / non_zero_buckets;
            }
            case OpType::GREATER_THAN: case OpType::GREATER_THAN_OR_EQ: {
               double result = 0, cur_ppb = 0;
                int non_zero_buckets = 0;
                for (int i = (int)buckets_.size() - 1; i >= 0; --i) {
                  int cnt = buckets_[i];
                  cur_ppb += (double)cnt / all_count_;
                  if (cur_ppb > 0) {
                    non_zero_buckets += 1;
                    result += cur_ppb;
                  }
                }
                return result / non_zero_buckets;
            }
            case OpType::LESS_THAN: case OpType::LESS_THAN_OR_EQ: {
                double result = 0, cur_ppb = 0;
                int non_zero_buckets = 0;
                for (int cnt : buckets_) {
                  cur_ppb += (double)cnt / all_count_;
                  if (cur_ppb > 0) {
                    non_zero_buckets += 1;
                    result += cur_ppb;
                  }
                }
                return result / non_zero_buckets;
            }
            case OpType::NOT_EQUALS: {
                return 1.0 - avg_selectivity(OpType::EQUALS);
            }
            default:
                throw std::invalid_argument("Unexpected operator in avg selectivity IntHistogram");
        }
    }

private:
    const int min_;
    const int max_;
    const int bucket_count_;
    const int range_per_bucket_;
    
    int all_count_ = 0;
    std::vector<int> buckets_;

    inline int bucket_for_value(const int value) {
        return value / range_per_bucket_;
    }
};