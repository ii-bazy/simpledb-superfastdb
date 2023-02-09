#pragma once

#include "src/execution/Predicate.hpp"

class IntHistogram {
   public:
    IntHistogram(int buckets, int min, int max)
        : min_{min},
          max_{max},
          bucket_count_{std::min(buckets, max - min + 1)},
          buckets_{std::vector<int>(bucket_count_, 0)},
          range_per_bucket_{
              ((max_ - min_ + 1 + bucket_count_ - 1) / bucket_count_)} {
        assert(min <= max);
    }

    void add_value(int value) {
        assert(min_ <= value && value <= max_);
        // LOG(INFO) << "minmax: " << min_ << " " << max_;
        // LOG(INFO) << value << " " << range_per_bucket_ << " "
        //     << value / range_per_bucket_ << " " << bucket_count_;
        buckets_[bucket_for_value(value)]++;
        all_count_++;
    }

    double estimate_selectivity(const OpType op, const int value) const {
        double result = 0;

        switch (op) {
            case OpType::EQUALS: {
                if (value < min_ || value > max_) return 0;
                const int target_bucket = bucket_for_value(value);
                result += (double)buckets_[target_bucket] / range_per_bucket_;
                break;
            }
            case OpType::GREATER_THAN: {
                if (value > max_) return 0;
                if (value < min_) return 1;
                const int target_bucket = bucket_for_value(value);
                for (int i = target_bucket + 1; i < bucket_count_; ++i) {
                    result += buckets_[i];
                }

                result += (double)(bucket_max_value(target_bucket) - value) *
                          buckets_[target_bucket] / range_per_bucket_;
                break;
            }
            case OpType::GREATER_THAN_OR_EQ: {
                if (value > max_) return 0;
                if (value < min_) return 1;
                const int target_bucket = bucket_for_value(value);
                for (int i = target_bucket + 1; i < bucket_count_; ++i) {
                    result += buckets_[i];
                }

                result +=
                    (double)(bucket_max_value(target_bucket) - value + 1) *
                    buckets_[target_bucket] / range_per_bucket_;
                break;
            }
            case OpType::LESS_THAN: {
                if (value < min_) return 0;
                if (value > max_) return 1;
                const int target_bucket = bucket_for_value(value);
                for (int i = 0; i < target_bucket; ++i) {
                    result += buckets_[i];
                }
                result += (double)(value - bucket_min_value(target_bucket)) *
                          buckets_[target_bucket] / range_per_bucket_;
                break;
            }
            case OpType::LESS_THAN_OR_EQ: {
                if (value < min_) return 0;
                if (value > max_) return 1;
                const int target_bucket = bucket_for_value(value);
                for (int i = 0; i < target_bucket; ++i) {
                    result += buckets_[i];
                }
                result +=
                    (double)(value - bucket_min_value(target_bucket) + 1) *
                    buckets_[target_bucket] / range_per_bucket_;
                break;
            }
            case OpType::NOT_EQUALS: {
                return 1.0 - estimate_selectivity(OpType::EQUALS, value);
            }
            default:
                throw std::invalid_argument(
                    "Unexpected operator in estimate selectivity IntHistogram");
        }

        return result / all_count_;
    }

    double avg_selectivity(OpType op) const {
        switch (op) {
            case OpType::EQUALS: {
                // TODO: ?
                int non_zero_buckets = 0;
                for (int cnt : buckets_) {
                    if (cnt > 0) {
                        non_zero_buckets += 1;
                    }
                }
                return 1.0 / non_zero_buckets;
            }
            case OpType::GREATER_THAN:
            case OpType::GREATER_THAN_OR_EQ: {
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
            case OpType::LESS_THAN:
            case OpType::LESS_THAN_OR_EQ: {
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
                throw std::invalid_argument(
                    "Unexpected operator in avg selectivity IntHistogram");
        }
    }

   private:
    const int min_;
    const int max_;
    const int bucket_count_;
    const int range_per_bucket_;

    int all_count_ = 0;
    std::vector<int> buckets_;

    inline int bucket_for_value(const int value) const {
        return (value - min_) / range_per_bucket_;
    }

    inline int bucket_min_value(const int index) const {
        return min_ + index * range_per_bucket_;
    }

    inline int bucket_max_value(const int index) const {
        return min_ + (index + 1) * range_per_bucket_ - 1;
    }
};