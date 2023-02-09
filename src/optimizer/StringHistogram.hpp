#pragma once
#include "src/optimizer/IntHistogram.hpp"

class StringHistogram {
   public:
    StringHistogram(int buckets)
        : int_histogram_(buckets, min_val(), max_val()) {}

    int string_to_int(std::string s) const {
        int v = 0;
        for (int i = 3; i >= 0; --i) {
            if (s.size() > 3 - i) {
                const int ci = s[3 - i];
                v += (ci) << (i * 8);
            }
        }

        if (!(s == "" || s == "zzzz")) {
            if (v < min_val()) {
                v = min_val();
            }
            if (v > max_val()) {
                v = max_val();
            }
        }

        return v;
    }

    int max_val() const { return string_to_int("zzzz"); }

    int min_val() const { return string_to_int(""); }

    void add_value(std::string s) {
        int val = string_to_int(s);
        int_histogram_.add_value(val);
    }

    double estimate_selectivity(OpType op, std::string s) const {
        int val = string_to_int(s);
        return int_histogram_.estimate_selectivity(op, val);
    }

    double avg_selectivity(OpType op) const {
        return int_histogram_.avg_selectivity(op);
    }

   private:
    IntHistogram int_histogram_;
};