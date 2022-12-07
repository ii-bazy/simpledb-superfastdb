#pragma once

#include <string>

enum class AggregatorOp {
    MIN, MAX, SUM, AVG, COUNT,
    /**
        * SUM_COUNT: compute sum and count simultaneously, will be
        * needed to compute distributed avg in lab7.
        */
    SUM_COUNT,
    /**
        * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
        * will be used to compute distributed avg in lab7.
        */
    SC_AVG
};

std::string to_string(AggregatorOp op) {
    switch (op) {
        case AggregatorOp::MIN:
            return "min";
        case AggregatorOp::MAX:
            return "max";
        case AggregatorOp::SUM:
            return "sum";
        case AggregatorOp::AVG:
            return "avg";
        case AggregatorOp::COUNT:
            return "count";
        case AggregatorOp::SUM_COUNT:
        case AggregatorOp::SC_AVG:
            return "lab7 ;)";
        default:
            throw "?";
    }
}