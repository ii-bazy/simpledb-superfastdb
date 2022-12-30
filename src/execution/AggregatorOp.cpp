#include "src/execution/AggregatorOp.hpp"

std::string to_string(AggregatorOp op) {
    switch (op) {
        case AggregatorOp::NONE:
            return "none";
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

AggregatorOp get_aggregator_op(std::string agg_name) {
    if (agg_name == "none") {
        return AggregatorOp::NONE;
    }
    if (agg_name == "min") {
        return AggregatorOp::MIN;
    }
    if (agg_name == "max") {
        return AggregatorOp::MAX;
    }
    if (agg_name == "sum") {
        return AggregatorOp::SUM;
    }
    if (agg_name == "avg") {
        return AggregatorOp::AVG;
    }
    if (agg_name == "count") {
        return AggregatorOp::COUNT;
    }
    throw "?";
}