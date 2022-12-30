#pragma once

#include <string>

enum class AggregatorOp {
    NONE,
    MIN,
    MAX,
    SUM,
    AVG,
    COUNT,
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

std::string to_string(AggregatorOp op);
AggregatorOp get_aggregator_op(std::string agg_name);