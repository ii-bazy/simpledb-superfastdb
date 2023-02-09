#include "src/flags.hpp"

DEFINE_string(convert, "", "Path to file to convert to binary.");
DEFINE_string(types, "", "Types of columns in table.");
DEFINE_string(schema_path, "", "Path to schema.");
DEFINE_bool(benchmark, false, "Run benchmark. TODO:");
DEFINE_bool(explain, false, "Display execution plan.");
DEFINE_bool(use_materialize, true, "Should materialize inner loop in join");
DEFINE_uint32(materialize_size, 4, "Available memory for materialization in MB.");
DEFINE_bool(use_join_optimization, true, "Use Selinger's algorithm to optimize joins order");