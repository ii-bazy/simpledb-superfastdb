#pragma once
#include <gflags/gflags.h>

DECLARE_string(convert);
DECLARE_string(types);
DECLARE_string(schema_path);
DECLARE_bool(benchmark);
DECLARE_bool(explain);
DECLARE_bool(use_materialize);
DECLARE_uint32(materialize_size);
DECLARE_bool(use_join_optimization);