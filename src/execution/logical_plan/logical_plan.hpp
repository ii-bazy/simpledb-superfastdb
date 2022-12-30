#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "hsql/SQLParser.h"
#include "src/execution/logical_plan/column_ref.hpp"
#include "src/execution/OpIterator.hpp"
#include "src/execution/AggregatorOp.hpp"
#include "src/execution/logical_plan/filter_node.hpp"
#include "src/execution/logical_plan/join_node.hpp"
#include "src/execution/logical_plan/scan_node.hpp"
#include "src/execution/logical_plan/select_node.hpp"
#include "src/execution/OrderBy.hpp"
#include "src/common/Catalog.hpp"

class LogicalPlan {
   public:
    LogicalPlan() {}

    absl::Status AddFilterColCol(ColumnRef lref, OpType op, ColumnRef rref);
    absl::Status AddFilterColConst(ColumnRef ref, OpType op,
                                   std::unique_ptr<Field> const_field);

    absl::Status AddJoin(ColumnRef lref, OpType op, ColumnRef rref);
    absl::Status AddSubqueryJoin(ColumnRef ref, OpType op, LogicalPlan subplan);

    absl::Status AddScan(int table_id, std::string_view alias);

    absl::Status AddGroupBy(ColumnRef ref);

    absl::Status AddAggregate(const std::string& agg_fun, ColumnRef ref);

    absl::Status AddSelect(ColumnRef ref);

    absl::Status AddOrderBy(ColumnRef ref, OrderBy::Order asc);

    absl::StatusOr<ColumnRef> GetColumnRef(std::string_view table,
                                                   std::string_view column);

    absl::StatusOr<ColumnRef> GetColumnRef(char* table, char* column);

    absl::StatusOr<const Type*> GetColumnType(ColumnRef ref);

    void Dump();

   private:
    // Returns InvalidArgumentError if this reference is invalid.
    absl::Status CheckColumnRef(ColumnRef ref);

    std::vector<FilterNode> filters;
    std::vector<JoinNode> joins;
    std::vector<SelectNode> selects;
    std::vector<ScanNode> scans;

    absl::flat_hash_map<std::string, int> alias_to_id;
    AggregatorOp agg_type = AggregatorOp::NONE;
    ColumnRef agg_column = {};
    ColumnRef group_by_column = {};
    ColumnRef order_by_column = {};
    OrderBy::Order order;
};
