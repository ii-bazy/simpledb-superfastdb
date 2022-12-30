#include "src/execution/logical_plan/logical_plan.hpp"

#include <iostream>
#include <memory>
#include <string_view>

#include "src/storage/TupleDesc.hpp"
#include "src/common/Database.hpp"
#include "src/utils/status_macros.hpp"
#include "src/execution/AggregatorOp.hpp"

absl::Status LogicalPlan::CheckColumnRef(ColumnRef ref) {
    if (ref.IsStar()) return absl::OkStatus();
    auto it = alias_to_id.find(ref.table);
    if (it == alias_to_id.end()) {
        return absl::InvalidArgumentError(
            absl::StrCat("Unknown alias ", ref.table));
    }
    auto td = Database::get_catalog().get_tuple_desc(it->second);
    td->index_for_field_name(ref.column);
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddScan(int table_id, std::string_view alias) {
    if (alias_to_id.contains(alias)) {
        return absl::InvalidArgumentError(
            absl::StrCat("Another use of alias ", alias));
    }
    scans.push_back(ScanNode(table_id, alias));
    alias_to_id[alias] = table_id;
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddJoin(ColumnRef lref, OpType op, ColumnRef rref) {
    RETURN_IF_ERROR(CheckColumnRef(lref));
    RETURN_IF_ERROR(CheckColumnRef(rref));
    joins.push_back(JoinNode(lref, op, rref));
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddSubqueryJoin(ColumnRef ref, OpType op,
                                          LogicalPlan subplan) {
    RETURN_IF_ERROR(CheckColumnRef(ref));
    std::unique_ptr<LogicalPlan> subplan_ptr =
        std::make_unique<LogicalPlan>(std::move(subplan));
    joins.push_back(JoinNode(ref, op, std::move(subplan_ptr)));
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddFilterColCol(ColumnRef lref, OpType op,
                                          ColumnRef rref) {
    RETURN_IF_ERROR(CheckColumnRef(lref));
    RETURN_IF_ERROR(CheckColumnRef(rref));
    filters.push_back(FilterNode(lref, op, rref));
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddFilterColConst(
    ColumnRef ref, OpType op, std::unique_ptr<Field> const_field) {
    RETURN_IF_ERROR(CheckColumnRef(ref));
    filters.push_back(FilterNode(ref, op, std::move(const_field)));
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddGroupBy(ColumnRef ref) {
    RETURN_IF_ERROR(CheckColumnRef(ref));
    group_by_column = ref;
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddAggregate(const std::string& agg_fun,
                                       ColumnRef ref) {
    agg_type = get_aggregator_op(agg_fun);
    RETURN_IF_ERROR(CheckColumnRef(ref));
    agg_column = ref;
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddSelect(ColumnRef ref) {
    RETURN_IF_ERROR(CheckColumnRef(ref));
    selects.push_back(SelectNode(ref));
    return absl::OkStatus();
}

absl::Status LogicalPlan::AddOrderBy(ColumnRef ref, OrderBy::Order asc) {
    RETURN_IF_ERROR(CheckColumnRef(ref));
    order_by_column = ref;
    order = asc;
    return absl::OkStatus();
}

absl::StatusOr<ColumnRef> LogicalPlan::GetColumnRef(std::string_view table,
                                                    std::string_view column) {
    if (table != "") {
        return ColumnRef(table, column);
    }

    for (auto& it : alias_to_id) {
        auto td = Database::get_catalog().get_tuple_desc(it.second);
        td->index_for_field_name(std::string(column));
        if (table != "") {
            return absl::InvalidArgumentError(absl::StrCat(
                "Column ", column, " without table name is ambigous"));
        }
        table = it.first;
    }

    if (table == "") {
        return absl::InvalidArgumentError(
            absl::StrCat("Column ", column, " does not appear in any table"));
    }
    return ColumnRef(table, column);
}

absl::StatusOr<ColumnRef> LogicalPlan::GetColumnRef(char* table,
                                                            char* column) {
    return GetColumnRef(table ? table : "", column ? column : "");
}

absl::StatusOr<const Type*> LogicalPlan::GetColumnType(ColumnRef ref) {
    auto it = alias_to_id.find(ref.table);
    if (it == alias_to_id.end()) {
        return absl::InvalidArgumentError(absl::StrCat(
            "Invalid column reference: no table with alias ", ref.table));
    }
    auto td = Database::get_catalog().get_tuple_desc(it->second);
    const int column_idx = td->index_for_field_name(ref.column);
    const Type* t = td->get_field_type(column_idx);
    return t;
}

void LogicalPlan::Dump() {
    std::cout << "alias_to_id: (" << alias_to_id.size() << ")\n";

    for (auto& it : alias_to_id) {
        std::cout << it.first << " -> " << it.second << "\n";
    }

    std::cout << "Scan nodes: (" << scans.size() << ")\n";
    for (auto& it : scans) {
        std::cout << static_cast<std::string>(it) << "\n";
    }

    std::cout << "Select nodes: (" << selects.size() << ")\n";
    for (auto& it : selects) {
        std::cout << static_cast<std::string>(it) << "\n";
    }

    std::cout << "Filter nodes: (" << filters.size() << ")\n";
    for (auto& it : filters) {
        std::cout << static_cast<std::string>(it) << "\n";
    }

    std::cout << "Join nodes: (" << joins.size() << ")\n";
    for (auto& it : joins) {
        std::cout << static_cast<std::string>(it.lref) + " " +
                         to_string(it.op)
                  << " ";
        if (it.type == JoinNode::COL_COL) {
            std::cout << static_cast<std::string>(it.rref) << "\n";
        } else {
            std::cout << "Subplan(";
            it.subplan->Dump();
            std::cout << ")\n";
        }
    }

    std::cout << "GroupBy: " << static_cast<std::string>(group_by_column)
              << "\n";
    std::cout << "Aggregate: " << to_string(agg_type)
              << "(" << static_cast<std::string>(agg_column) << ")\n";

    std::cout << "OrderBy: " << static_cast<std::string>(order_by_column)
              << " ";
    std::cout << (order == OrderBy::ASCENDING ? "asc" : "desc") << "\n";
};
