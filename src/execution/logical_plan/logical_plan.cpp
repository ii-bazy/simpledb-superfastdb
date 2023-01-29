#include "src/execution/logical_plan/logical_plan.hpp"

#include <iostream>
#include <memory>
#include <string_view>

#include "src/storage/TupleDesc.hpp"
#include "src/common/Database.hpp"
#include "src/utils/status_macros.hpp"
#include "src/execution/AggregatorOp.hpp"
#include "src/execution/SeqScan.hpp"
#include "src/execution/Aggregate.hpp"
#include "src/execution/Project.hpp"
#include "src/execution/Filter.hpp"

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

std::unique_ptr<OpIterator> LogicalPlan::PhysicalPlan(TransactionId tid) {
    // TODO: ??? DLACZEGO TO JAKO ZMIENNE W KLASIE
    absl::flat_hash_map<std::string, std::unique_ptr<OpIterator>> subplan_map;
    
    absl::flat_hash_map<std::string, std::string> equiv_map;
    absl::flat_hash_map<std::string, float> filter_selectivities;
    // absl::flat_hash_map<std::string, TABLE STATS> stats_map;

    for (const auto& scan_node : scans) {
        auto ss = std::make_unique<SeqScan>(
            tid,
            scan_node.id,
            scan_node.alias 
        );

        subplan_map[scan_node.alias] = std::move(ss);

        std::string base_table_name = Database::get_catalog()
            .get_table_name(scan_node.id);

        // TODO: STATSY
        // statsMap.put(baseTableName, baseTableStats.get(baseTableName));
    //     filterSelectivities.put(table.alias, 1.0);
    }

    std::cerr << "Phase 1 done" << std::endl;

    for (const auto& filter_node : filters) {
        auto& sub_plan = subplan_map[filter_node.lcol.table];
        if (sub_plan == nullptr) {
            // TODO: exception
        }

        auto it = alias_to_id.find(filter_node.lcol.table);
        auto td = Database::get_catalog().get_tuple_desc(it->second);

        auto filter = std::make_unique<Filter>(
            Predicate(td->index_for_field_name(filter_node.lcol.column), 
                        filter_node.op, filter_node.constant.get()),
            std::move(sub_plan)
        );

        sub_plan = std::move(filter);
    }

    std::cerr << "Phase 2 done" << std::endl;
    // JoinOptimizer jo = new JoinOptimizer(this, joins);
    // joins = jo.orderJoins(statsMap, filterSelectivities, explain);
    
    for (const auto& join_node : joins) {
        std::unique_ptr<OpIterator> plan_1 = nullptr;
        std::unique_ptr<OpIterator> plan_2 = nullptr;
        bool is_subquery_join = join_node.subplan != nullptr;
        std::string t1_name, t2_name;

        if (equiv_map.contains(join_node.lref.table)) {
            t1_name = equiv_map[join_node.lref.table];
        } else {
            t1_name = join_node.lref.table;
        }

        std::cerr << "Phase 3.1 done" << std::endl;

        if (equiv_map.contains(join_node.rref.table)) {
            t2_name = equiv_map[join_node.rref.table];
        } else {
            t2_name = join_node.rref.table;
        }

        std::cerr << "Phase 3.2 done" << std::endl;

        auto plan_ptr = subplan_map.find(t1_name);

        assert(plan_ptr != subplan_map.end());
        std::cerr << "Phase 3.22 done" << std::endl;
        plan_1 = std::move(plan_ptr->second);
        std::cerr << "Phase 3.222 done" << std::endl;
        subplan_map.erase(plan_ptr);

        std::cerr << "Phase 3.3 done" << std::endl;

        if (is_subquery_join) {
            plan_2 = join_node.subplan->PhysicalPlan(tid);

            if (plan_2 == nullptr) { throw std::invalid_argument("Plan 2 not working!!!"); }
        } else {
            auto plan_ptr = subplan_map.find(t2_name);
            plan_2 = std::move(plan_ptr->second);
            subplan_map.erase(plan_ptr);
        }

        std::cerr << "Phase 3.4 done" << std::endl;

        if (plan_1 == nullptr) { throw std::invalid_argument("Unknow table in WHERE clause plan_1"); }
        if (plan_2 == nullptr) { throw std::invalid_argument("Unknow table in WHERE clause plan_1"); }
        
        // TODO: TU WAZNE ZEBY JOINY DZIALALY XD
        // std::unique_ptr<OpIterator> j = nullptr;

        // j = JoinOptimizer.instantiateJoin(lj, plan1, plan2);

        subplan_map[t1_name] = std::move(j);

        std::cerr << "Phase 3.5 done" << std::endl;

        if (!is_subquery_join) {

            std::cerr << "Phase 3.6 done" << std::endl;
            auto t2_subplan_ptr = subplan_map.find(t2_name);
            if (t2_subplan_ptr != subplan_map.end()) {
                subplan_map.erase(t2_subplan_ptr);
            }
            std::cerr << "Phase 3.66 done" << std::endl;
            equiv_map[t2_name] = t1_name;
            std::cerr << "Phase 3.666 done" << std::endl;

            for (auto& [key, value] : equiv_map) {
                if (value == t2_name) {
                    value = t1_name;
                }
            }

            std::cerr << "Phase 3.6666 done" << std::endl;
        }
    }

    std::cerr << "Phase 3 done" << std::endl;

    if (subplan_map.size() > 1 || subplan_map.size() == 0) {
        throw std::invalid_argument("Query does not include join expressions joining all nodes!");
    }

    std::unique_ptr<OpIterator> node = std::move(subplan_map.begin()->second);

    const bool has_agg = agg_column != ColumnRef{};
    const bool has_group = group_by_column != ColumnRef{};
    const bool has_order = order_by_column != ColumnRef{};

    std::vector<int> out_fields;
    std::vector<const Type*> out_types;

    std::cerr << "Phase 4 done" << std::endl;

    std::cerr << "SELECT_NODE CNT\t" << selects.size() << "\n";

    if (!(has_agg || has_group))
    for (auto& select_node : selects) {
        std::cerr << "REF COL\t" << select_node.ref.column << std::endl;

        if (select_node.ref == agg_column) {
            throw 1;
            // out_fields.push_back(group_by_column != ColumnRef{}
            // ? 1
            // : 0);

            // const auto td = node->get_tuple_desc();

            // try {
            //     td->index_for_field_name(select_node.ref.column);
            // } catch (...) {
            //     throw std::invalid_argument("BUG IN PHYSICAL PLAN FIRST IF");
            // }

            // out_types.push_back(Type::INT_TYPE());
        } else if (has_agg) {
            throw 2;
            // if (group_by_column == ColumnRef{}) {
            //     throw std::invalid_argument("PHYSICAL PLAN 2ND IF");
            // }

            // out_fields.push_back(0);
            // const auto td = node->get_tuple_desc();

            // int id;
            // try {
            //     id = td->index_for_field_name(group_by_column.column);
            // } catch (...) {
            //     throw std::invalid_argument("PHYSICAL PLAN 2ND IF INVALID FIELD");
            // }

            // out_types.push_back(td->get_field_type(id));

            // out_fields.push_back(1);
            // id;
            // try {
            //     id = td->index_for_field_name(agg_column.column);
            // } catch (...) {
            //     throw std::invalid_argument("PHYSICAL PLAN 3ND IF INVALID FIELD");
            // }

            // out_types.push_back(td->get_field_type(id));
        } else if (select_node.ref.IsStar() || select_node.ref.column == "*") {
            std::cerr << "STARRRRRRR" << std::endl;
            std::cerr << node << std::endl;
            const auto td = node->get_tuple_desc();
            std::cerr << "STAR TD CNT\t" << td->num_fields() << std::endl;
            for (int i = 0; i < td->num_fields(); ++i) {
                out_fields.push_back(i);
                out_types.push_back(td->get_field_type(i));
            }

            break;
        } else {
            const auto td = node->get_tuple_desc();
            int id;
            try {
                id = td->index_for_field_name(select_node.ref.column);
            } catch (...) {
                throw std::invalid_argument("PHYSICAL PLAN FIELD NOT FOUND");
            }
            out_fields.push_back(id);
            out_types.push_back(td->get_field_type(id));
        }
    }

    std::cerr << "Phase 5 done" << std::endl;


    for (auto p: out_fields) std::cerr << "OUT_FIELDS: " << p << std::endl;

    std::cerr << "agg_column: " << agg_column.column << "\t" << agg_column.IsValid() << "\t" << agg_column.IsStar() << std::endl;
    std::cerr << "Has_agg: " << has_agg << "\thas_order: " << has_order << "\thas_group: " << has_group << std::endl; 

    // throw std::invalid_argument("EEEEEEEEEEEEEEEEEE");

    if (has_agg) {
        const int agg_field = node->get_tuple_desc()->index_for_field_name(agg_column.column);
        node = std::make_unique<Aggregate>(
            std::move(node),
            agg_field,
            has_group 
                ? (node->get_tuple_desc()->index_for_field_name(group_by_column.column))
                : Aggregator::NO_GROUPING,
            agg_type
        );

        std::cerr << "AGGR\t" << node->get_tuple_desc()->to_string() << "\n";
    }

    std::cerr << "Phase 6 done" << std::endl;

    if (has_order) {
        node = std::make_unique<OrderBy>(
            node->get_tuple_desc()->index_for_field_name(order_by_column.column),
            order,
            node
        );
    }

    if (out_fields.size() == 0 && out_types.size() == 0) {
        const auto td = node->get_tuple_desc();
        for (int i = 0; i < td->num_fields(); ++i) {
            out_fields.push_back(i);
            out_types.push_back(td->get_field_type(i));
        }
    }

    return std::make_unique<Project>(
        out_fields,
        out_types,
        std::move(node)
    );
}

