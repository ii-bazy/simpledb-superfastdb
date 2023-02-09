#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "src/execution/Join.hpp"
#include "src/execution/JoinPredicate.hpp"
#include "src/execution/logical_plan/logical_plan.hpp"
#include "src/flags.hpp"
#include "src/optimizer/CostCard.hpp"
#include "src/optimizer/PlanCache.hpp"
#include "src/optimizer/TableStats.hpp"

class JoinOptimizer {
   public:
    JoinOptimizer(LogicalPlan* logical_plan, const std::vector<JoinNode> joins)
        : logical_plan_{logical_plan}, joins_{joins} {}

    static std::unique_ptr<OpIterator> instantiate_join(
        const JoinNode lj, std::unique_ptr<OpIterator> plan1,
        std::unique_ptr<OpIterator> plan2) {
        int t1id = 0, t2id = 0;
        std::unique_ptr<OpIterator> j = nullptr;

        try {
            t1id =
                plan1->get_tuple_desc()->index_for_field_name(lj.lref.column);
        } catch (...) {
            throw std::invalid_argument("Unknown field instantiatejoin ljf1");
        }

        if (lj.subplan != nullptr) {
            t2id = 0;
        } else {
            try {
                t2id = plan2->get_tuple_desc()->index_for_field_name(
                    lj.rref.column);
            } catch (...) {
                throw std::invalid_argument(
                    "Unknown field instantiatejoin ljf2");
            }
        }

        JoinPredicate p = JoinPredicate(t1id, lj.op, t2id);

        j = std::make_unique<Join>(p, std::move(plan1), std::move(plan2));

        return j;
    }

    double estimate_join_cost(const JoinNode& j, int card1, int card2,
                              double cost1, double cost2) {
        if (j.subplan != nullptr) {
            return card1 + cost1 + cost2;
        } else {
            // Currently supports only nested loops
            return cost1 + card1 * cost2;
            //  + card1 * card2 / 10000;
        }
    }

    int estimate_join_cardinality(
        const JoinNode& j, int card1, int card2, bool t1pkey, bool t2pkey,
        const absl::flat_hash_map<std::string, TableStats>& stats) {
        if (j.subplan != nullptr) {
            return card1;
        } else {
            return estimate_table_join_cardinality(
                j.op, j.lref.table, j.rref.table, j.lref.column, j.rref.column,
                card1, card2, t1pkey, t2pkey, stats,
                logical_plan_->get_table_alias_to_id_mapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     */
    static int estimate_table_join_cardinality(
        OpType joinOp, const std::string& table1Alias,
        const std::string& table2Alias, const std::string& field1PureName,
        const std::string& field2PureName, int card1, int card2, bool t1pkey,
        bool t2pkey, const absl::flat_hash_map<std::string, TableStats>& stats,
        const absl::flat_hash_map<std::string, int>& tableAliasToId) {
        int lcard = 1;
        int rcard = 1;

        if (!t1pkey) {
            auto t1_name = Database::get_catalog().get_table_name(
                tableAliasToId.at(table1Alias));
            auto selectivity = stats.at(t1_name).avg_selectivity(
                Database::get_catalog()
                    .get_tuple_desc(tableAliasToId.at(table1Alias))
                    ->index_for_field_name(field1PureName),
                flip_op_type(joinOp));

            lcard = stats.at(t1_name).estimate_table_cardinality(selectivity);
            LOG(INFO) << absl::StrCat(
                "Table: ", table1Alias, " Column: ", field1PureName,
                " Selectivity ", selectivity, " Card: ", lcard,
                " Op: ", to_string(flip_op_type(joinOp)));
        }

        if (!t2pkey) {
            auto t2_name = Database::get_catalog().get_table_name(
                tableAliasToId.at(table2Alias));
            auto selectivity = stats.at(t2_name).avg_selectivity(
                Database::get_catalog()
                    .get_tuple_desc(tableAliasToId.at(table2Alias))
                    ->index_for_field_name(field2PureName),
                joinOp);
            LOG(INFO) << "Table name: " << t2_name
                      << "\tColumn name: " << field2PureName
                      << "\tSelectivity: " << selectivity;
            rcard = stats.at(t2_name).estimate_table_cardinality(selectivity);

            LOG(INFO) << absl::StrCat(
                "Table: ", table2Alias, " Column: ", field2PureName,
                " Selectivity ", selectivity, " Card: ", lcard,
                " Op: ", to_string(joinOp));
        }

        const int card = std::max(1, lcard * rcard);
        return card;
    }

    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * the Lab 3 description for hints on how this should be implemented.
     *
     * @param stats               Statistics for each table involved in the
     * join, referenced by base table names, not alias
     * @param filterSelectivities Selectivities of the filter predicates on each
     * table in the join, referenced by table alias (if no alias, the base table
     *                            name)
     * @param explain             Indicates whether your code should explain its
     * query plan or simply execute it
     * @return A List<LogicalJoinNode> that stores joins in the left-deep
     *         order in which they should be executed.
     * @throws ParsingException when stats or filter selectivities is missing a
     * table in the join, or or when another internal error occurs
     */
    std::vector<JoinNode> orderJoins(
        absl::flat_hash_map<std::string, TableStats>& stats,
        absl::flat_hash_map<std::string, float>& filterSelectivities,
        bool explain) {
        if (!FLAGS_use_join_optimization) {
            return joins_;
        }

        LOG(INFO) << "Joins size: " << joins_.size();
        PlanCache pc((1ULL << joins_.size()));

        for (uint64_t mask = 0; mask < (1ULL << joins_.size()); ++mask) {
            CostCard best_cost_card;

            LOG(INFO) << "SOLVING MASK:" << mask;

            for (uint64_t i = 0; i < joins_.size(); ++i) {
                // for (int64_t i = (joins_.size() - 1); i >= 0; --i) {
                if (!(mask & (1ULL << i))) {
                    continue;
                }

                LOG(INFO) << "Trying with " << i << " as lead!";
                LOG(INFO) << "Join node: " << std::string(joins_[i]);

                update_cost_and_card_of_subplan(stats, filterSelectivities,
                                                joins_[i], mask ^ (1ULL << i),
                                                pc, best_cost_card);
            }

            LOG(INFO) << "Cost for mask: " << mask << " = "
                      << best_cost_card.cost;
            pc.addPlan(mask, std::move(best_cost_card));
        }

        uint64_t whole_mask = (1ULL << joins_.size()) - 1;

        auto best_plan = pc.getCostCard(whole_mask);
        LOG(INFO) << "Best plan cost: " << best_plan.cost
                  << "\tcard: " << best_plan.card;
        for (auto lj : best_plan.plan) {
            LOG(INFO) << "NEXT ITEM IN PLAN\t" << lj.lref.table << "."
                      << lj.lref.column << "WITH  " << lj.rref.table << "."
                      << lj.rref.column;
        }

        // exit(0);
        return pc.getCostCard(whole_mask).plan;
    }

   private:
    LogicalPlan* logical_plan_;
    std::vector<JoinNode> joins_;

    /**
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     *
     * @param stats               table stats for all of the tables, referenced
     * by table names rather than alias (see {@link #orderJoins})
     * @param filterSelectivities the selectivities of the filters over each of
     * the tables (where tables are indentified by their alias or name if no
     *                            alias is given)
     * @param joinToRemove        the join to remove from joinSet
     * @param joinSet             the set of joins being considered
     * @param bestCostSoFar       the best way to join joinSet so far (minimum
     * of previous invocations of computeCostAndCardOfSubplan for this joinSet,
     *                            from returned CostCard)
     * @param pc                  the PlanCache for this join; should have
     * subplans for all plans of size joinSet.size()-1
     * @return A {@link CostCard} objects desribing the cost, cardinality,
     *         optimal subplan
     * @throws ParsingException when stats, filterSelectivities, or pc object is
     * missing tables involved in join
     */
    void update_cost_and_card_of_subplan(
        absl::flat_hash_map<std::string, TableStats>& stats,
        absl::flat_hash_map<std::string, float>& filter_selectivities,
        JoinNode j, uint64_t news, PlanCache& pc, CostCard& best_cost_card) {
        std::vector<JoinNode> prevBest = pc.getCostCard(news).plan;

        if (logical_plan_->get_table_id(j.lref.table) == 0)
            throw std::invalid_argument("Unknown table ccacos1");
        if (logical_plan_->get_table_id(j.rref.table) == 0)
            throw std::invalid_argument("Unknown table ccacos2");

        std::string table1Name = Database::get_catalog().get_table_name(
            logical_plan_->get_table_id(j.lref.table));

        std::string table2Name = Database::get_catalog().get_table_name(
            logical_plan_->get_table_id(j.rref.table));

        std::string table1Alias = j.lref.table;
        std::string table2Alias = j.rref.table;

        double t1cost, t2cost;
        int t1card, t2card;
        bool leftPkey, rightPkey;

        if (news == 0) {  // base case -- both are base relations
            t1cost = stats[table1Name].estimate_scan_cost();
            t1card = stats[table1Name].estimate_table_cardinality(
                filter_selectivities[table1Alias]);
            leftPkey = isPkey(table1Alias, j.lref.column);

            t2cost = stats[table2Name].estimate_scan_cost();
            t2card = stats[table2Name].estimate_table_cardinality(
                filter_selectivities[table2Alias]);
            rightPkey = isPkey(table2Alias, j.rref.column);

            LOG(INFO) << "SINGLE JOIN!";
            LOG(INFO) << absl::StrCat("Left cost: ", t1cost, " card: ", t1card);
            LOG(INFO) << absl::StrCat("Right cost: ", t2cost,
                                      " card: ", t2card);

            // std::cerr << "LEFT: " << t1cost << " " << t1card << "\n";
            // std::cerr << "RIGHT: " << t2cost << " " << t2card << "\n";
        } else {
            // news is not empty -- figure best way to join j to news
            auto cc = pc.getCostCard(news);

            LOG(INFO) << absl::StrCat("Best subplan for news ", news,
                                      " cost: ", cc.cost, " card: ", cc.card);

            prevBest = cc.plan;

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest.empty()) {
                return;
            }

            double prevBestCost = cc.cost;
            int bestCard = cc.card;

            // estimate cost of right subtree
            if (doesJoin(prevBest, table1Alias)) {  // j.t1 is in prevBest
                LOG(INFO) << "Does Join to table1Alias: " << table1Alias;
                t1cost = prevBestCost;  // left side just has cost of whatever
                // left
                // subtree is
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j.rref.table == ""
                             ? 0
                             : stats[table2Name].estimate_scan_cost();
                t2card = j.rref.table == ""
                             ? 0
                             : stats[table2Name].estimate_table_cardinality(
                                   filter_selectivities[j.rref.table]);

                rightPkey =
                    j.rref.table != "" && isPkey(j.rref.table, j.rref.column);

            } else if (doesJoin(prevBest,
                                table2Alias)) {  // j.t2 is in prevbest
                LOG(INFO) << "Does Join to table2Alias: " << table2Alias;
                // (both shouldn't be)
                t2cost = prevBestCost;  // left side just has cost of whatever
                // left
                // subtree is
                t2card = bestCard;
                rightPkey = hasPkey(prevBest);
                t1cost = stats[table1Name].estimate_scan_cost();
                t1card = stats[table1Name].estimate_table_cardinality(
                    filter_selectivities[j.lref.table]);

                leftPkey = isPkey(j.lref.table, j.lref.column);

            } else {
                LOG(INFO) << "CROSS PRODUCT!";
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return;
            }

            LOG(INFO) << absl::StrCat("left = (", t1cost, ",", t1card, ")",
                                      leftPkey);
            LOG(INFO) << absl::StrCat("right = (", t2cost, ",", t2card, ")",
                                      rightPkey);
        }

        // case where prevbest is left
        double cost1 = estimate_join_cost(j, t1card, t2card, t1cost, t2cost);

        JoinNode j2 = j.swapInnerOuter();
        double cost2 = estimate_join_cost(j2, t2card, t1card, t2cost, t1cost);

        LOG(INFO) << "Checking swap!";

        LOG(INFO) << "Order1 cost: " << cost1;
        LOG(INFO) << "Order2 cost: " << cost2;

        if (cost2 < cost1) {
            LOG(INFO) << "Swaping!";
            j = j2;
            cost1 = cost2;
            std::swap(leftPkey, rightPkey);
        }

        LOG(INFO) << "J: " << std::string(j);

        if (best_cost_card.plan.size() > 0u && cost1 >= best_cost_card.cost)
            return;

        CostCard cc;

        cc.card = estimate_join_cardinality(j, t1card, t2card, leftPkey,
                                            rightPkey, stats);

        LOG(INFO) << "cost: " << cost1 << " card: " << cc.card;
        cc.cost = cost1;
        cc.plan = prevBest;
        cc.plan.push_back(j);  // prevbest is left -- add new join to end
        best_cost_card = std::move(cc);
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    bool doesJoin(std::vector<JoinNode>& joinlist, const std::string& table) {
        for (auto& j : joinlist) {
            if (j.lref.table == table || j.rref.table == table) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if field is a primary key of the specified table, false
     * otherwise
     *
     * @param tableAlias The alias of the table in the query
     * @param field      The pure name of the field
     */

    bool isPkey(const std::string& table_alias, const std::string& field) {
        int tid1 = logical_plan_->get_table_id(table_alias);
        std::string pkey1 = Database::get_catalog().get_primary_key(tid1);
        return pkey1 == field;
    }

    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    bool hasPkey(std::vector<JoinNode>& joinlist) {
        for (auto& j : joinlist) {
            if (isPkey(j.lref.table, j.lref.column) ||
                (j.rref.table != "" && isPkey(j.rref.table, j.rref.column))) {
                return true;
            }
        }
        return false;
    }
};