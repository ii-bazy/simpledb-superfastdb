#pragma once

#include "src/execution/logical_plan/logical_plan.hpp"
#include "src/execution/JoinPredicate.hpp"
#include "src/execution/Join.hpp"
#include "src/optimizer/CostCard.hpp"
#include "src/optimizer/PlanCache.hpp"
#include "src/optimizer/TableStats.hpp"

class JoinOptimizer {
public:
    JoinOptimizer(std::unique_ptr<LogicalPlan> logical_plan, const std::vector<JoinNode> joins)
        : logical_plan_ {std::move(logical_plan)}, joins_ {joins} {}

    static std::unique_ptr<OpIterator> instantiate_join(const JoinNode lj,
                                      std::unique_ptr<OpIterator> plan1,
                                      std::unique_ptr<OpIterator> plan2) {

        int t1id = 0, t2id = 0;
        std::unique_ptr<OpIterator> j = nullptr;

        try {
            t1id = plan1->get_tuple_desc()->index_for_field_name(lj.lref.column);
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
                throw std::invalid_argument("Unknown field instantiatejoin ljf2");
            }
        }

        JoinPredicate p = JoinPredicate(t1id, lj.op, t2id);

        j = std::make_unique<Join>(p, std::move(plan1), std::move(plan2));

        return j;
    }

    double estimate_join_cost(const JoinNode j, int card1, int card2,
                                   double cost1, double cost2) {
        if (j.subplan != nullptr) {
            return card1 + cost1 + cost2;
        } else {
            // Currently supports only nested loops
            return cost1 + cost2 * card1;
        }
    }

    int estimate_join_cardinality(const JoinNode j, int card1, int card2,
                                  boolean t1pkey, boolean t2pkey, std::map<String, TableStats> stats) {
        if (j.subplan != nullptr) {
            return card1;
        } else {
            return estimate_table_join_cardinality(j.op, j.lref.table, j.rref.table,
                    j.lref.column, j.rref.column, card1, card2, t1pkey, t2pkey,
                    stats, logical_plan_->get_table_alias_to_id_mapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     */
    public static int estimate_table_join_cardinality(OpType joinOp,
                                                   std::string table1Alias, std::string table2Alias, std::string field1PureName,
                                                   std::string field2PureName, int card1, int card2, bool t1pkey,
                                                   bool t2pkey, std::map<std::string, TableStats> stats,
                                                   absl::flat_hash_map<std::string, int> tableAliasToId) {
        int card = 1;
        // TODO: some code goes here
        return card <= 0 ? 1 : card;
    }

private:
    std::unique_ptr<LogicalPlan> logical_plan_;
    std::vector<JoinNode> joins_;

    /**
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     *
     * @param stats               table stats for all of the tables, referenced by table names
     *                            rather than alias (see {@link #orderJoins})
     * @param filterSelectivities the selectivities of the filters over each of the tables
     *                            (where tables are indentified by their alias or name if no
     *                            alias is given)
     * @param joinToRemove        the join to remove from joinSet
     * @param joinSet             the set of joins being considered
     * @param bestCostSoFar       the best way to join joinSet so far (minimum of previous
     *                            invocations of computeCostAndCardOfSubplan for this joinSet,
     *                            from returned CostCard)
     * @param pc                  the PlanCache for this join; should have subplans for all
     *                            plans of size joinSet.size()-1
     * @return A {@link CostCard} objects desribing the cost, cardinality,
     *         optimal subplan
     * @throws ParsingException when stats, filterSelectivities, or pc object is missing
     *                          tables involved in join
     */
    CostCard compute_cost_and_card_of_subplan(
            std::map<std::string, TableStats> stats,
            std::map<std::string, double> filter_selectivities,
            JoinNode joinToRemove, std::set<JoinNode> joinSet,
            double bestCostSoFar, PlanCache pc) {

        JoinNode j = joinToRemove;

        std::vector<JoinNode> prevBest;

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

        std::set<JoinNode> news = joinSet;
        news.erase(news.find(joinToRemove));

        double t1cost, t2cost;
        int t1card, t2card;
        bool leftPkey, rightPkey;

        if (news.empty()) { // base case -- both are base relations
            t1cost = stats[table1Name].estimate_scan_cost();
            t1card = stats[table1Name].estimate_table_cardinality(
                filter_selectivities[j.lref.column]
            );
            
            leftPkey = isPkey(j.lref.table, j.lref.column);


            // TODO: CO?
            // t2cost = table2Alias == null ? 0 : stats.get(table2Name)
            //         .estimateScanCost();
            // t2card = table2Alias == null ? 0 : stats.get(table2Name)
            //         .estimateTableCardinality(
            //                 filterSelectivities.get(j.t2Alias));
            // rightPkey = table2Alias != null && isPkey(table2Alias,
            //         j.f2PureName);
            t2cost = stats[table2Name].estimate_scan_cost();
            t2card = stats[table2Name].estimate_table_cardinality(
                filter_selectivities[j.rref.table]
            );

            rightPkey = isPkey(table2Alias, j.rref.column);
        } else {
            // news is not empty -- figure best way to join j to news
            prevBest = pc.getOrder(news);

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest.empty()) {
                // TODO: co tu????
                return null;
            }

            double prevBestCost = pc.getCost(news);
            int bestCard = pc.getCard(news);

            // estimate cost of right subtree
            if (doesJoin(prevBest, table1Alias)) { // j.t1 is in prevBest
                t1cost = prevBestCost; // left side just has cost of whatever
                // left
                // subtree is
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateTableCardinality(
                                filterSelectivities.get(j.t2Alias));
                rightPkey = j.t2Alias != null && isPkey(j.t2Alias,
                        j.f2PureName);
            } else if (doesJoin(prevBest, j.t2Alias)) { // j.t2 is in prevbest
                // (both
                // shouldn't be)
                t2cost = prevBestCost; // left side just has cost of whatever
                // left
                // subtree is
                t2card = bestCard;
                rightPkey = hasPkey(prevBest);
                t1cost = stats.get(table1Name).estimateScanCost();
                t1card = stats.get(table1Name).estimateTableCardinality(
                        filterSelectivities.get(j.t1Alias));
                leftPkey = isPkey(j.t1Alias, j.f1PureName);

            } else {
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return null;
            }
        }

        // case where prevbest is left
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar)
            return null;

        CostCard cc = new CostCard();

        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey,
                rightPkey, stats);
        cc.cost = cost1;
        cc.plan = new ArrayList<>(prevBest);
        cc.plan.add(j); // prevbest is left -- add new join to end
        return cc;
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    private boolean doesJoin(List<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table)
                    || (j.t2Alias != null && j.t2Alias.equals(table)))
                return true;
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
    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    private boolean hasPkey(List<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName)
                    || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName)))
                return true;
        }
        return false;

    }

};