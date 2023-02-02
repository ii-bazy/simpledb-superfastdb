#pragma once

#include "absl/status/statusor.h"
#include <hsql/sql/SelectStatement.h>

#include "hsql/SQLParser.h"
#include "src/execution/OpType.hpp"
#include "src/execution/logical_plan/logical_plan.hpp"

class Parser {
   private:
    absl::StatusOr<LogicalPlan> ParseSelectStatement(
        const hsql::SelectStatement* stmt);

    absl::Status ParseFromClause(LogicalPlan& lp, const hsql::TableRef* from);

    absl::Status ParseSimpleExpression(LogicalPlan& lp, hsql::Expr* lexpr,
                                       OpType op, hsql::Expr* rexpr);

    absl::Status ParseWhereExpression(LogicalPlan& lp, hsql::Expr* expr);

    absl::Status ParseGroupBy(LogicalPlan& lp, hsql::GroupByDescription* descr);

    absl::Status ParseColumnSelection(LogicalPlan& lp,
                                      std::vector<hsql::Expr*>* columns);

    absl::Status ParseOrderBy(LogicalPlan& lp,
                              std::vector<hsql::OrderDescription*>* descr);

   public:
    absl::StatusOr<LogicalPlan> ParseQuery(std::string_view query);
};
