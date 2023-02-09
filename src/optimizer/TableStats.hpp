#pragma once

#include <cmath>
#include <memory>
#include <vector>

#include "src/common/Database.hpp"
#include "src/optimizer/IntHistogram.hpp"
#include "src/optimizer/StringHistogram.hpp"
#include "src/storage/IntField.hpp"
#include "src/storage/StringField.hpp"

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 */
class TableStats {
   public:
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate
     * between sequential-scan IO and disk seeks.
     */
    TableStats() {
        throw std::invalid_argument("TableStats default constructor.");
    }

    TableStats(int table_id, int io_cost_per_page)
        : table_id_(table_id), io_cost_per_page_(io_cost_per_page) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // TODO: some code goes here

        calc_stats();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    double estimate_scan_cost() const {
        // TODO: some code goes here
        auto file = std::dynamic_pointer_cast<HeapFile>(
            Database::get_catalog().get_db_file(table_id_));
        if (file == nullptr) {
            return 0;
        }
        return file->num_pages() * io_cost_per_page_;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    int estimate_table_cardinality(double selectivity_factor) const {
        return selectivity_factor * total_tuples_;
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and
     * then given a tuple, of which we do not know the value of the field,
     * return the expected selectivity. You may estimate this value from the
     * histograms.
     */
    double avg_selectivity(int field, OpType op) const {
        if (int_histograms_[field] != nullptr) {
            return int_histograms_[field]->avg_selectivity(op);
        }
        if (string_histograms_[field] != nullptr) {
            return string_histograms_[field]->avg_selectivity(op);
        }
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    double estimate_selectivity(int field, OpType op, const Field* constant) const {
        if (constant->get_type() == Type::INT_TYPE()) {
            return int_histograms_[field]->estimate_selectivity(
                op, dynamic_cast<const IntField*>(constant)->get_value());
        } else if (constant->get_type() == Type::STRING_TYPE()) {
            return string_histograms_[field]->estimate_selectivity(
                op,
                std::string(
                    dynamic_cast<const StringField*>(constant)->get_value()));
        }
        return 0;
    }

    /**
     * return the total number of tuples in this table
     */
    int totalTuples() const { return total_tuples_; }

   private:
    void calc_stats() {
        auto it = Database::get_catalog().get_db_file(table_id_)->iterator();
        auto td = Database::get_catalog().get_tuple_desc(table_id_);
        min_field.resize(td->num_fields());
        max_field.resize(td->num_fields());
        int_histograms_.resize(td->num_fields());
        string_histograms_.resize(td->num_fields());
        values_.resize(td->num_fields()); // TODO: wywal

        TransactionId tid;
        it->rewind(tid);
        total_tuples_ = 0;
        while (it->has_next(tid)) {
            total_tuples_ += 1;
            auto tuple = it->next();
            // LOG(INFO) << tuple->to_string();
            for (int idx = 0; idx < td->num_fields(); ++idx) {
                if (auto field = std::dynamic_pointer_cast<IntField>(
                        tuple->get_field(idx));
                    field != nullptr) {
                    if (total_tuples_ == 1) {
                        max_field[idx] = min_field[idx] = field->get_value();
                    } else {
                        max_field[idx] =
                            std::max(max_field[idx], field->get_value());
                        min_field[idx] =
                            std::min(min_field[idx], field->get_value());
                    }
                }
            }
        }
        it->rewind(tid);
        while (it->has_next(tid)) {
            auto tuple = it->next();
            // LOG(INFO) << tuple->to_string();
            for (int idx = 0; idx < td->num_fields(); ++idx) {
                auto field = tuple->get_field(idx);
                if (field->get_type() == Type::INT_TYPE()) {
                    if (int_histograms_[idx] == nullptr) {
                        int_histograms_[idx] = std::make_unique<IntHistogram>(
                            kNumHistBins, min_field[idx], max_field[idx]);
                    }
                    int_histograms_[idx]->add_value(
                        std::dynamic_pointer_cast<IntField>(field)
                            ->get_value());
                    values_[idx].push_back(
                        std::dynamic_pointer_cast<IntField>(field)
                            ->get_value());
                } else if (field->get_type() == Type::STRING_TYPE()) {
                    if (string_histograms_[idx] == nullptr) {
                        string_histograms_[idx] =
                            std::make_unique<StringHistogram>(kNumHistBins);
                    }
                    string_histograms_[idx]->add_value(std::string(
                        std::dynamic_pointer_cast<StringField>(field)
                            ->get_value()));
                }
            }
        }

        // for (int idx = 0; idx < td->num_fields(); ++idx) {
        //     if (td->get_field_name(idx) != "age") {
        //         continue;
        //     }

        //     auto& v = values_[idx];
        //     std::sort(v.begin(), v.end());

        //     for (auto i : v) {
        //         std::cerr << i << " ";
        //     }
        //     std::cerr << "\n";

        //     exit(0);
        // }
    }

    const int kNumHistBins = 100;
    int table_id_;
    int io_cost_per_page_;
    int total_tuples_;
    std::vector<std::vector<int>> values_; // TODO: wywal
    std::vector<std::unique_ptr<IntHistogram>> int_histograms_;
    std::vector<std::unique_ptr<StringHistogram>> string_histograms_;
    std::vector<int> min_field;
    std::vector<int> max_field;
};
