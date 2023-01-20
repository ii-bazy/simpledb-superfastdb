#pragma once

#include <string>

struct ColumnRef {
    // Table alias.
    std::string table;
    // Column name.
    std::string column;

    ColumnRef() : table(""), column("") {}

    ColumnRef(std::string_view table, std::string_view column)
        : table(table), column(column) {}

    bool IsValid() { return table != "" && column != ""; }

    bool IsStar() { return table == "null" && column == "*"; }

    operator std::string() const { return table + "." + column; }

    bool operator==(const ColumnRef& other) const {
        return table == other.table && column == other.column;
    }
    
    bool operator!=(const ColumnRef& other) const {
        return table != other.table || column != other.column;
    }
};

const ColumnRef COLUMN_REF_STAR("null", "*");
