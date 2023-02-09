#pragma once

#include <string>

class ScanNode {
   public:
    // Table id in the catalog.
    int id;
    // Table alias in the query.
    std::string alias;

    ScanNode(int id, std::string_view alias) : id(id), alias(alias) {}

    operator std::string() const { return std::to_string(id) + ", " + alias; }
};
