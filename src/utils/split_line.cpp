#include "src/utils/split_line.hpp"

#include <sstream>
#include <utility>

std::vector<std::string> split_line(std::string line, char delimeter) {
    std::stringstream ss(line);
    std::vector<std::string> res;
    while (ss.good()) {
        std::string sub;
        std::getline(ss, sub, delimeter);
        res.push_back(std::move(sub));
    }
    return res;
}
