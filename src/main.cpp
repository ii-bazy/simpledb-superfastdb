// #include "src/storage/Field.hpp"
#include "src/common/Type.hpp"
#include "src/storage/TupleDesc.hpp"
#include "src/storage/Tuple.hpp"

#include <iostream>

int main() {
    const auto& type = Type::STRING_TYPE();

    std::cout << type->get_len() << "\n";

    return 0;
}

// bazel test //tests:TupleTest
// bazel build //src:main
// bazel run //src:main