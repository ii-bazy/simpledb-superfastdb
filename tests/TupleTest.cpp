#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest-spi.h"
#include "gtest/gtest.h"
#include "src/storage/IntField.hpp"
#include "src/storage/Tuple.hpp"
#include "tests/Utility.hpp"

TEST(TupleTest, ModifyFields) {
    auto td = utility::get_tuple_desc(2);
    Tuple tup(td);

    tup.set_field(0, std::make_shared<IntField>(-1));
    tup.set_field(1, std::make_shared<IntField>(2));

    EXPECT_EQ(IntField(-1), tup.get_field(0));
    EXPECT_EQ(IntField(2), tup.get_field(1));

    tup.set_field(0, std::make_shared<IntField>(1));
    tup.set_field(1, std::make_shared<IntField>(37));

    EXPECT_EQ(IntField(1), tup.get_field(0));
    EXPECT_EQ(IntField(37), tup.get_field(1));
}

TEST(TupleTest, GetTupleDesc) {
    auto td = utility::get_tuple_desc(5);
    Tuple tup(td);
    EXPECT_EQ(td, tup.get_tuple_desc());
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}