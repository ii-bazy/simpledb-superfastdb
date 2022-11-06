#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest-spi.h"
#include "gtest/gtest.h"
#include "src/storage/TupleDesc.hpp"
#include "tests/Utility.hpp"

bool combined_string_arrays(const TupleDesc* td1, const TupleDesc* td2,
                            const TupleDesc* combined) {
    for (int i = 0; i < td1->num_fields(); ++i) {
        if (td1->get_field_name(i) != combined->get_field_name(i)) {
            return false;
        }
    }
    for (int i = 0; i < td2->num_fields(); ++i) {
        if (td2->get_field_name(i) !=
            combined->get_field_name(td1->num_fields() + i)) {
            return false;
        }
    }
    return true;
}

TEST(TupleDescTest, Combine) {
    const auto td1 = utility::get_tuple_desc(1, "td1");
    const auto td2 = utility::get_tuple_desc(2, "td2");

    // test td1.combine(td2)
    auto td3 = TupleDesc::merge(*td1, *td2);
    EXPECT_EQ(3, td3.num_fields());
    EXPECT_EQ(3 * Type::INT_TYPE()->get_len(), td3.get_size());
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(Type::INT_TYPE(), td3.get_field_type(i));
    }
    EXPECT_TRUE(combined_string_arrays(td1.get(), td2.get(), &td3));

    // test td2.combine(td1)
    td3 = TupleDesc::merge(*td2, *td1);
    EXPECT_EQ(3, td3.num_fields());
    EXPECT_EQ(3 * Type::INT_TYPE()->get_len(), td3.get_size());
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(Type::INT_TYPE(), td3.get_field_type(i));
    }
    EXPECT_TRUE(combined_string_arrays(td2.get(), td1.get(), &td3));

    // test td2.combine(td2)
    td3 = TupleDesc::merge(*td2, *td2);
    EXPECT_EQ(4, td3.num_fields());
    EXPECT_EQ(4 * Type::INT_TYPE()->get_len(), td3.get_size());
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(Type::INT_TYPE(), td3.get_field_type(i));
    }
    EXPECT_TRUE(combined_string_arrays(td2.get(), td2.get(), &td3));
}

TEST(TupleDescTest, GetType) {
    const std::vector<int> lengths = {1, 2, 1000};
    for (int len : lengths) {
        const auto td = utility::get_tuple_desc(len);
        for (int i = 0; i < len; ++i) {
            EXPECT_EQ(Type::INT_TYPE(), td->get_field_type(i));
        }
    }
}

TEST(TupleDescTest, NameToID) {
    const std::vector<int> lengths = {1, 2, 1000};
    const std::string prefix = "test";
    for (int len : lengths) {
        const auto td = utility::get_tuple_desc(len, prefix);
        for (int i = 0; i < len; ++i) {
            EXPECT_EQ(i, td->index_for_field_name(prefix + std::to_string(i)));
        }

        try {
            td->index_for_field_name("foo");
            FAIL() << "Expected std::invalid_argument";
        } catch (const std::invalid_argument& e) {
            EXPECT_EQ(e.what(), std::string("Field name not found."));
        } catch (const std::exception& e) {
            FAIL() << "Expected std::invalid_argument, got " << e.what();
        }

        const auto td2 = utility::get_tuple_desc(len);
        try {
            td2->index_for_field_name(prefix);
            FAIL() << "Expected std::invalid_argument";
        } catch (const std::exception& e) {
        }
    }
}

TEST(TupleDescTest, GetSize) {
    const std::vector<int> lengths = {1, 2, 1000};
    for (int len : lengths) {
        const auto td = utility::get_tuple_desc(len);
        EXPECT_EQ(len * Type::INT_TYPE()->get_len(), td->get_size());
    }
}

TEST(TupleDescTest, NumFields) {
    const std::vector<int> lengths = {1, 2, 1000};
    for (int len : lengths) {
        const auto td = utility::get_tuple_desc(len);
        EXPECT_EQ(len, td->num_fields());
    }
}

TEST(TupleDescTest, TestEquals) {
    {
        const auto td1 = TupleDesc({Type::INT_TYPE()});
        const auto td2 = TupleDesc({Type::INT_TYPE()});
        EXPECT_TRUE(td1.equals(td2));
        EXPECT_TRUE(td2.equals(td1));
    }
    {
        const auto td1 = TupleDesc({Type::INT_TYPE()});
        const auto td2 = TupleDesc({Type::STRING_TYPE()});
        EXPECT_FALSE(td1.equals(td2));
        EXPECT_FALSE(td2.equals(td1));
    }
    {
        const std::vector<const Type*> types = {Type::INT_TYPE(),
                                                Type::STRING_TYPE()};
        const auto td1 = TupleDesc(types);
        const auto td2 = TupleDesc(types);
        EXPECT_TRUE(td1.equals(td2));
        EXPECT_TRUE(td2.equals(td1));
    }
    {
        const std::vector<const Type*> types1 = {Type::INT_TYPE(),
                                                 Type::STRING_TYPE()};
        const std::vector<const Type*> types2 = {
            Type::INT_TYPE(), Type::STRING_TYPE(), Type::STRING_TYPE()};
        const auto td1 = TupleDesc(types1);
        const auto td2 = TupleDesc(types2);
        EXPECT_FALSE(td1.equals(td2));
        EXPECT_FALSE(td2.equals(td1));
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}