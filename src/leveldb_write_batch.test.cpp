module;

#include <doctest/doctest.h>

module leveldb.write_batch;

TEST_SUITE("[write-batch]")
{
    TEST_CASE("clear initializes header")
    {
        leveldb::WriteBatch wb;
        wb.clear();

        REQUIRE(wb.sequence() == 0);
        REQUIRE(wb.count() == 0);
        REQUIRE(wb.approximate_size() == 12);
        REQUIRE(wb.byte_size() == 12);
    }

    TEST_CASE("put and remove increase count")
    {
        leveldb::WriteBatch wb;
        wb.clear();

        wb.put("k1", "v1");
        REQUIRE(wb.count() == 1);
        REQUIRE(wb.approximate_size() > 12);

        wb.remove("k1");
        REQUIRE(wb.count() == 2);
    }

    TEST_CASE("append merges count and payload")
    {
        leveldb::WriteBatch lhs;
        leveldb::WriteBatch rhs;
        lhs.clear();
        rhs.clear();

        lhs.put("k1", "v1");
        rhs.put("k2", "v2");
        rhs.remove("k3");

        const auto lhs_size = lhs.approximate_size();
        const auto rhs_size = rhs.approximate_size();

        lhs.append(rhs);

        REQUIRE(lhs.count() == 3);
        REQUIRE(lhs.approximate_size() > lhs_size);
        REQUIRE(lhs.approximate_size() == lhs_size + (rhs_size - 12));
    }
}