module;

#include <doctest/doctest.h>

#include <functional>
#include <limits>
#include <string>

module leveldb.memory_table;

TEST_SUITE("[memory_table]")
{
    using memory_table_t = leveldb::MemoryTable<>;

    TEST_CASE("get returns value for existing key")
    {
        memory_table_t memory_table;

        memory_table.add(1, "key1", "value1");
        memory_table.add(2, "key2", "value2");

        auto result1 = memory_table.get("key1", 1);
        REQUIRE(result1.has_value());
        REQUIRE(*result1 == "value1");

        auto result2 = memory_table.get("key2", 2);
        REQUIRE(result2.has_value());
        REQUIRE(*result2 == "value2");
    }

    TEST_CASE("get returns not found for non-existing key")
    {
        memory_table_t memory_table;

        auto result = memory_table.get("nonexistent", 1);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().not_found());
    }

    TEST_CASE("get should not match neighboring user key")
    {
        memory_table_t memory_table;

        memory_table.add(1, "a", "value-a");
        memory_table.add(1, "b", "value-b");

        auto result = memory_table.get("aa", 1);
        REQUIRE_FALSE(result.has_value());
        REQUIRE(result.error().not_found());
    }

    TEST_CASE("deletion marker should make key not found at newer sequence")
    {
        memory_table_t memory_table;

        memory_table.add(1, "k", "v1");
        memory_table.add(2, "k");

        auto deleted = memory_table.get("k", 2);
        REQUIRE_FALSE(deleted.has_value());
        REQUIRE(deleted.error().not_found());

        auto older = memory_table.get("k", 1);
        REQUIRE(older.has_value());
        REQUIRE(*older == "v1");
    }

    TEST_CASE("get returns latest visible version at requested sequence")
    {
        memory_table_t memory_table;

        memory_table.add(1, "k", "v1");
        memory_table.add(3, "k", "v3");

        auto at_newer = memory_table.get("k", 3);
        REQUIRE(at_newer.has_value());
        REQUIRE(*at_newer == "v3");

        auto at_middle = memory_table.get("k", 2);
        REQUIRE(at_middle.has_value());
        REQUIRE(*at_middle == "v1");
    }

    TEST_CASE("deletion without prior value remains not found")
    {
        memory_table_t memory_table;

        memory_table.add(1, "ghost");

        auto at_delete = memory_table.get("ghost", 1);
        REQUIRE_FALSE(at_delete.has_value());
        REQUIRE(at_delete.error().not_found());

        auto after_delete = memory_table.get("ghost", 100);
        REQUIRE_FALSE(after_delete.has_value());
        REQUIRE(after_delete.error().not_found());
    }

    TEST_CASE("mixed versions follow sequence visibility")
    {
        memory_table_t memory_table;

        memory_table.add(1, "k", "v1");
        memory_table.add(2, "k", "v2");
        memory_table.add(3, "k");
        memory_table.add(4, "k", "v4");

        auto at1 = memory_table.get("k", 1);
        REQUIRE(at1.has_value());
        REQUIRE(*at1 == "v1");

        auto at2 = memory_table.get("k", 2);
        REQUIRE(at2.has_value());
        REQUIRE(*at2 == "v2");

        auto at3 = memory_table.get("k", 3);
        REQUIRE_FALSE(at3.has_value());
        REQUIRE(at3.error().not_found());

        auto at4 = memory_table.get("k", 4);
        REQUIRE(at4.has_value());
        REQUIRE(*at4 == "v4");
    }

    TEST_CASE("sequence boundary values are handled")
    {
        memory_table_t memory_table;
        constexpr leveldb::SequenceNumber max_sequence = std::numeric_limits<leveldb::SequenceNumber>::max() >> 8;

        memory_table.add(0, "zero", "v0");
        memory_table.add(max_sequence, "max", "vmax");

        auto zero = memory_table.get("zero", 0);
        REQUIRE(zero.has_value());
        REQUIRE(*zero == "v0");

        auto max = memory_table.get("max", max_sequence);
        REQUIRE(max.has_value());
        REQUIRE(*max == "vmax");
    }

    TEST_CASE("custom comparator works for existing keys")
    {
        using reverse_memory_table_t = leveldb::MemoryTable<std::greater<std::string>>;
        reverse_memory_table_t memory_table;

        memory_table.add(1, "alpha", "a1");
        memory_table.add(1, "beta", "b1");

        auto alpha = memory_table.get("alpha", 1);
        REQUIRE(alpha.has_value());
        REQUIRE(*alpha == "a1");

        auto beta = memory_table.get("beta", 1);
        REQUIRE(beta.has_value());
        REQUIRE(*beta == "b1");
    }

    // 这种情况由上层调用保证不出现，所以不需要特别处理，但我们也测试一下，看看是否符合预期
    TEST_CASE("same key and same sequence uses deletion as winner")
    {
        memory_table_t memory_table;

        memory_table.add(7, "k", "v7");
        memory_table.add(7, "k");

        auto result = memory_table.get("k", 7);
        REQUIRE_FALSE(result.has_value());
        // 所以这里的false是符合预期的
        REQUIRE_FALSE(result.error().not_found());
    }
}