module;

#include <doctest/doctest.h>

#include <functional>
#include <memory_resource>
#include <vector>

module leveldb.skip_list;

TEST_SUITE("[skip_list]")
{
    using skip_list_t = leveldb::SkipList<int>;

    TEST_CASE("contains returns true only for inserted keys")
    {
        std::pmr::unsynchronized_pool_resource memory_resource;
        skip_list_t skip_list{ &memory_resource };

        skip_list.insert(10);
        skip_list.insert(3);
        skip_list.insert(7);

        REQUIRE(skip_list.contains(3));
        REQUIRE(skip_list.contains(7));
        REQUIRE(skip_list.contains(10));

        REQUIRE_FALSE(skip_list.contains(2));
        REQUIRE_FALSE(skip_list.contains(8));
        REQUIRE_FALSE(skip_list.contains(11));
    }

    TEST_CASE("begin iteration yields sorted ascending order")
    {
        std::pmr::unsynchronized_pool_resource memory_resource;
        skip_list_t skip_list{ &memory_resource };

        for (int key : { 8, 1, 5, 3, 9, 2 })
            skip_list.insert(key);

        std::vector<int> actual;
        for (int iterator : skip_list)
            actual.push_back(iterator);

        const std::vector<int> expected{ 1, 2, 3, 5, 8, 9 };
        REQUIRE(actual == expected);
        REQUIRE(skip_list.front() == expected.front());
        REQUIRE(skip_list.back() == expected.back());
    }

    TEST_CASE("seek finds first greater-or-equal key")
    {
        std::pmr::unsynchronized_pool_resource memory_resource;
        skip_list_t skip_list{ std::less<int>{}, &memory_resource };

        for (int key : { 2, 4, 6, 8 })
            skip_list.insert(key);

        auto at_exact = skip_list.seek(4);
        REQUIRE(at_exact.valid());
        REQUIRE(at_exact.key() == 4);

        auto at_gap = skip_list.seek(5);
        REQUIRE(at_gap.valid());
        REQUIRE(*at_gap == 6);

        auto at_end = skip_list.seek(9);
        REQUIRE_FALSE(at_end.valid());
        REQUIRE(at_end == skip_list.end());
    }

    TEST_CASE("iterator prev from end moves to last element")
    {
        std::pmr::unsynchronized_pool_resource memory_resource;
        skip_list_t skip_list{ &memory_resource };

        for (int key : { 4, 1, 7 })
            skip_list.insert(key);

        auto iterator = skip_list.end();
        iterator.prev();

        REQUIRE(iterator.valid());
        REQUIRE(iterator.key() == 7);

        iterator.prev();
        REQUIRE(iterator.valid());
        REQUIRE(*iterator == 4);

        iterator.prev();
        REQUIRE(iterator.valid());
        REQUIRE(*iterator == 1);

        iterator.prev();
        REQUIRE_FALSE(iterator.valid());
        REQUIRE(iterator == skip_list.end());
    }

    TEST_CASE("iterator prev from end stays invalid for empty list")
    {
        std::pmr::unsynchronized_pool_resource memory_resource;
        skip_list_t skip_list{ &memory_resource };

        auto iterator = skip_list.end();
        iterator.prev();

        REQUIRE_FALSE(iterator.valid());
        REQUIRE(iterator == skip_list.end());
    }
}