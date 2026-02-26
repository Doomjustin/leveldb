module;

#include <doctest/doctest.h>

#include <cstdint>
#include <string>

module leveldb.status;

TEST_SUITE("[status]")
{
    auto decode_u32_le(const std::string& bytes) -> std::uint32_t
    {
        REQUIRE(bytes.size() >= 4);
        return static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[0])) |
               (static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[1])) << 8) |
               (static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[2])) << 16) |
               (static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[3])) << 24);
    }

    TEST_CASE("default status is OK")
    {
        auto s = leveldb::Status::OK();

        REQUIRE(s.ok());
        REQUIRE_FALSE(s.not_found());
        REQUIRE_FALSE(s.corruption());
        REQUIRE_FALSE(s.not_supported());
        REQUIRE_FALSE(s.invalid_argument());
        REQUIRE_FALSE(s.io_error());
        REQUIRE(s.message().empty());
    }

    TEST_CASE("factory message supports format placeholders")
    {
        auto s = leveldb::Status::NotFound("{} {}", "file", "missing");
        REQUIRE(s.message() == "file missing");
    }

    TEST_CASE("factory message keeps literal pattern when no placeholders")
    {
        auto s = leveldb::Status::NotFound("file ", "missing");
        REQUIRE(s.message() == "file ");
    }

    TEST_CASE("factory status predicates")
    {
        REQUIRE(leveldb::Status::NotFound("a").not_found());
        REQUIRE(leveldb::Status::Corruption("a").corruption());
        REQUIRE(leveldb::Status::NotSupported("a").not_supported());
        REQUIRE(leveldb::Status::InvalidArgument("a").invalid_argument());
        REQUIRE(leveldb::Status::IOError("a").io_error());
    }

    TEST_CASE("to_string stores message length and payload")
    {
        auto s = leveldb::Status::InvalidArgument("bad ", "input");
        const auto encoded = s.to_string();

        REQUIRE(encoded.size() == 4 + 1 + s.message().size());
        REQUIRE(decode_u32_le(encoded) == s.message().size());
        REQUIRE(encoded.substr(5) == s.message());
    }
}