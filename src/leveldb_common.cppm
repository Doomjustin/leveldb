module;

#include <cstdint>

export module leveldb.common;

export namespace leveldb {

using SequenceNumber = std::uint64_t;

enum class ValueType : std::uint8_t { Deletion = 0b0, Value = 0b1 };

} // namespace leveldb