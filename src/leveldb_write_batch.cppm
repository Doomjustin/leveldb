module;

#include <cassert>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

export module leveldb.write_batch;

import leveldb.common;
import xin.base.coding;

using namespace xin::base;

namespace leveldb {

export class WriteBatch {
public:
    using sequence_number = SequenceNumber;
    using count_type = int;
    using size_type = std::size_t;

    void put(std::string_view key, std::string_view value)
    {
        count(count() + 1);
        rep_.push_back(std::to_underlying(ValueType::Value));
        add_record(key);
        add_record(value);
    }

    void remove(std::string_view key)
    {
        count(count() + 1);
        rep_.push_back(std::to_underlying(ValueType::Deletion));
        add_record(key);
    }

    void clear()
    {
        rep_.clear();
        rep_.resize(HEADER_SIZE);
    }

    void append(const WriteBatch& source)
    {
        assert(rep_.size() >= HEADER_SIZE);

        count(count() + source.count());
        rep_.append(source.rep_.substr(HEADER_SIZE));
    }

    [[nodiscard]]
    constexpr auto approximate_size() const noexcept -> size_type
    {
        return rep_.size();
    }

    [[nodiscard]]
    auto sequence() const noexcept -> sequence_number
    {
        std::istringstream ss{ rep_ };
        return fixed::decode<sequence_number>(ss);
    }

    [[nodiscard]]
    auto count() const noexcept -> count_type
    {
        std::istringstream ss{ rep_ };
        ss.seekg(SEQUENCE_NUMBER_SIZE);
        return fixed::decode<count_type>(ss);
    }

    [[nodiscard]]
    constexpr auto byte_size() const noexcept -> size_type
    {
        return rep_.size();
    }

private:
    static constexpr int SEQUENCE_NUMBER_SIZE = sizeof(sequence_number);
    static constexpr int COUNT_SIZE = sizeof(count_type);
    static constexpr int HEADER_SIZE = 12;

    std::string rep_;

    void count(count_type n)
    {
        assert(rep_.size() >= HEADER_SIZE);

        std::stringstream ss{};
        fixed::encode(ss, n);

        auto new_count = ss.str();
        assert(new_count.size() == COUNT_SIZE);

        rep_.replace(SEQUENCE_NUMBER_SIZE, COUNT_SIZE, new_count);
    }

    void content(std::string_view content)
    {
        assert(content.size() >= HEADER_SIZE);
        rep_.assign(content);
    }

    void add_record(std::string_view str)
    {
        std::ostringstream ss{};
        varint::encode(ss, str.size());
        rep_ += ss.str();
        rep_ += str;
    }
};

} // namespace leveldb