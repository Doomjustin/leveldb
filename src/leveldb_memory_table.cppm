module;

#include <expected>
#include <memory_resource>
#include <sstream>
#include <string>
#include <string_view>

export module leveldb.memory_table;

export import leveldb.common;
export import leveldb.status;
import leveldb.skip_list;
import xin.base.coding;

using namespace xin::base;

namespace leveldb {

template<typename Comparator = std::less<std::string>>
class MemoryTable {
public:
    using sequence_number = SequenceNumber;
    using comparator_type = Comparator;
    using key_type = std::pmr::string;
    using memory_resource = std::pmr::monotonic_buffer_resource;

    MemoryTable(Comparator comparator = Comparator{})
        : comparator_{ std::move(comparator) }
    {
    }

    MemoryTable(const MemoryTable&) = delete;
    auto operator=(const MemoryTable&) -> MemoryTable& = delete;

    MemoryTable(MemoryTable&&) noexcept = delete;
    auto operator=(MemoryTable&&) noexcept -> MemoryTable& = delete;

    ~MemoryTable() = default;

    [[nodiscard]]
    auto get(std::string_view key, sequence_number seq) const -> std::expected<std::string, Status>
    {
        key_type lookup_key{ std::pmr::new_delete_resource() };
        std::stringstream ss{ lookup_key };

        // key_size + tag size
        varint::encode(ss, key.size() + 8);
        ss << key;
        // tag
        fixed::encode(ss, (seq << 8) | static_cast<std::uint64_t>(ValueType::Value));
        // value_size
        varint::encode(ss, 0);

        lookup_key = ss.str();

        auto iter = table_.seek(lookup_key);
        if (iter == table_.end())
            return std::unexpected(Status::NotFound("{}", key));

        auto [tag, user_key, value] = InternalKey::extract(*iter);

        auto type = static_cast<ValueType>(tag & 0xFF);

        if (user_key != key || type == ValueType::Deletion)
            return std::unexpected(Status::NotFound("{}", key));

        return value;
    }

    void add(sequence_number seq, std::string_view key) { add(seq, ValueType::Deletion, key); }

    void add(sequence_number seq, std::string_view key, std::string_view value)
    {
        add(seq, ValueType::Value, key, value);
    }

private:
    struct InternalKey {
        std::uint64_t tag;
        std::string user_key;
        std::string value;

        static auto extract(std::string_view internal_key) -> InternalKey
        {
            std::stringstream ss{ std::string{ internal_key } };

            auto key_size = varint::decode<std::size_t>(ss);
            InternalKey result{};
            result.user_key.resize(key_size - 8);
            ss.read(result.user_key.data(), static_cast<std::streamsize>(key_size - 8));

            result.tag = fixed::decode<std::uint64_t>(ss);

            auto value_size = varint::decode<std::size_t>(ss);
            result.value.resize(value_size);
            ss.read(result.value.data(), static_cast<std::streamsize>(value_size));

            return result;
        }
    };

    struct PackedKeyComparator {
        comparator_type user_key_comparator;

        auto operator()(std::string_view a, std::string_view b) const -> bool
        {
            auto [tag_a, user_key_a, value_a] = InternalKey::extract(a);
            auto [tag_b, user_key_b, value_b] = InternalKey::extract(b);

            // 和原作的语义不太一样，原作是相等性比较，返回值是int，负数表示a < b，0表示a == b，正数表示a > b
            // 而我这里的comparator是less比较，所以当a < b时返回true，当a >= b时返回false
            if (user_key_a != user_key_b)
                return user_key_comparator(user_key_a, user_key_b);

            return tag_a > tag_b;
        }
    };

    using table = SkipList<key_type, PackedKeyComparator>;

    Comparator comparator_;
    memory_resource memory_resource_;
    table table_{ &memory_resource_ };

    void add(sequence_number seq, ValueType type, std::string_view key, std::string_view value = {})
    {
        table_.insert(pack_key_value(seq, type, key, value));
    }

    auto pack_key_value(sequence_number seq, ValueType type, std::string_view key, std::string_view value = {})
        -> key_type
    {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  tag          : uint64((sequence << 8) | type)
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        key_type internal_key{ &memory_resource_ };
        std::stringstream ss{ internal_key };

        // key_size + tag size
        varint::encode(ss, key.size() + 8);
        ss << key;
        // tag
        fixed::encode(ss, (seq << 8) | static_cast<std::uint64_t>(type));
        // value_size
        varint::encode(ss, value.size());
        ss << value;

        // TODO: 考虑优化成不用流的方式，直接在key_type上构造出最终的internal_key
        internal_key = ss.str();
        return internal_key;
    }
};

} // namespace leveldb