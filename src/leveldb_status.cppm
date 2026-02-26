module;

#include <cassert>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

export module leveldb.status;

import xin.base.coding;
import xin.base.formats;

using namespace xin::base;

namespace leveldb {

export class Status {
public:
    Status() = default;

    [[nodiscard]]
    auto message() const noexcept -> std::string_view
    {
        return message_;
    }

    // state_[0...3] = length of message
    // state_[4] = code of this status
    // state_[5...] = message
    auto to_string() const -> std::string
    {
        std::stringstream ss{};
        fixed::encode(ss, static_cast<std::uint32_t>(message_.size()));
        ss.put(static_cast<char>(code_));
        ss << message_;

        return ss.str();
    }

    [[nodiscard]]
    constexpr auto ok() const noexcept -> bool
    {
        return code_ == Code::OK;
    }

    [[nodiscard]]
    constexpr auto not_found() const noexcept -> bool
    {
        return code_ == Code::NotFound;
    }

    [[nodiscard]]
    constexpr auto corruption() const noexcept -> bool
    {
        return code_ == Code::Corruption;
    }

    [[nodiscard]]
    constexpr auto not_supported() const noexcept -> bool
    {
        return code_ == Code::NotSupported;
    }

    [[nodiscard]]
    constexpr auto invalid_argument() const noexcept -> bool
    {
        return code_ == Code::InvalidArgument;
    }

    [[nodiscard]]
    constexpr auto io_error() const noexcept -> bool
    {
        return code_ == Code::IOError;
    }

    static auto OK() noexcept -> Status { return Status{}; }

    template<typename... Args>
    static auto NotFound(std::string_view message, Args&&... args) -> Status
    {
        return Status{ Code::NotFound, xin::base::format(message, std::forward<Args>(args)...) };
    }

    template<typename... Args>
    static auto Corruption(std::string_view message, Args&&... args) -> Status
    {
        return Status{ Code::Corruption, xin::base::format(message, std::forward<Args>(args)...) };
    }

    template<typename... Args>
    static auto NotSupported(std::string_view message, Args&&... args) -> Status
    {
        return Status{ Code::NotSupported, xin::base::format(message, std::forward<Args>(args)...) };
    }

    template<typename... Args>
    static auto InvalidArgument(std::string_view message, Args&&... args) -> Status
    {
        return Status{ Code::InvalidArgument, xin::base::format(message, std::forward<Args>(args)...) };
    }

    template<typename... Args>
    static auto IOError(std::string_view message, Args&&... args) -> Status
    {
        return Status{ Code::IOError, xin::base::format(message, std::forward<Args>(args)...) };
    }

private:
    enum class Code : char { OK = 1, NotFound = 2, Corruption = 3, NotSupported = 4, InvalidArgument = 5, IOError = 6 };

    Code code_ = Code::OK;
    std::string message_;

    Status(Code code, std::string_view message)
        : code_{ code }
        , message_{ message }
    {
    }
};

} // namespace leveldb