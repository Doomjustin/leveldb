module;

#include <array>
#include <atomic>
#include <cassert>
#include <functional>
#include <memory_resource>
#include <ranges>

export module leveldb.skip_list;

import xin.base.random;

using namespace xin::base;

namespace leveldb {

export template<typename Key, typename Comparator = std::less<Key>, std::size_t MaxHeight = 12,
                float Probability = 0.25F>
class SkipList {
public:
    template<typename T>
    using container = std::pmr::vector<T>;

    template<typename T>
    using allocator_type = std::pmr::polymorphic_allocator<T>;

    using memory_resource = std::pmr::memory_resource;
    using key_type = Key;
    using comparator_type = Comparator;

private:
    class Node {
    public:
        Node(key_type key, std::size_t level, memory_resource* memory_resource)
            : key{ std::move(key) }
            , forward_{ memory_resource }
        {
            forward_.resize(level, nullptr);
        }

        void next(std::size_t level, Node* node)
        {
            auto& slot = forward_[level];
            std::atomic_ref<Node*>{ slot }.store(node, std::memory_order_release);
        }

        auto next(std::size_t level) const -> Node*
        {
            auto& slot = forward_[level];
            return std::atomic_ref<Node*>{ slot }.load(std::memory_order_acquire);
        }

        void no_barrier_next(std::size_t level, Node* node)
        {
            auto& slot = forward_[level];
            std::atomic_ref<Node*>{ slot }.store(node, std::memory_order_relaxed);
        }

        auto no_barrier_next(std::size_t level) const -> Node*
        {
            auto& slot = forward_[level];
            return std::atomic_ref<Node*>{ slot }.load(std::memory_order_relaxed);
        }

        key_type key;

    private:
        // atomic本身不可copy和move，但是vector的很多操作都要求克copy和move，所以只能用这种方式绕过去
        mutable container<Node*> forward_;
    };

public:
    explicit SkipList(memory_resource* memory_resource)
        : SkipList{ comparator_type{}, memory_resource }
    {
    }

    SkipList(comparator_type comparator, memory_resource* memory_resource)
        : comparator_{ comparator }
        , memory_resource_{ memory_resource }
    {
    }

    SkipList(const SkipList&) = delete;
    auto operator=(const SkipList&) -> SkipList& = delete;

    SkipList(SkipList&&) noexcept = delete;
    auto operator=(SkipList&&) noexcept -> SkipList& = delete;

    ~SkipList()
    {
        auto* node = head_;
        while (node) {
            auto* next = node->no_barrier_next(0);
            destroy_node(node);
            node = next;
        }

        head_ = nullptr;
    }

    void insert(key_type key)
    {
        std::array<Node*, MaxHeight> prev;
        auto* node = find_greater_or_equal(key, prev);

        assert(!node || !equal_to(key, node));

        auto height = random_height();
        if (height > current_height()) {
            for (auto i = current_height(); i < height; ++i)
                prev[i] = head_;

            current_height(height);
        }

        node = create_node(std::move(key), height);

        for (std::size_t i = 0; i < height; ++i) {
            node->no_barrier_next(i, prev[i]->no_barrier_next(i));
            prev[i]->next(i, node);
        }
    }

    auto contains(const key_type& key) const -> bool
    {
        auto* node = find_greater_or_equal(key);
        return equal_to(key, node);
    }

    [[nodiscard]]
    constexpr auto current_height() const noexcept -> std::size_t
    {
        return current_height_.load(std::memory_order_relaxed);
    }

    class Iterator {
    public:
        Iterator(Node* current, const SkipList* skip_list)
            : current_{ current }
            , skip_list_{ skip_list }
        {
        }

        [[nodiscard]]
        constexpr auto valid() const -> bool
        {
            return current_ != nullptr;
        }

        [[nodiscard]]
        auto key() const -> const key_type&
        {
            assert(valid());
            return current_->key;
        }

        void next()
        {
            assert(valid());
            current_ = current_->next(0);
        }

        void prev()
        {
            if (current_ == nullptr) {
                current_ = skip_list_->find_last();
                return;
            }

            current_ = skip_list_->find_less_than(current_->key);

            if (current_ == skip_list_->head_)
                current_ = nullptr;
        }

        auto operator==(const Iterator& other) const -> bool { return current_ == other.current_; }

        auto operator!=(const Iterator& other) const -> bool { return !(*this == other); }

        auto operator++() -> Iterator&
        {
            next();
            return *this;
        }

        auto operator++(int) -> Iterator
        {
            auto temp = *this;
            next();
            return temp;
        }

        auto operator*() const -> const key_type&
        {
            assert(valid());
            return current_->key;
        }

        auto operator->() const -> const key_type*
        {
            assert(valid());
            return &current_->key;
        }

    private:
        Node* current_;
        const SkipList* skip_list_;
    };

    auto seek(const key_type& key) const -> Iterator { return Iterator{ find_greater_or_equal(key), this }; }

    auto begin() -> Iterator { return Iterator{ head_->next(0), this }; }

    auto end() -> Iterator { return Iterator{ nullptr, this }; }

    auto begin() const -> Iterator { return Iterator{ head_->next(0), this }; }

    auto end() const -> Iterator { return Iterator{ nullptr, this }; }

    auto front() const -> const key_type&
    {
        assert(head_->next(0) != nullptr);
        return head_->next(0)->key;
    }

    auto back() const -> const key_type&
    {
        auto* last = find_last();
        assert(last != nullptr);
        return last->key;
    }

private:
    comparator_type comparator_;
    // 区别于原作的实现，我这里采用C++17的内存资源管理器来管理跳表节点的内存分配
    // 所以，我也不会在实现arena内存分配器了，直接使用std::pmr::memory_resource来分配跳表节点的内存。
    memory_resource* memory_resource_;
    // 跳表的头节点，头节点不存储任何有效数据
    Node* head_ = create_node(key_type{}, MaxHeight);
    std::atomic<std::size_t> current_height_{ 1 };

    auto create_node(key_type key, std::size_t level) -> Node*
    {
        allocator_type<Node> allocator{ memory_resource_ };
        auto* node = allocator.allocate(1);
        std::construct_at(node, std::move(key), level, memory_resource_);
        return node;
    }

    void destroy_node(Node* node)
    {
        allocator_type<Node> allocator{ memory_resource_ };
        std::destroy_at(node);
        allocator.deallocate(node, 1);
    }

    auto random_height() -> std::size_t
    {
        std::size_t height = 1;
        while (height < MaxHeight && random::bernoulli(Probability))
            ++height;

        return height;
    }

    template<typename Range>
        requires std::ranges::random_access_range<Range> && std::same_as<std::ranges::range_value_t<Range>, Node*>
    auto find_greater_or_equal(const key_type& key, Range& prev) const -> Node*
    {
        auto* node = head_;
        auto level = current_height() - 1;
        while (true) {
            auto* next = node->next(level);
            if (less_than(key, next)) {
                node = next;
            }
            else {
                assert(prev.size() >= level);
                prev[level] = node;

                if (level == 0)
                    return next;

                --level;
            }
        }
    }

    auto find_greater_or_equal(const key_type& key) const -> Node*
    {
        auto* node = head_;
        auto level = current_height() - 1;
        while (true) {
            auto* next = node->next(level);
            if (less_than(key, next)) {
                node = next;
            }
            else {
                if (level == 0)
                    return next;

                --level;
            }
        }
    }

    auto find_less_than(const key_type& key) const -> Node*
    {
        auto* node = head_;
        auto level = current_height() - 1;
        while (true) {
            auto* next = node->next(level);
            if (less_than(key, next)) {
                node = next;
            }
            else {
                if (level == 0)
                    return node;

                --level;
            }
        }
    }

    auto find_last() const -> Node*
    {
        auto* node = head_;
        auto level = current_height() - 1;

        while (true) {
            auto* next = node->next(level);
            if (next != nullptr) {
                node = next;
            }
            else {
                if (level == 0)
                    return node == head_ ? nullptr : node;

                --level;
            }
        }
    }

    void current_height(std::size_t new_height)
    {
        /*
        auto old_height = current_height();
        while (old_height < new_height &&
               !current_height_.compare_exchange_weak(old_height, new_height, std::memory_order_release,
                                                      std::memory_order_relaxed))
            ;
        */
        // 原作因为约定了多读单写的应用场景，所以没采用CAS的方式
        // 我们这里尊重原作，除非后期有特殊需求
        current_height_.store(new_height, std::memory_order_release);
    }

    auto less_than(const key_type& key, Node* node) const -> bool { return node && comparator_(node->key, key); }

    auto equal_to(const key_type& key, Node* node) const -> bool
    {
        return node && !comparator_(node->key, key) && !comparator_(key, node->key);
    }

    auto greater_or_equal(const key_type& key, Node* node) const -> bool
    {
        return node && !comparator_(key, node->key);
    }
};

} // namespace leveldb