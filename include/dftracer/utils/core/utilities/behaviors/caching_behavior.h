#ifndef DFTRACER_UTILS_UTILITIES_BEHAVIORS_CACHING_BEHAVIOR_H
#define DFTRACER_UTILS_UTILITIES_BEHAVIORS_CACHING_BEHAVIOR_H

#include <dftracer/utils/core/utilities/behaviors/behavior.h>

#include <chrono>
#include <list>
#include <optional>
#include <unordered_map>

namespace dftracer {
namespace utils {
namespace utilities {
namespace behaviors {

/**
 * @brief LRU cache entry with TTL support.
 *
 * @tparam I Input type (cache key)
 * @tparam O Output type (cache value)
 */
template <typename I, typename O>
struct CacheEntry {
    O value;
    std::chrono::steady_clock::time_point insertion_time;
    typename std::list<I>::iterator lru_iterator;
};

/**
 * @brief Caching behavior with LRU eviction and TTL support.
 *
 * Implements caching using:
 * - LRU (Least Recently Used) eviction when cache is full
 * - TTL (Time To Live) to invalidate stale entries
 * - Optional hash function for custom input types
 *
 * The cache intercepts utility execution in after_process to store
 * results, and can return cached results on future calls.
 *
 * @tparam I Input type (must be hashable or provide custom hash)
 * @tparam O Output type (must be copyable)
 */
template <typename I, typename O>
class CachingBehavior : public UtilityBehavior<I, O> {
   private:
    std::size_t max_cache_size_;
    std::chrono::seconds ttl_;
    bool use_lru_;

    // LRU list: most recently used at front
    std::list<I> lru_list_;

    // Cache storage: input -> entry
    std::unordered_map<I, CacheEntry<I, O>> cache_;

    /**
     * @brief Check if cache entry is expired.
     */
    bool is_expired(const CacheEntry<I, O>& entry) const {
        auto now = std::chrono::steady_clock::now();
        auto age = std::chrono::duration_cast<std::chrono::seconds>(
            now - entry.insertion_time);
        return age >= ttl_;
    }

    /**
     * @brief Evict least recently used item.
     */
    void evict_lru() {
        if (lru_list_.empty()) return;

        // LRU item is at back of list
        const I& key = lru_list_.back();
        cache_.erase(key);
        lru_list_.pop_back();
    }

    /**
     * @brief Update LRU position for key.
     */
    void touch(const I& key, CacheEntry<I, O>& entry) {
        if (!use_lru_) return;

        // Move to front (most recently used)
        lru_list_.erase(entry.lru_iterator);
        lru_list_.push_front(key);
        entry.lru_iterator = lru_list_.begin();
    }

   public:
    /**
     * @brief Construct caching behavior.
     *
     * @param max_size Maximum number of entries to cache
     * @param ttl Time-to-live for cache entries
     * @param use_lru Whether to use LRU eviction
     */
    CachingBehavior(std::size_t max_size, std::chrono::seconds ttl,
                    bool use_lru = true)
        : max_cache_size_(max_size), ttl_(ttl), use_lru_(use_lru) {}

    /**
     * @brief Check cache before processing.
     *
     * Currently just a hook point. Cache lookup happens in after_process
     * to avoid duplicating the lookup logic.
     */
    void before_process([[maybe_unused]] const I& input) override {
        // Hook point for future extensions (e.g., cache warming)
    }

    /**
     * @brief Cache the result after processing.
     *
     * Stores result in cache with current timestamp. Handles:
     * - Cache eviction when full
     * - LRU list updates
     * - Entry expiration
     *
     * @param input The input key
     * @param result The output to cache
     * @return The unmodified result
     */
    O after_process(const I& input, O result) override {
        // Check if already in cache
        auto it = cache_.find(input);
        if (it != cache_.end()) {
            // Update existing entry
            if (is_expired(it->second)) {
                // Remove expired entry
                if (use_lru_) {
                    lru_list_.erase(it->second.lru_iterator);
                }
                cache_.erase(it);
            } else {
                // Update value and touch LRU
                it->second.value = result;
                it->second.insertion_time = std::chrono::steady_clock::now();
                touch(input, it->second);
                return result;
            }
        }

        // Need to insert new entry
        if (cache_.size() >= max_cache_size_) {
            evict_lru();
        }

        // Create new entry
        CacheEntry<I, O> entry;
        entry.value = result;
        entry.insertion_time = std::chrono::steady_clock::now();

        if (use_lru_) {
            lru_list_.push_front(input);
            entry.lru_iterator = lru_list_.begin();
        }

        cache_[input] = std::move(entry);

        return result;
    }

    /**
     * @brief Attempt to retrieve from cache on error.
     *
     * If the utility fails but we have a (possibly stale) cached value,
     * return it as a fallback recovery value. Otherwise, pass through
     * to let retry behavior handle it.
     *
     * @param input The input key
     * @param e The exception that occurred
     * @param attempt Current retry attempt
     * @return Cached value if available, nullopt to pass through
     */
    std::variant<BehaviorErrorResult, std::optional<O>> on_error(
        const I& input, [[maybe_unused]] const std::exception& e,
        [[maybe_unused]] std::size_t attempt) override {
        auto it = cache_.find(input);
        if (it != cache_.end()) {
            // Return cached value even if expired (degraded mode)
            touch(input, it->second);
            return std::optional<O>(it->second.value);
        }
        // No cached value, pass through to next behavior
        return std::optional<O>(std::nullopt);
    }

    /**
     * @brief Get cached value if available and not expired.
     *
     * @param input The input key
     * @return Cached output if available, nullopt otherwise
     */
    std::optional<O> get(const I& input) {
        auto it = cache_.find(input);
        if (it != cache_.end() && !is_expired(it->second)) {
            touch(input, it->second);
            return it->second.value;
        }
        return std::nullopt;
    }

    /**
     * @brief Clear all cached entries.
     */
    void clear() {
        cache_.clear();
        lru_list_.clear();
    }

    /**
     * @brief Get current cache size.
     */
    std::size_t size() const { return cache_.size(); }
};

}  // namespace behaviors
}  // namespace utilities
}  // namespace utils
}  // namespace dftracer

#endif  // DFTRACER_UTILS_UTILITIES_BEHAVIORS_CACHING_BEHAVIOR_H
