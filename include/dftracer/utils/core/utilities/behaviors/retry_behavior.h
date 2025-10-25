#ifndef DFTRACER_UTILS_UTILITIES_BEHAVIORS_RETRY_BEHAVIOR_H
#define DFTRACER_UTILS_UTILITIES_BEHAVIORS_RETRY_BEHAVIOR_H

#include <dftracer/utils/core/utilities/behaviors/behavior.h>

#include <chrono>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

namespace dftracer {
namespace utils {
namespace utilities {
namespace behaviors {

/**
 * @brief Exception thrown when max retry attempts are exceeded.
 */
class MaxRetriesExceeded : public std::runtime_error {
   private:
    std::size_t attempts_;
    std::exception_ptr original_exception_;

   public:
    /**
     * @brief Construct with attempt count and original exception.
     *
     * @param attempts Number of attempts made
     * @param original The original exception that caused failures
     */
    MaxRetriesExceeded(std::size_t attempts,
                       std::exception_ptr original = nullptr)
        : std::runtime_error("Max retry attempts (" + std::to_string(attempts) +
                             ") exceeded"),
          attempts_(attempts),
          original_exception_(original) {}

    /**
     * @brief Get number of attempts made.
     */
    std::size_t get_attempts() const { return attempts_; }

    /**
     * @brief Get the original exception that caused the failures.
     */
    std::exception_ptr get_original_exception() const {
        return original_exception_;
    }
};

/**
 * @brief Retry behavior with exponential backoff support.
 *
 * Implements retry logic with configurable:
 * - Maximum retry attempts
 * - Base delay between retries
 * - Optional exponential backoff
 *
 * The behavior intercepts errors in on_error and signals to the
 * executor whether to retry by returning nullopt (retry) or a
 * value (stop retrying with that value).
 *
 * @tparam I Input type
 * @tparam O Output type
 */
template <typename I, typename O>
class RetryBehavior : public UtilityBehavior<I, O> {
   private:
    std::size_t max_retries_;
    std::chrono::milliseconds base_delay_;
    bool exponential_backoff_;

    /**
     * @brief Calculate delay for given attempt.
     *
     * @param attempt Current attempt number (0-indexed)
     * @return Delay duration to wait before retry
     */
    std::chrono::milliseconds calculate_delay(std::size_t attempt) const {
        if (!exponential_backoff_) {
            return base_delay_;
        }

        // Exponential backoff: delay * 2^attempt
        // Cap at reasonable maximum to avoid overflow
        std::size_t multiplier = 1ULL << std::min(attempt, std::size_t(10));
        return base_delay_ * multiplier;
    }

   public:
    /**
     * @brief Construct retry behavior.
     *
     * @param max_retries Maximum number of retry attempts
     * @param base_delay Base delay between retries
     * @param exponential_backoff Whether to use exponential backoff
     */
    RetryBehavior(std::size_t max_retries, std::chrono::milliseconds base_delay,
                  bool exponential_backoff = true)
        : max_retries_(max_retries),
          base_delay_(base_delay),
          exponential_backoff_(exponential_backoff) {}

    /**
     * @brief Hook before processing (no-op for retry).
     */
    void before_process([[maybe_unused]] const I& input) override {
        // No action needed before processing
    }

    /**
     * @brief Hook after processing (no-op for retry).
     */
    O after_process([[maybe_unused]] const I& input, O result) override {
        // No modification to successful results
        return result;
    }

    /**
     * @brief Handle error and determine whether to retry.
     *
     * If we haven't exceeded max_retries, wait for calculated delay
     * and return Retry action. Otherwise, return Rethrow action with
     * MaxRetriesExceeded exception.
     *
     * @param input The input that caused the error
     * @param e The exception that occurred
     * @param attempt Current attempt number (0-indexed)
     * @return BehaviorErrorResult with Retry or Rethrow action
     */
    std::variant<BehaviorErrorResult, std::optional<O>> on_error(
        [[maybe_unused]] const I& input,
        [[maybe_unused]] const std::exception& e,
        std::size_t attempt) override {
        // If we've exceeded max retries, return rethrow with wrapped exception
        if (attempt >= max_retries_) {
            try {
                throw MaxRetriesExceeded(attempt + 1, std::current_exception());
            } catch (...) {
                return BehaviorErrorResult::rethrow(std::current_exception());
            }
        }

        // Wait before retry
        auto delay = calculate_delay(attempt);
        std::this_thread::sleep_for(delay);

        // Return retry action
        return BehaviorErrorResult::retry();
    }

    /**
     * @brief Get maximum retry attempts.
     */
    std::size_t get_max_retries() const { return max_retries_; }

    /**
     * @brief Get base delay.
     */
    std::chrono::milliseconds get_base_delay() const { return base_delay_; }

    /**
     * @brief Check if exponential backoff is enabled.
     */
    bool is_exponential_backoff() const { return exponential_backoff_; }
};

}  // namespace behaviors
}  // namespace utilities
}  // namespace utils
}  // namespace dftracer

#endif  // DFTRACER_UTILS_UTILITIES_BEHAVIORS_RETRY_BEHAVIOR_H
