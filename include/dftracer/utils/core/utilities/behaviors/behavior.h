#ifndef DFTRACER_UTILS_CORE_UTILITIES_BEHAVIORS_BEHAVIOR_H
#define DFTRACER_UTILS_CORE_UTILITIES_BEHAVIORS_BEHAVIOR_H

#include <dftracer/utils/core/utilities/behaviors/behavior_error_result.h>

#include <exception>
#include <optional>
#include <variant>

namespace dftracer::utils::utilities::behaviors {

/**
 * @brief Base interface for utility behaviors.
 *
 * Behaviors are composable hooks that can be applied to utilities to add
 * cross-cutting concerns like caching, retry logic, monitoring, etc.
 *
 * Each behavior can intercept three points in utility execution:
 * 1. before_process() - Called before utility.process()
 * 2. after_process() - Called after utility.process(), can transform result
 * 3. on_error() - Called when exception occurs, can handle or retry
 *
 * Behaviors are meant to be single-purpose.
 *
 * @tparam I Input type for the utility
 * @tparam O Output type for the utility
 */
template <typename I, typename O>
class UtilityBehavior {
   public:
    virtual ~UtilityBehavior() = default;

    /**
     * @brief Hook called before utility.process().
     *
     * Use this for:
     * - Pre-validation
     * - Logging start time
     * - Rate limiting
     * - Pre-processing
     *
     * @param input Input that will be passed to utility
     */
    virtual void before_process([[maybe_unused]] const I& input) {}

    /**
     * @brief Hook called after utility.process() succeeds.
     *
     * Use this for:
     * - Result transformation
     * - Caching results
     * - Logging completion
     * - Post-processing
     *
     * @param input Input that was passed to utility
     * @param result Result from utility.process()
     * @return Potentially transformed result
     */
    virtual O after_process([[maybe_unused]] const I& input, O result) {
        return result;
    }

    /**
     * @brief Hook called when utility.process() throws exception.
     *
     * Can return one of three results:
     * - BehaviorErrorResult: Explicit retry or rethrow action
     * - std::optional<O> with value: Recovery value to use instead
     * - std::nullopt: Pass through to next behavior in chain
     *
     * Use this for:
     * - Error logging (return nullopt to pass through)
     * - Retry logic (return BehaviorErrorResult::retry() or ::rethrow())
     * - Fallback values (return std::optional<O> with value)
     * - Error recovery (return recovery value)
     *
     * @param input Input that was passed to utility
     * @param e Exception that was thrown
     * @param attempt Current attempt number (0-indexed)
     * @return BehaviorErrorResult, recovery value, or nullopt
     */
    virtual std::variant<BehaviorErrorResult, std::optional<O>> on_error(
        [[maybe_unused]] const I& input,
        [[maybe_unused]] const std::exception& e,
        [[maybe_unused]] std::size_t attempt) {
        return BehaviorErrorResult::rethrow();  // Default: rethrow
    }
};

}  // namespace dftracer::utils::utilities::behaviors

#endif  // DFTRACER_UTILS_CORE_UTILITIES_BEHAVIORS_BEHAVIOR_H
