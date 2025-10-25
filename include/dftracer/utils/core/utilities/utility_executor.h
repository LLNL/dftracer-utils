#ifndef DFTRACER_UTILS_UTILITIES_BEHAVIORS_UTILITY_EXECUTOR_H
#define DFTRACER_UTILS_UTILITIES_BEHAVIORS_UTILITY_EXECUTOR_H

#include <dftracer/utils/core/utilities/behaviors/behavior_chain.h>
#include <dftracer/utils/core/utilities/utility.h>
#include <dftracer/utils/core/utilities/utility_traits.h>

#include <exception>
#include <memory>
#include <optional>

namespace dftracer {
namespace utils {
namespace utilities {
namespace behaviors {

/**
 * @brief Executes a utility with behavior chain wrapping.
 *
 * UtilityExecutor orchestrates the execution of a utility by:
 * 1. Running before_process hooks on all behaviors
 * 2. Executing the utility's process() method
 * 3. Running after_process hooks on all behaviors
 * 4. Handling errors through behavior on_error hooks
 *
 * This class bridges utilities and behaviors, providing a unified
 * execution path regardless of which process() overload the utility
 * implements.
 *
 * @tparam I Input type
 * @tparam O Output type
 * @tparam Tags Variadic tag types
 */
template <typename I, typename O, typename... Tags>
class UtilityExecutor {
   private:
    std::shared_ptr<Utility<I, O, Tags...>> utility_;
    BehaviorChain<I, O> behavior_chain_;

   public:
    /**
     * @brief Construct executor with utility and behavior chain.
     *
     * @param utility The utility to execute
     * @param chain Behavior chain to wrap execution
     */
    UtilityExecutor(std::shared_ptr<Utility<I, O, Tags...>> utility,
                    BehaviorChain<I, O> chain)
        : utility_(utility), behavior_chain_(std::move(chain)) {}

    /**
     * @brief Execute utility without context.
     *
     * @param input Input to process
     * @return Output result
     * @throws Any exception from utility or behaviors
     */
    O execute(const I& input) {
        std::size_t attempt = 0;

        while (true) {
            try {
                // Run before hooks
                behavior_chain_.before_process(input);

                // Execute utility (context not available in this path)
                O result = utility_->process(input);

                // Run after hooks
                result =
                    behavior_chain_.after_process(input, std::move(result));

                return result;

            } catch (const std::exception& e) {
                // Run error hooks - behaviors decide how to handle
                auto result = behavior_chain_.on_error(input, e, attempt);

                // Check if it's an error action (retry/rethrow)
                if (std::holds_alternative<BehaviorErrorResult>(result)) {
                    auto& error_result = std::get<BehaviorErrorResult>(result);

                    if (error_result.action == BehaviorErrorAction::Retry) {
                        // Retry requested
                        ++attempt;
                        continue;
                    } else {
                        // Rethrow requested
                        if (error_result.exception) {
                            std::rethrow_exception(error_result.exception);
                        } else {
                            throw;
                        }
                    }
                }

                // Check if it's a recovery value
                auto& optional_result = std::get<std::optional<O>>(result);
                if (optional_result.has_value()) {
                    return optional_result.value();
                }

                // Should not reach here (chain returns rethrow by default)
                throw;
            }
        }
    }

    /**
     * @brief Execute utility with context.
     *
     * Sets context reference before calling process(), then clears it after.
     *
     * @param input Input to process
     * @param ctx Task context for dynamic task emission
     * @return Output result
     * @throws Any exception from utility or behaviors
     */
    O execute_with_context(const I& input, TaskContext& ctx) {
        std::size_t attempt = 0;

        while (true) {
            try {
                behavior_chain_.before_process(input);

                // Set context reference before calling process()
                utility_->set_context(ctx);

                O result = utility_->process(input);

                // Clear context after process completes
                utility_->clear_context();

                result =
                    behavior_chain_.after_process(input, std::move(result));

                return result;

            } catch (const std::exception& e) {
                // Clear context on error
                utility_->clear_context();

                // Run error hooks - behaviors decide how to handle
                auto result = behavior_chain_.on_error(input, e, attempt);

                // Check if it's an error action (retry/rethrow)
                if (std::holds_alternative<BehaviorErrorResult>(result)) {
                    auto& error_result = std::get<BehaviorErrorResult>(result);

                    if (error_result.action == BehaviorErrorAction::Retry) {
                        // Retry requested
                        ++attempt;
                        continue;
                    } else {
                        // Rethrow requested
                        if (error_result.exception) {
                            std::rethrow_exception(error_result.exception);
                        } else {
                            throw;
                        }
                    }
                }

                // Check if it's a recovery value
                auto& optional_result = std::get<std::optional<O>>(result);
                if (optional_result.has_value()) {
                    return optional_result.value();
                }

                // Should not reach here (chain returns rethrow by default)
                throw;
            }
        }
    }

    /**
     * @brief Get reference to underlying utility.
     */
    std::shared_ptr<Utility<I, O, Tags...>> get_utility() const {
        return utility_;
    }

    /**
     * @brief Get reference to behavior chain.
     */
    BehaviorChain<I, O>& get_behavior_chain() { return behavior_chain_; }

    /**
     * @brief Get const reference to behavior chain.
     */
    const BehaviorChain<I, O>& get_behavior_chain() const {
        return behavior_chain_;
    }
};

}  // namespace behaviors
}  // namespace utilities
}  // namespace utils
}  // namespace dftracer

#endif  // DFTRACER_UTILS_UTILITIES_BEHAVIORS_UTILITY_EXECUTOR_H
