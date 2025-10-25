#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_BATCH_PROCESSOR_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_BATCH_PROCESSOR_H

#include <dftracer/utils/core/tasks/task_context.h>
#include <dftracer/utils/core/tasks/task_result.h>
#include <dftracer/utils/core/tasks/task_tag.h>
#include <dftracer/utils/core/utilities/utilities.h>

#include <algorithm>
#include <functional>
#include <optional>
#include <vector>

namespace dftracer::utils::components::workflows {

/**
 * @brief Generic batch processor that processes a list of items in parallel.
 *
 * @tparam ItemInput Type of input items
 * @tparam ItemOutput Type of output from processing each item
 */
template <typename ItemInput, typename ItemOutput>
class BatchProcessor
    : public utilities::Utility<std::vector<ItemInput>, std::vector<ItemOutput>,
                                utilities::tags::NeedsContext> {
   public:
    using ItemProcessorFn =
        std::function<ItemOutput(const ItemInput&, TaskContext&)>;
    using ComparatorFn =
        std::function<bool(const ItemOutput&, const ItemOutput&)>;

   private:
    ItemProcessorFn processor_;
    std::optional<ComparatorFn> comparator_;

   public:
    /**
     * @brief Construct batch processor with an item processing function.
     *
     * @param processor Function that processes a single item
     */
    explicit BatchProcessor(ItemProcessorFn processor)
        : processor_(std::move(processor)), comparator_(std::nullopt) {}

    /**
     * @brief Set a comparator for sorting results.
     *
     * @param comparator Function to compare two outputs for sorting
     * @return Reference to this processor for chaining
     */
    BatchProcessor& with_comparator(ComparatorFn comparator) {
        comparator_ = std::move(comparator);
        return *this;
    }

    /**
     * @brief Process all items in parallel, optionally sorting results.
     *
     * @param items List of items to process
     * @return Vector of results (sorted if comparator was set)
     */
    std::vector<ItemOutput> process(
        const std::vector<ItemInput>& items) override {
        if (items.empty()) {
            return {};
        }

        // Get TaskContext for parallel execution
        TaskContext& ctx = this->context();

        // Emit parallel tasks for each item
        std::vector<typename TaskResult<ItemOutput>::Future> futures;
        futures.reserve(items.size());

        for (const auto& item : items) {
            auto task_result =
                ctx.emit<ItemInput, ItemOutput>(processor_, Input{item});
            futures.push_back(std::move(task_result.future()));
        }

        // Wait for all tasks to complete
        std::vector<ItemOutput> results;
        results.reserve(futures.size());

        for (auto& future : futures) {
            results.push_back(future.get());
        }

        // Sort if comparator provided
        if (comparator_.has_value()) {
            std::sort(results.begin(), results.end(), comparator_.value());
        }

        return results;
    }
};

}  // namespace dftracer::utils::components::workflows

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_BATCH_PROCESSOR_H
