#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_LINE_BATCH_PROCESSOR_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_LINE_BATCH_PROCESSOR_H

#include <dftracer/utils/components/io/lines/line_range.h>
#include <dftracer/utils/components/io/lines/line_types.h>
#include <dftracer/utils/components/io/lines/streaming_line_reader.h>
#include <dftracer/utils/components/workflows/workflow_types.h>
#include <dftracer/utils/core/utilities/utilities.h>

#include <functional>
#include <optional>
#include <vector>

namespace dftracer::utils::components::workflows {

/**
 * @brief Workflow for processing lines from a file with streaming iteration.
 *
 * This workflow:
 * 1. Uses StreamingLineReader for lazy line iteration
 * 2. Applies a processing function to each line
 * 3. Collects results (optional - can filter out nullopt)
 *
 * Template Parameters:
 * - LineOutput: Type returned by the line processor function
 *
 * Usage:
 * @code
 * auto processor = [](const Line& line) -> std::optional<MyData> {
 *     // Process line, return std::nullopt to skip
 *     if (should_process(line)) {
 *         return MyData{...};
 *     }
 *     return std::nullopt;
 * };
 *
 * LineBatchProcessor<MyData> workflow(processor);
 * auto results = workflow.process(LineBatchInput{"/path/to/file.gz",
 * "file.gz.idx"});
 * @endcode
 */
template <typename LineOutput>
class LineBatchProcessor : public utilities::Utility<io::lines::LineReadInput,
                                                     std::vector<LineOutput>> {
   public:
    using LineProcessorFn =
        std::function<std::optional<LineOutput>(const io::lines::Line&)>;

   private:
    LineProcessorFn processor_;

   public:
    /**
     * @brief Construct processor with a line processing function.
     *
     * @param processor Function that processes a single line
     */
    explicit LineBatchProcessor(LineProcessorFn processor)
        : processor_(std::move(processor)) {}

    /**
     * @brief Process lines from a file using streaming iteration.
     *
     * @param input Line batch configuration
     * @return Vector of processed line results
     */
    std::vector<LineOutput> process(
        const io::lines::LineReadInput& input) override {
        std::vector<LineOutput> results;

        // Use StreamingLineReader for auto-detection and lazy iteration
        io::lines::LineRange range;

        if (!input.idx_path.empty()) {
            // Indexed file (compressed)
            if (input.start_line > 0 && input.end_line > 0) {
                range = io::lines::StreamingLineReader::read_indexed(
                    input.file_path, input.idx_path, input.start_line,
                    input.end_line);
            } else {
                range = io::lines::StreamingLineReader::read_indexed(
                    input.file_path, input.idx_path);
            }
        } else {
            // Plain text file
            if (input.start_line > 0 && input.end_line > 0) {
                range = io::lines::StreamingLineReader::read_plain(
                    input.file_path, input.start_line, input.end_line);
            } else {
                range =
                    io::lines::StreamingLineReader::read_plain(input.file_path);
            }
        }

        // Process each line lazily
        while (range.has_next()) {
            io::lines::Line line = range.next();

            // Apply processor function
            auto result = processor_(line);

            // Collect non-null results
            if (result.has_value()) {
                results.push_back(std::move(result.value()));
            }
        }

        return results;
    }
};

/**
 * @brief Simplified line batch processor that always processes all lines.
 *
 * Use this when you want to process every line without filtering.
 */
template <typename LineOutput>
class SimpleLineBatchProcessor
    : public utilities::Utility<io::lines::LineReadInput,
                                std::vector<LineOutput>> {
   public:
    using SimpleLineProcessorFn =
        std::function<LineOutput(const io::lines::Line&)>;

   private:
    SimpleLineProcessorFn processor_;

   public:
    explicit SimpleLineBatchProcessor(SimpleLineProcessorFn processor)
        : processor_(std::move(processor)) {}

    std::vector<LineOutput> process(
        const io::lines::LineReadInput& input) override {
        std::vector<LineOutput> results;

        // Use StreamingLineReader
        io::lines::LineRange range;

        if (!input.idx_path.empty()) {
            if (input.start_line > 0 && input.end_line > 0) {
                range = io::lines::StreamingLineReader::read_indexed(
                    input.file_path, input.idx_path, input.start_line,
                    input.end_line);
            } else {
                range = io::lines::StreamingLineReader::read_indexed(
                    input.file_path, input.idx_path);
            }
        } else {
            if (input.start_line > 0 && input.end_line > 0) {
                range = io::lines::StreamingLineReader::read_plain(
                    input.file_path, input.start_line, input.end_line);
            } else {
                range =
                    io::lines::StreamingLineReader::read_plain(input.file_path);
            }
        }

        // Process all lines
        while (range.has_next()) {
            results.push_back(processor_(range.next()));
        }

        return results;
    }
};

}  // namespace dftracer::utils::components::workflows

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_LINE_BATCH_PROCESSOR_H
