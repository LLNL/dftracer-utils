#ifndef DFTRACER_UTILS_COMPONENTS_IO_STREAMING_FILE_READER_H
#define DFTRACER_UTILS_COMPONENTS_IO_STREAMING_FILE_READER_H

#include <dftracer/utils/components/io/chunk_iterator.h>
#include <dftracer/utils/components/io/streaming.h>
#include <dftracer/utils/components/io/types/types.h>
#include <dftracer/utils/core/utilities/utility.h>

#include <stdexcept>

namespace dftracer::utils::components::io {

/**
 * @brief Streaming file reader utility that returns lazy iterator.
 *
 * This utility provides a ChunkRange for lazy iteration over file chunks.
 * Only ONE chunk is in memory at a time - true streaming!
 *
 * Composable utility pattern:
 * - Input: StreamReadInput (file path + chunk size)
 * - Output: ChunkRange (lazy iterator)
 *
 * Usage:
 * @code
 * auto reader = std::make_shared<StreamingFileReader>();
 *
 * StreamReadInput input{"/path/to/large/file.txt", 64 * 1024};
 * ChunkRange chunks = reader->process(input);
 *
 * // Only one chunk in memory at a time!
 * for (const auto& chunk : chunks) {
 *     // Process chunk immediately
 *     compressor.process_chunk(chunk);
 * }
 * @endcode
 *
 * With streaming compression:
 * @code
 * auto reader = std::make_shared<StreamingFileReader>();
 * StreamingCompressor compressor(&writer);
 *
 * for (const auto& chunk : reader->process(StreamReadInput{"input.txt"})) {
 *     compressor.process_chunk(chunk);  // True streaming - constant memory!
 * }
 * compressor.finalize();
 * @endcode
 */
class StreamingFileReader
    : public utilities::Utility<StreamReadInput, ChunkRange> {
   public:
    StreamingFileReader() = default;
    ~StreamingFileReader() override = default;

    /**
     * @brief Get lazy chunk iterator for file.
     *
     * @param input StreamReadInput with file path and chunk size
     * @return ChunkRange for iterating over chunks
     * @throws std::runtime_error if file cannot be accessed
     */
    ChunkRange process(const StreamReadInput& input) override {
        if (!fs::exists(input.path)) {
            throw std::runtime_error("File does not exist: " +
                                     input.path.string());
        }

        if (!fs::is_regular_file(input.path)) {
            throw std::runtime_error("Path is not a regular file: " +
                                     input.path.string());
        }

        return ChunkRange{input.path, input.chunk_size};
    }
};

}  // namespace dftracer::utils::components::io

#endif  // DFTRACER_UTILS_COMPONENTS_IO_STREAMING_FILE_READER_H
