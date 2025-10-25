#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_FILE_DECOMPRESSOR_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_FILE_DECOMPRESSOR_H

#include <dftracer/utils/components/compression/gzip/streaming_decompressor.h>
#include <dftracer/utils/components/io/streaming_file_reader.h>
#include <dftracer/utils/components/io/streaming_file_writer.h>
#include <dftracer/utils/core/common/filesystem.h>
#include <dftracer/utils/core/utilities/tags/parallelizable.h>
#include <dftracer/utils/core/utilities/utility.h>

#include <string>

namespace dftracer::utils::components::workflows {

/**
 * @brief Input for file decompression workflow.
 */
struct FileDecompressionInput {
    std::string input_path;  // Input .gz file path
    std::string
        output_path;  // Output decompressed file path (empty = auto-generate)
    std::size_t chunk_size;  // Chunk size for streaming (bytes)

    /**
     * @brief Create input with auto-generated output path.
     *
     * Strips .gz extension from input path to generate output path.
     */
    static FileDecompressionInput from_file(const std::string& input_path,
                                            std::size_t chunk_size = 64 *
                                                                     1024) {
        std::string output = input_path;

        // Strip .gz extension if present
        if (output.size() > 3 && output.substr(output.size() - 3) == ".gz") {
            output = output.substr(0, output.size() - 3);
        } else {
            // If no .gz extension, append .decompressed
            output += ".decompressed";
        }

        return FileDecompressionInput{input_path, output, chunk_size};
    }

    /**
     * @brief Fluent builder: Set output path.
     */
    FileDecompressionInput& with_output(const std::string& path) {
        output_path = path;
        return *this;
    }

    /**
     * @brief Fluent builder: Set chunk size.
     */
    FileDecompressionInput& with_chunk_size(std::size_t size) {
        chunk_size = size;
        return *this;
    }
};

/**
 * @brief Output from file decompression workflow.
 */
struct FileDecompressionOutput {
    std::string input_path;         // Original .gz input file path
    std::string output_path;        // Decompressed output file path
    bool success;                   // Decompression succeeded?
    std::size_t compressed_size;    // Compressed file size (bytes)
    std::size_t decompressed_size;  // Decompressed file size (bytes)
    std::string error_message;      // Error message if failed

    /**
     * @brief Get compression ratio of the original file.
     */
    double original_compression_ratio() const {
        if (decompressed_size == 0) return 0.0;
        return static_cast<double>(compressed_size) /
               static_cast<double>(decompressed_size);
    }
};

/**
 * @brief Workflow for decompressing gzip files using streaming decompression.
 *
 * This workflow:
 * 1. Reads compressed .gz file in chunks using StreamingFileReader
 * 2. Decompresses each chunk using StreamingDecompressor
 * 3. Writes decompressed data to output file using StreamingFileWriter
 *
 * Tagged with Parallelizable - safe for parallel batch processing.
 *
 * Usage:
 * @code
 * // Single file decompression
 * auto decompressor = std::make_shared<FileDecompressor>();
 * auto input = FileDecompressionInput::from_file("archive.gz");
 * auto result = decompressor->process(input);
 *
 * // Parallel batch decompression
 * auto batch_decompressor = std::make_shared<
 *     BatchProcessor<FileDecompressionInput, FileDecompressionOutput>>(
 *         [decompressor](const FileDecompressionInput& input, TaskContext& ctx)
 * { return decompressor->process(input);
 *         }
 * );
 *
 * std::vector<FileDecompressionInput> files = { ... };
 * auto results = batch_decompressor->process(files);
 * @endcode
 */
class FileDecompressor
    : public utilities::Utility<FileDecompressionInput, FileDecompressionOutput,
                                utilities::tags::Parallelizable> {
   public:
    FileDecompressor() = default;
    ~FileDecompressor() override = default;

    /**
     * @brief Decompress a gzip file using streaming decompression.
     *
     * @param input Decompression configuration
     * @return Decompression result with statistics
     */
    FileDecompressionOutput process(
        const FileDecompressionInput& input) override {
        FileDecompressionOutput result{
            input.input_path,
            input.output_path,
            false,  // success
            0,      // compressed_size
            0,      // decompressed_size
            ""      // error_message
        };

        try {
            // Validate input file exists
            if (!fs::exists(input.input_path)) {
                result.error_message =
                    "Input file does not exist: " + input.input_path;
                return result;
            }

            // Get compressed file size
            result.compressed_size = fs::file_size(input.input_path);

            // Step 1: Create streaming reader with chunk size
            auto reader = std::make_shared<io::StreamingFileReader>();

            // Step 2: Create streaming decompressor
            auto decompressor =
                std::make_shared<compression::gzip::StreamingDecompressor>();

            // Step 3: Create streaming writer
            io::StreamingFileWriter writer(input.output_path);

            // Step 4: Read compressed file as chunks (chunk size in input)
            io::StreamReadInput read_input{input.input_path, input.chunk_size};
            io::ChunkRange chunks = reader->process(read_input);

            // Step 5: Decompress chunks and write
            for (const auto& chunk : chunks) {
                // Treat chunk as compressed data
                io::CompressedData compressed_chunk{chunk.data};

                // Decompress chunk (may produce multiple output chunks)
                std::vector<io::RawData> decompressed_chunks =
                    decompressor->decompress_chunk(compressed_chunk);

                // Write all decompressed chunks
                for (const auto& decompressed : decompressed_chunks) {
                    writer.write_chunk(decompressed);
                }
            }

            // Step 6: Close writer (flushes any remaining data)
            writer.close();

            // Get final decompressed size
            result.decompressed_size = fs::file_size(input.output_path);
            result.success = true;

        } catch (const std::exception& e) {
            result.error_message =
                std::string("Decompression failed: ") + e.what();

            // Clean up partial output file on error
            if (fs::exists(input.output_path)) {
                try {
                    fs::remove(input.output_path);
                } catch (...) {
                    // Ignore cleanup errors
                }
            }
        }

        return result;
    }
};

}  // namespace dftracer::utils::components::workflows

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_FILE_DECOMPRESSOR_H
