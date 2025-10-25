#ifndef DFTRACER_UTILS_COMPONENTS_COMPRESSION_GZIP_DECOMPRESSOR_H
#define DFTRACER_UTILS_COMPONENTS_COMPRESSION_GZIP_DECOMPRESSOR_H

#include <dftracer/utils/components/io/shared.h>
#include <dftracer/utils/core/utilities/utility.h>
#include <zlib.h>

#include <stdexcept>
#include <string>

namespace dftracer::utils::components::compression::gzip {

// Use I/O types for compression
using io::CompressedData;
using io::RawData;

/**
 * @brief Utility that decompresses gzip-compressed data.
 *
 * This utility takes compressed data and decompresses it using the zlib
 * library. It's the inverse operation of Compressor and can be composed in
 * pipelines.
 *
 * Features:
 * - Decompresses gzip-formatted data
 * - Uses original size hint for efficient allocation (if available)
 * - Automatic retry with larger buffer if needed
 * - Can be tagged with Cacheable, Retryable, Monitored behaviors
 *
 * Usage:
 * @code
 * auto compressor = std::make_shared<Compressor>();
 * auto decompressor = std::make_shared<Decompressor>();
 *
 * RawData original("Hello, World!");
 * CompressedData compressed = compressor->process(original);
 * RawData restored = decompressor->process(compressed);
 *
 * // original.data == restored.data
 * assert(original == restored);
 * @endcode
 *
 * With pipeline:
 * @code
 * Pipeline pipeline;
 * auto compress_task = use(compressor).emit_on(pipeline);
 * auto decompress_task = use(decompressor).emit_on(pipeline);
 *
 * // Chain: RawData -> CompressedData -> RawData
 * pipeline.connect(compress_task.id(), decompress_task.id());
 *
 * auto output = SequentialExecutor().execute(pipeline, RawData{"data"});
 * auto restored = output.get<RawData>(decompress_task.id());
 * @endcode
 */
class Decompressor : public utilities::Utility<CompressedData, RawData> {
   public:
    Decompressor() = default;
    ~Decompressor() override = default;

    /**
     * @brief Decompress gzip-compressed data.
     *
     * @param input Compressed data (from Compressor or compatible source)
     * @return RawData with decompressed bytes
     * @throws std::runtime_error if decompression fails
     */
    RawData process(const CompressedData& input) override {
        if (input.data.empty()) {
            return RawData(std::vector<unsigned char>{});
        }

        uLong src_len = static_cast<uLong>(input.data.size());

        // Use original_size as hint, but allow for expansion if needed
        uLong dest_len = input.original_size > 0
                             ? static_cast<uLong>(input.original_size)
                             : src_len * 4;  // Default: assume 4x expansion

        std::vector<unsigned char> decompressed;
        int result = Z_BUF_ERROR;

        // Retry with larger buffer if needed
        constexpr int max_attempts = 5;
        for (int attempt = 0; attempt < max_attempts && result == Z_BUF_ERROR;
             ++attempt) {
            decompressed.resize(dest_len);

            result = uncompress(decompressed.data(), &dest_len,
                                input.data.data(), src_len);

            if (result == Z_BUF_ERROR) {
                // Buffer too small, double the size and retry
                dest_len *= 2;
            }
        }

        if (result != Z_OK) {
            std::string error_msg =
                "Gzip decompression failed with error code: " +
                std::to_string(result);
            if (result == Z_MEM_ERROR) {
                error_msg += " (insufficient memory)";
            } else if (result == Z_BUF_ERROR) {
                error_msg += " (buffer too small after retries)";
            } else if (result == Z_DATA_ERROR) {
                error_msg += " (corrupted or invalid data)";
            }
            throw std::runtime_error(error_msg);
        }

        // Resize to actual decompressed size
        decompressed.resize(dest_len);

        return RawData(std::move(decompressed));
    }
};

}  // namespace dftracer::utils::components::compression::gzip

#endif  // DFTRACER_UTILS_COMPONENTS_COMPRESSION_GZIP_DECOMPRESSOR_H
