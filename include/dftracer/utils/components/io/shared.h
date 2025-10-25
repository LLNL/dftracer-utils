#ifndef DFTRACER_UTILS_COMPONENTS_IO_SHARED_H
#define DFTRACER_UTILS_COMPONENTS_IO_SHARED_H

#include <dftracer/utils/core/common/filesystem.h>

#include <string>
#include <vector>

namespace dftracer::utils::components::io {

/**
 * @brief Generic binary/raw data structure.
 *
 * This is a fundamental I/O type that represents raw bytes.
 * Used by:
 * - BinaryFileReader output
 * - Compression input/output
 * - Binary data manipulation utilities
 */
struct RawData {
    std::vector<unsigned char> data;

    RawData() = default;

    explicit RawData(std::vector<unsigned char> d) : data(std::move(d)) {}

    explicit RawData(const std::string& str) : data(str.begin(), str.end()) {}

    // Equality for caching support
    bool operator==(const RawData& other) const { return data == other.data; }

    bool operator!=(const RawData& other) const { return !(*this == other); }

    // Convert to string (useful for text data)
    std::string to_string() const {
        return std::string(data.begin(), data.end());
    }

    std::size_t size() const { return data.size(); }

    bool empty() const { return data.empty(); }
};

/**
 * @brief Compressed binary data structure.
 *
 * This represents compressed data along with metadata about the compression.
 * Used by:
 * - Compression utilities (gzip, bzip2, etc.)
 * - File writers that support compression
 */
struct CompressedData {
    std::vector<unsigned char> data;
    std::size_t original_size = 0;  // Original uncompressed size

    CompressedData() = default;

    explicit CompressedData(std::vector<unsigned char> d,
                            std::size_t orig_size = 0)
        : data(std::move(d)), original_size(orig_size) {}

    // Equality for caching support
    bool operator==(const CompressedData& other) const {
        return data == other.data && original_size == other.original_size;
    }

    bool operator!=(const CompressedData& other) const {
        return !(*this == other);
    }

    // Get compression ratio
    double compression_ratio() const {
        if (original_size == 0) return 0.0;
        return static_cast<double>(data.size()) /
               static_cast<double>(original_size);
    }

    // Get space savings percentage
    double space_savings() const {
        if (original_size == 0) return 0.0;
        return (1.0 - compression_ratio()) * 100.0;
    }

    std::size_t size() const { return data.size(); }

    bool empty() const { return data.empty(); }
};

}  // namespace dftracer::utils::components::io

// Hash specializations to enable caching
namespace std {
template <>
struct hash<dftracer::utils::components::io::RawData> {
    std::size_t operator()(
        const dftracer::utils::components::io::RawData& raw) const noexcept {
        // Hash the data bytes
        std::size_t h = 0;
        for (const auto& byte : raw.data) {
            h ^= std::hash<unsigned char>{}(byte) + 0x9e3779b9 + (h << 6) +
                 (h >> 2);
        }
        return h;
    }
};

template <>
struct hash<dftracer::utils::components::io::CompressedData> {
    std::size_t operator()(
        const dftracer::utils::components::io::CompressedData& compressed)
        const noexcept {
        // Hash the data bytes
        std::size_t h = 0;
        for (const auto& byte : compressed.data) {
            h ^= std::hash<unsigned char>{}(byte) + 0x9e3779b9 + (h << 6) +
                 (h >> 2);
        }
        // Combine with original_size
        h ^= std::hash<std::size_t>{}(compressed.original_size) + 0x9e3779b9 +
             (h << 6) + (h >> 2);
        return h;
    }
};
}  // namespace std

#endif  // DFTRACER_UTILS_COMPONENTS_IO_SHARED_H
