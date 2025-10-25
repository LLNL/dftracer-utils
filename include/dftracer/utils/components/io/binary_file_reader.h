#ifndef DFTRACER_UTILS_COMPONENTS_IO_BINARY_FILE_READER_H
#define DFTRACER_UTILS_COMPONENTS_IO_BINARY_FILE_READER_H

#include <dftracer/utils/components/filesystem/directory_scanner.h>
#include <dftracer/utils/components/io/types/types.h>
#include <dftracer/utils/core/utilities/tags/parallelizable.h>
#include <dftracer/utils/core/utilities/utility.h>

#include <fstream>
#include <stdexcept>

namespace dftracer::utils::components::io {

/**
 * @brief Utility that reads a file and returns its binary content.
 *
 * This utility takes a FileEntry and reads the file content as RawData
 * (binary). It composes with existing types from filesystem and compression
 * components.
 *
 * Composition examples:
 * - DirectoryScanner → BinaryFileReader → io::RawData
 * - FileEntry → BinaryFileReader → io::RawData → Compressor →
 * io::CompressedData
 * - FileEntry → BinaryFileReader → io::RawData → TextHasher (for binary
 * hashing)
 *
 * Features:
 * - Reads entire file into memory as binary data
 * - Can be tagged with Cacheable, Retryable, Monitored behaviors
 * - Composes with compression utilities for processing
 *
 * Usage:
 * @code
 * auto reader = std::make_shared<BinaryFileReader>();
 *
 * FileEntry file{"/path/to/file.bin"};
 * compression::gzip::RawData content = reader->process(file);
 *
 * std::cout << "Read " << content.data.size() << " bytes\n";
 * @endcode
 *
 * Composition with compression:
 * @code
 * auto reader = std::make_shared<BinaryFileReader>();
 * auto compressor = std::make_shared<compression::gzip::Compressor>();
 *
 * FileEntry file{"large_file.bin"};
 * auto raw = reader->process(file);
 * auto compressed = compressor->process(raw);
 *
 * std::cout << "Compressed from " << raw.data.size()
 *           << " to " << compressed.data.size() << " bytes\n";
 * @endcode
 */
class BinaryFileReader
    : public utilities::Utility<filesystem::FileEntry, RawData,
                                utilities::tags::Parallelizable> {
   public:
    BinaryFileReader() = default;
    ~BinaryFileReader() override = default;

    /**
     * @brief Read file content as binary data.
     *
     * @param input FileEntry representing the file to read
     * @return RawData containing binary file content
     * @throws std::runtime_error if file cannot be read
     */
    RawData process(const filesystem::FileEntry& input) override {
        if (!fs::exists(input.path)) {
            throw std::runtime_error("File does not exist: " +
                                     input.path.string());
        }

        if (!input.is_regular_file) {
            throw std::runtime_error("Path is not a regular file: " +
                                     input.path.string());
        }

        std::ifstream file(input.path, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open file: " +
                                     input.path.string());
        }

        // Read entire file into vector
        file.seekg(0, std::ios::end);
        std::size_t file_size = static_cast<std::size_t>(file.tellg());
        file.seekg(0, std::ios::beg);

        std::vector<unsigned char> data(file_size);
        file.read(reinterpret_cast<char*>(data.data()),
                  static_cast<std::streamsize>(file_size));

        if (!file) {
            throw std::runtime_error("Error reading file: " +
                                     input.path.string());
        }

        return RawData{std::move(data)};
    }
};

}  // namespace dftracer::utils::components::io

#endif  // DFTRACER_UTILS_COMPONENTS_IO_BINARY_FILE_READER_H
