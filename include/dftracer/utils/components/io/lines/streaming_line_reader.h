#ifndef DFTRACER_UTILS_COMPONENTS_IO_LINES_STREAMING_LINE_READER_H
#define DFTRACER_UTILS_COMPONENTS_IO_LINES_STREAMING_LINE_READER_H

#include <dftracer/utils/components/io/lines/line_range.h>
#include <dftracer/utils/components/io/lines/line_types.h>
#include <dftracer/utils/core/common/filesystem.h>
#include <dftracer/utils/reader/reader_factory.h>

#include <memory>
#include <string>

namespace dftracer::utils::components::io::lines {

/**
 * @brief Composable utility for streaming line reading from various sources.
 *
 * This utility automatically detects the file format and creates the
 * appropriate line iterator. It supports:
 * - Indexed compressed files (.gz, .tar.gz) via Reader
 * - Plain text files
 * - Automatic index file detection
 *
 * Usage:
 * @code
 * // Auto-detect format
 * auto range = StreamingLineReader::read("data.gz");  // Uses index if
 * available while (range.has_next()) { Line line = range.next();
 *     // Process line...
 * }
 *
 * // Explicit line range
 * auto range2 = StreamingLineReader::read("data.gz", 100, 200);
 *
 * // Force plain file reading (no decompression)
 * auto range3 = StreamingLineReader::read_plain("data.txt");
 * @endcode
 */
class StreamingLineReader {
   public:
    /**
     * @brief Read lines from a file, auto-detecting format and index.
     *
     * This method automatically:
     * 1. Detects if an index file exists (.idx)
     * 2. Creates appropriate reader (indexed or plain)
     * 3. Returns a LineRange for streaming iteration
     *
     * @param file_path Path to the file
     * @param start_line Starting line (1-based, inclusive), 0 means start
     * @param end_line Ending line (1-based, inclusive), 0 means end
     * @return LineRange for streaming iteration
     */
    static LineRange read(const std::string& file_path,
                          std::size_t start_line = 0,
                          std::size_t end_line = 0) {
        // Check if index file exists
        std::string idx_path = file_path + ".idx";
        bool has_index = fs::exists(idx_path);

        // Check file extension to determine if it's compressed
        bool is_compressed = is_compressed_format(file_path);

        if (is_compressed && has_index) {
            // Use indexed reader for compressed files
            auto reader =
                dftracer::utils::ReaderFactory::create(file_path, idx_path);

            std::size_t actual_start = (start_line > 0) ? start_line : 1;
            std::size_t actual_end =
                (end_line > 0) ? end_line : reader->get_num_lines();

            return LineRange::from_indexed_file(reader, actual_start,
                                                actual_end);
        } else {
            // Use plain file reader
            if (start_line > 0 && end_line > 0) {
                return LineRange::from_plain_file(file_path, start_line,
                                                  end_line);
            } else {
                return LineRange::from_plain_file(file_path);
            }
        }
    }

    /**
     * @brief Read lines from a file using indexed reader.
     *
     * @param file_path Path to the compressed file
     * @param idx_path Path to the index file
     * @param start_line Starting line (1-based, inclusive), 0 means start
     * @param end_line Ending line (1-based, inclusive), 0 means end
     * @return LineRange for streaming iteration
     */
    static LineRange read_indexed(const std::string& file_path,
                                  const std::string& idx_path,
                                  std::size_t start_line = 0,
                                  std::size_t end_line = 0) {
        auto reader =
            dftracer::utils::ReaderFactory::create(file_path, idx_path);

        std::size_t actual_start = (start_line > 0) ? start_line : 1;
        std::size_t actual_end =
            (end_line > 0) ? end_line : reader->get_num_lines();

        return LineRange::from_indexed_file(reader, actual_start, actual_end);
    }

    /**
     * @brief Read lines from a plain text file (no decompression).
     *
     * @param file_path Path to the plain text file
     * @param start_line Starting line (1-based, inclusive), 0 means start
     * @param end_line Ending line (1-based, inclusive), 0 means end
     * @return LineRange for streaming iteration
     */
    static LineRange read_plain(const std::string& file_path,
                                std::size_t start_line = 0,
                                std::size_t end_line = 0) {
        if (start_line > 0 && end_line > 0) {
            return LineRange::from_plain_file(file_path, start_line, end_line);
        } else {
            return LineRange::from_plain_file(file_path);
        }
    }

    /**
     * @brief Read lines using an existing Reader.
     *
     * @param reader Shared pointer to Reader
     * @param start_line Starting line (1-based, inclusive), 0 means start
     * @param end_line Ending line (1-based, inclusive), 0 means end
     * @return LineRange for streaming iteration
     */
    static LineRange read_from_reader(
        std::shared_ptr<dftracer::utils::Reader> reader,
        std::size_t start_line = 0, std::size_t end_line = 0) {
        std::size_t actual_start = (start_line > 0) ? start_line : 1;
        std::size_t actual_end =
            (end_line > 0) ? end_line : reader->get_num_lines();

        return LineRange::from_indexed_file(reader, actual_start, actual_end);
    }

   private:
    /**
     * @brief Check if file extension indicates compressed format.
     */
    static bool is_compressed_format(const std::string& file_path) {
        fs::path p(file_path);
        std::string ext = p.extension().string();

        // Check for .gz extension
        if (ext == ".gz") {
            return true;
        }

        // Check for .tar.gz or .tgz
        std::string stem = p.stem().string();
        if (!stem.empty()) {
            fs::path stem_path(stem);
            if (stem_path.extension().string() == ".tar") {
                return true;
            }
        }

        if (ext == ".tgz") {
            return true;
        }

        return false;
    }
};

}  // namespace dftracer::utils::components::io::lines

#endif  // DFTRACER_UTILS_COMPONENTS_IO_LINES_STREAMING_LINE_READER_H
