#ifndef DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_PLAIN_FILE_BYTES_ITERATOR_H
#define DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_PLAIN_FILE_BYTES_ITERATOR_H

#include <dftracer/utils/components/io/lines/line_types.h>

#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace dftracer::utils::components::io::lines::sources {

/**
 * @brief Iterator for reading lines within byte boundaries from plain text
 * files.
 *
 * This iterator seeks to a byte offset and reads complete lines within
 * the specified byte range. Line boundaries are automatically aligned.
 *
 * Usage:
 * @code
 * PlainFileBytesIterator it("data.txt", 1000, 5000);  // Bytes 1000-5000
 * while (it.has_next()) {
 *     Line line = it.next();
 *     // Process line...
 * }
 * @endcode
 */
class PlainFileBytesIterator {
   private:
    std::string file_path_;
    std::size_t start_byte_;
    std::size_t end_byte_;
    std::vector<Line> lines_;
    std::size_t current_index_;

   public:
    /**
     * @brief Construct iterator for plain text file.
     *
     * Reads all lines within the byte range during construction.
     * Automatically aligns to line boundaries.
     *
     * @param file_path Path to the plain text file
     * @param start_byte Starting byte offset (0-based, inclusive)
     * @param end_byte Ending byte offset (0-based, exclusive)
     */
    PlainFileBytesIterator(std::string file_path, std::size_t start_byte,
                           std::size_t end_byte)
        : file_path_(std::move(file_path)),
          start_byte_(start_byte),
          end_byte_(end_byte),
          current_index_(0) {
        if (start_byte_ >= end_byte_) {
            throw std::invalid_argument("Invalid byte range");
        }

        // Pre-load all lines in the byte range
        load_lines();
    }

    /**
     * @brief Check if more lines are available.
     */
    bool has_next() const { return current_index_ < lines_.size(); }

    /**
     * @brief Get the next line.
     *
     * @return Line object with content and line number
     * @throws std::runtime_error if no more lines available
     */
    Line next() {
        if (!has_next()) {
            throw std::runtime_error("No more lines available");
        }
        return lines_[current_index_++];
    }

    /**
     * @brief Get the current position in the line buffer.
     */
    std::size_t current_position() const { return current_index_; }

    /**
     * @brief Get total number of lines loaded.
     */
    std::size_t total_lines() const { return lines_.size(); }

   private:
    /**
     * @brief Load all lines within the byte range.
     */
    void load_lines() {
        std::ifstream file_stream(file_path_, std::ios::binary);
        if (!file_stream.is_open()) {
            return;
        }

        // Seek to start_byte
        file_stream.seekg(start_byte_);
        std::size_t current_byte = start_byte_;

        // If we're not at the beginning, skip to next newline to align to line
        // boundary
        if (start_byte_ > 0) {
            std::string discard;
            std::getline(file_stream, discard);
            current_byte = file_stream.tellg();
            if (current_byte == static_cast<std::size_t>(-1)) {
                return;  // EOF or error
            }
        }

        // Read lines until we exceed end_byte
        std::size_t line_num = 1;
        while (current_byte < end_byte_ && !file_stream.eof()) {
            Line line;
            line.line_number = line_num;

            std::getline(file_stream, line.content);
            std::size_t line_length =
                line.content.length() + 1;  // +1 for newline

            // Only add if we're still within range
            if (current_byte < end_byte_) {
                lines_.push_back(line);
            }

            current_byte += line_length;
            line_num++;
        }
    }
};

}  // namespace dftracer::utils::components::io::lines::sources

#endif  // DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_PLAIN_FILE_BYTES_ITERATOR_H
