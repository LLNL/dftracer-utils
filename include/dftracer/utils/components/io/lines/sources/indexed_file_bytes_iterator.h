#ifndef DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_INDEXED_FILE_BYTES_ITERATOR_H
#define DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_INDEXED_FILE_BYTES_ITERATOR_H

#include <dftracer/utils/components/io/lines/line_types.h>
#include <dftracer/utils/reader/line_processor.h>
#include <dftracer/utils/reader/reader.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace dftracer::utils::components::io::lines::sources {

/**
 * @brief Iterator for reading lines within byte boundaries from indexed files.
 *
 * This iterator uses the Reader's byte-based API to read lines that fall
 * within a specified byte range. Line boundaries are automatically aligned.
 *
 * Usage:
 * @code
 * auto reader = ReaderFactory::create("file.gz", "file.gz.idx");
 * IndexedFileBytesIterator it(reader, 1000, 5000);  // Bytes 1000-5000
 * while (it.has_next()) {
 *     Line line = it.next();
 *     // Process line...
 * }
 * @endcode
 */
class IndexedFileBytesIterator {
   private:
    std::shared_ptr<dftracer::utils::Reader> reader_;
    std::size_t start_byte_;
    std::size_t end_byte_;
    std::vector<Line> lines_;
    std::size_t current_index_;

   public:
    /**
     * @brief Construct iterator from existing Reader.
     *
     * Reads all lines within the byte range during construction.
     * Uses Reader's read_line_bytes_with_processor for efficient reading.
     *
     * @param reader Shared pointer to Reader
     * @param start_byte Starting byte offset (0-based, inclusive)
     * @param end_byte Ending byte offset (0-based, exclusive)
     */
    IndexedFileBytesIterator(std::shared_ptr<dftracer::utils::Reader> reader,
                             std::size_t start_byte, std::size_t end_byte)
        : reader_(std::move(reader)),
          start_byte_(start_byte),
          end_byte_(end_byte),
          current_index_(0) {
        if (!reader_) {
            throw std::invalid_argument("Reader cannot be null");
        }
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
        // Create a LineProcessor to collect lines
        class LineCollector : public dftracer::utils::LineProcessor {
           public:
            explicit LineCollector(std::vector<Line>* lines)
                : lines_(lines), line_num_(1) {}

            bool process(const char* data, std::size_t length) override {
                Line line;
                line.line_number = line_num_++;
                line.content = std::string(data, length);
                lines_->push_back(line);
                return true;  // Continue processing
            }

           private:
            std::vector<Line>* lines_;
            std::size_t line_num_;
        };

        LineCollector collector(&lines_);
        reader_->read_line_bytes_with_processor(start_byte_, end_byte_,
                                                collector);
    }
};

}  // namespace dftracer::utils::components::io::lines::sources

#endif  // DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_INDEXED_FILE_BYTES_ITERATOR_H
