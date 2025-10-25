#ifndef DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_INDEXED_FILE_LINE_ITERATOR_H
#define DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_INDEXED_FILE_LINE_ITERATOR_H

#include <dftracer/utils/components/io/lines/line_types.h>
#include <dftracer/utils/reader/line_processor.h>
#include <dftracer/utils/reader/reader.h>

#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace dftracer::utils::components::io::lines::sources {

/**
 * @brief Lazy line-by-line iterator over indexed archive files (gzip, tar.gz).
 *
 * This iterator wraps an existing Reader object and provides streaming access
 * to lines without loading the entire file into memory. Multiple iterators can
 * share the same Reader via shared_ptr.
 *
 * Usage:
 * @code
 * auto reader = ReaderFactory::create("file.gz", "file.gz.idx");
 * IndexedFileLineIterator it(reader, 1, 100);  // Lines 1-100
 * while (it.has_next()) {
 *     Line line = it.next();
 *     // Process line...
 * }
 * @endcode
 */
class IndexedFileLineIterator {
   private:
    std::shared_ptr<dftracer::utils::Reader> reader_;
    std::size_t start_line_;
    std::size_t end_line_;
    std::size_t current_line_;
    std::vector<Line> buffer_;
    std::size_t buffer_pos_;
    bool exhausted_;

    static constexpr std::size_t BATCH_SIZE = 1000;  // Lines per batch read

   public:
    /**
     * @brief Construct iterator from existing Reader.
     *
     * @param reader Shared pointer to Reader (supports gzip, tar.gz, etc.)
     * @param start_line Starting line number (1-based, inclusive)
     * @param end_line Ending line number (1-based, inclusive)
     */
    IndexedFileLineIterator(std::shared_ptr<dftracer::utils::Reader> reader,
                            std::size_t start_line, std::size_t end_line)
        : reader_(std::move(reader)),
          start_line_(start_line),
          end_line_(end_line),
          current_line_(start_line),
          buffer_pos_(0),
          exhausted_(false) {
        if (!reader_) {
            throw std::invalid_argument("Reader cannot be null");
        }
        if (start_line < 1 || end_line < start_line) {
            throw std::invalid_argument("Invalid line range");
        }
        if (end_line > reader_->get_num_lines()) {
            throw std::out_of_range("End line exceeds file line count");
        }
    }

    /**
     * @brief Check if more lines are available.
     */
    bool has_next() const {
        return !exhausted_ &&
               (buffer_pos_ < buffer_.size() || current_line_ <= end_line_);
    }

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

        // Refill buffer if needed
        if (buffer_pos_ >= buffer_.size()) {
            refill_buffer();
        }

        if (buffer_pos_ >= buffer_.size()) {
            exhausted_ = true;
            throw std::runtime_error("Failed to read next line");
        }

        return buffer_[buffer_pos_++];
    }

    /**
     * @brief Get the current line position (1-based).
     */
    std::size_t current_position() const { return current_line_; }

    /**
     * @brief Get total number of lines in this range.
     */
    std::size_t total_lines() const {
        return (end_line_ >= start_line_) ? (end_line_ - start_line_ + 1) : 0;
    }

    /**
     * @brief Get the underlying Reader.
     */
    std::shared_ptr<dftracer::utils::Reader> get_reader() const {
        return reader_;
    }

   private:
    /**
     * @brief Refill the internal buffer with the next batch of lines.
     */
    void refill_buffer() {
        if (current_line_ > end_line_) {
            exhausted_ = true;
            return;
        }

        buffer_.clear();
        buffer_pos_ = 0;

        // Calculate batch end (don't exceed end_line_)
        std::size_t batch_end =
            std::min(current_line_ + BATCH_SIZE - 1, end_line_);

        // Use LineProcessor to collect lines
        class LineCollector : public dftracer::utils::LineProcessor {
           private:
            std::vector<Line>& buffer_;
            std::size_t& current_line_;

           public:
            LineCollector(std::vector<Line>& buffer, std::size_t& current_line)
                : buffer_(buffer), current_line_(current_line) {}

            bool process(const char* data, std::size_t length) override {
                buffer_.emplace_back(std::string(data, length), current_line_);
                current_line_++;
                return true;  // Continue processing
            }
        };

        LineCollector collector(buffer_, current_line_);
        reader_->read_lines_with_processor(current_line_, batch_end, collector);

        if (buffer_.empty()) {
            exhausted_ = true;
        }
    }
};

}  // namespace dftracer::utils::components::io::lines::sources

#endif  // DFTRACER_UTILS_COMPONENTS_IO_LINES_SOURCES_INDEXED_FILE_LINE_ITERATOR_H
