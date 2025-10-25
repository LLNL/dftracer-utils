#ifndef DFTRACER_UTILS_COMPONENTS_IO_LINES_LINE_TYPES_H
#define DFTRACER_UTILS_COMPONENTS_IO_LINES_LINE_TYPES_H

#include <cstddef>
#include <functional>
#include <string>
#include <utility>

namespace dftracer::utils::components::io::lines {

/**
 * @brief Represents a single line of text with metadata.
 *
 * This structure holds a line's content along with its line number,
 * enabling easy tracking and processing of lines from various sources.
 */
struct Line {
    std::string content;
    std::size_t line_number;  // 1-based line number

    Line() : line_number(0) {}
    Line(std::string content_, std::size_t line_number_)
        : content(std::move(content_)), line_number(line_number_) {}

    bool empty() const { return content.empty(); }
    std::size_t size() const { return content.size(); }
};

/**
 * @brief Input for reading a range of lines from an indexed file.
 *
 * This structure encapsulates all information needed to read a specific
 * range of lines from an indexed archive (gzip, tar.gz, etc.).
 * Used for lazy evaluation and caching strategies.
 *
 * Usage:
 * @code
 * auto input = LineReadInput::from_file("data.txt")
 *                  .with_index("data.txt.idx")
 *                  .with_range(10, 100);
 * @endcode
 */
struct LineReadInput {
    std::string file_path;   // Path to the archive file
    std::string idx_path;    // Path to the index file (empty for plain files)
    std::size_t start_line;  // Starting line (1-based, inclusive), 0 = start
    std::size_t end_line;    // Ending line (1-based, inclusive), 0 = end

    LineReadInput() : start_line(0), end_line(0) {}

    LineReadInput(std::string file_path_, std::string idx_path_,
                  std::size_t start_line_, std::size_t end_line_)
        : file_path(std::move(file_path_)),
          idx_path(std::move(idx_path_)),
          start_line(start_line_),
          end_line(end_line_) {}

    static LineReadInput from_file(std::string path) {
        LineReadInput input;
        input.file_path = std::move(path);
        return input;
    }

    LineReadInput& with_index(std::string idx) {
        idx_path = std::move(idx);
        return *this;
    }

    LineReadInput& with_range(std::size_t start, std::size_t end) {
        start_line = start;
        end_line = end;
        return *this;
    }

    bool operator==(const LineReadInput& other) const {
        return file_path == other.file_path && idx_path == other.idx_path &&
               start_line == other.start_line && end_line == other.end_line;
    }

    bool operator!=(const LineReadInput& other) const {
        return !(*this == other);
    }

    std::size_t num_lines() const {
        return (end_line >= start_line) ? (end_line - start_line + 1) : 0;
    }
};

}  // namespace dftracer::utils::components::io::lines

// Hash specializations for using Line and LineReadInput in hash-based
// containers
namespace std {

template <>
struct hash<dftracer::utils::components::io::lines::Line> {
    std::size_t operator()(
        const dftracer::utils::components::io::lines::Line& line) const {
        // Combine hash of content and line number
        std::size_t h1 = std::hash<std::string>{}(line.content);
        std::size_t h2 = std::hash<std::size_t>{}(line.line_number);
        return h1 ^ (h2 << 1);
    }
};

template <>
struct hash<dftracer::utils::components::io::lines::LineReadInput> {
    std::size_t operator()(
        const dftracer::utils::components::io::lines::LineReadInput& req)
        const {
        // Combine hashes of all fields
        std::size_t h1 = std::hash<std::string>{}(req.file_path);
        std::size_t h2 = std::hash<std::string>{}(req.idx_path);
        std::size_t h3 = std::hash<std::size_t>{}(req.start_line);
        std::size_t h4 = std::hash<std::size_t>{}(req.end_line);
        return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
    }
};

}  // namespace std

#endif  // DFTRACER_UTILS_COMPONENTS_IO_LINES_LINE_TYPES_H
