#include <dftracer/utils/core/utils/timer.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/indexer/indexer_factory.h>
#include <dftracer/utils/reader/error.h>
#include <dftracer/utils/reader/gzip_reader.h>
#include <dftracer/utils/reader/string_line_processor.h>

#include <cstdio>
#include <cstring>
#include <limits>
#include <string_view>

static void validate_parameters(
    const char *buffer, std::size_t buffer_size, std::size_t start_bytes,
    std::size_t end_bytes,
    std::size_t max_bytes = std::numeric_limits<std::size_t>::max()) {
    if (!buffer || buffer_size == 0) {
        throw ReaderError(ReaderError::INVALID_ARGUMENT,
                          "Invalid buffer parameters");
    }
    if (start_bytes >= end_bytes) {
        throw ReaderError(ReaderError::INVALID_ARGUMENT,
                          "start_bytes must be less than end_bytes");
    }
    if (max_bytes != SIZE_MAX) {
        if (end_bytes > max_bytes) {
            throw ReaderError(ReaderError::INVALID_ARGUMENT,
                              "end_bytes exceeds maximum available bytes");
        }
        if (start_bytes > max_bytes) {
            throw ReaderError(ReaderError::INVALID_ARGUMENT,
                              "start_bytes exceeds maximum available bytes");
        }
    }
}

static void check_reader_state(bool is_open, const void *indexer) {
    if (!is_open || !indexer) {
        throw std::runtime_error("Reader is not open");
    }
}

static constexpr std::size_t DEFAULT_READER_BUFFER_SIZE = 1 * 1024 * 1024;

namespace dftracer::utils {

GzipReader::GzipReader(const std::string &gz_path_,
                       const std::string &idx_path_,
                       std::size_t index_ckpt_size)
    : gz_path(gz_path_),
      idx_path(idx_path_),
      is_open(false),
      default_buffer_size(DEFAULT_READER_BUFFER_SIZE),
      indexer(nullptr) {
    try {
        indexer =
            IndexerFactory::create(gz_path, idx_path, index_ckpt_size, false);
        is_open = true;

        stream_factory = std::make_unique<GzipStreamFactory>(*indexer);

        DFTRACER_UTILS_LOG_DEBUG(
            "Successfully created GZIP reader for gz: %s and index: %s",
            gz_path.c_str(), idx_path.c_str());
    } catch (const std::exception &e) {
        throw ReaderError(ReaderError::INITIALIZATION_ERROR,
                          "Failed to initialize reader with indexer: " +
                              std::string(e.what()));
    }
}

GzipReader::GzipReader(std::shared_ptr<Indexer> indexer_)
    : default_buffer_size(DEFAULT_READER_BUFFER_SIZE), indexer(indexer_) {
    if (!indexer) {
        throw ReaderError(ReaderError::INITIALIZATION_ERROR,
                          "Invalid indexer provided");
    }
    stream_factory = std::make_unique<GzipStreamFactory>(*indexer);
    is_open = true;
    gz_path = indexer->get_archive_path();
    idx_path = indexer->get_idx_path();
}

GzipReader::~GzipReader() = default;

GzipReader::GzipReader(GzipReader &&other) noexcept
    : gz_path(std::move(other.gz_path)),
      idx_path(std::move(other.idx_path)),
      is_open(other.is_open),
      default_buffer_size(other.default_buffer_size),
      indexer(std::move(other.indexer)),
      stream_factory(std::move(other.stream_factory)),
      byte_stream(std::move(other.byte_stream)),
      line_byte_stream(std::move(other.line_byte_stream)) {
    other.is_open = false;
}

GzipReader &GzipReader::operator=(GzipReader &&other) noexcept {
    if (this != &other) {
        gz_path = std::move(other.gz_path);
        idx_path = std::move(other.idx_path);
        is_open = other.is_open;
        default_buffer_size = other.default_buffer_size;
        indexer = std::move(other.indexer);
        stream_factory = std::move(other.stream_factory);
        byte_stream = std::move(other.byte_stream);
        line_byte_stream = std::move(other.line_byte_stream);
        other.is_open = false;
    }
    return *this;
}

std::size_t GzipReader::get_max_bytes() const {
    check_reader_state(is_open, indexer.get());
    std::size_t max_bytes =
        static_cast<std::size_t>(indexer.get()->get_max_bytes());
    DFTRACER_UTILS_LOG_DEBUG("Maximum bytes available: %zu", max_bytes);
    return max_bytes;
}

std::size_t GzipReader::get_num_lines() const {
    check_reader_state(is_open, indexer.get());
    std::size_t num_lines = static_cast<std::size_t>(indexer->get_num_lines());
    DFTRACER_UTILS_LOG_DEBUG("Total lines available: %zu", num_lines);
    return num_lines;
}

const std::string &GzipReader::get_archive_path() const { return gz_path; }

const std::string &GzipReader::get_idx_path() const { return idx_path; }

void GzipReader::set_buffer_size(std::size_t size) {
    default_buffer_size = size;
}

void GzipReader::reset() {
    check_reader_state(is_open, indexer.get());
    if (line_byte_stream) {
        line_byte_stream->reset();
    }
    if (byte_stream) {
        byte_stream->reset();
    }
}

std::size_t GzipReader::read(std::size_t start_bytes, std::size_t end_bytes,
                             char *buffer, std::size_t buffer_size) {
    check_reader_state(is_open, indexer.get());
    validate_parameters(buffer, buffer_size, start_bytes, end_bytes,
                        indexer.get()->get_max_bytes());

    DFTRACER_UTILS_LOG_DEBUG(
        "GzipReader::read - request: start_bytes=%zu, end_bytes=%zu, "
        "buffer_size=%zu",
        start_bytes, end_bytes, buffer_size);

    if (stream_factory->needs_new_byte_stream(byte_stream.get(), gz_path,
                                              start_bytes, end_bytes)) {
        DFTRACER_UTILS_LOG_DEBUG("GzipReader::read - creating new byte stream",
                                 "");
        byte_stream =
            stream_factory->create_byte_stream(gz_path, start_bytes, end_bytes);
    } else {
        DFTRACER_UTILS_LOG_DEBUG(
            "GzipReader::read - reusing existing byte stream", "");
    }

    if (byte_stream->is_finished()) {
        DFTRACER_UTILS_LOG_DEBUG("GzipReader::read - stream is finished", "");
        return 0;
    }

    std::size_t result = byte_stream->stream(buffer, buffer_size);
    DFTRACER_UTILS_LOG_DEBUG("GzipReader::read - returned %zu bytes", result);
    return result;
}

std::size_t GzipReader::read_line_bytes(std::size_t start_bytes,
                                        std::size_t end_bytes, char *buffer,
                                        std::size_t buffer_size) {
    check_reader_state(is_open, indexer.get());

    if (end_bytes > indexer.get()->get_max_bytes()) {
        end_bytes = indexer.get()->get_max_bytes();
    }

    validate_parameters(buffer, buffer_size, start_bytes, end_bytes,
                        indexer.get()->get_max_bytes());

    if (stream_factory->needs_new_line_stream(line_byte_stream.get(), gz_path,
                                              start_bytes, end_bytes)) {
        line_byte_stream =
            stream_factory->create_line_stream(gz_path, start_bytes, end_bytes);
    }

    if (line_byte_stream->is_finished()) {
        return 0;
    }

    return line_byte_stream->stream(buffer, buffer_size);
}

std::string GzipReader::read_lines(std::size_t start_line,
                                   std::size_t end_line) {
    check_reader_state(is_open, indexer.get());

    if (start_line == 0 || end_line == 0) {
        throw std::runtime_error("Line numbers must be 1-based (start from 1)");
    }
    check_reader_state(is_open, indexer.get());
    if (start_line > end_line) {
        throw std::runtime_error("Start line must be <= end line");
    }

    std::size_t total_lines = indexer.get()->get_num_lines();
    if (start_line > total_lines || end_line > total_lines) {
        throw std::runtime_error("Line numbers exceed total lines in file (" +
                                 std::to_string(total_lines) + ")");
    }

    std::string result;
    StringLineProcessor processor(result);
    read_lines_with_processor(start_line, end_line, processor);
    return result;
}

void GzipReader::read_lines_with_processor(std::size_t start_line,
                                           std::size_t end_line,
                                           LineProcessor &processor) {
    check_reader_state(is_open, indexer.get());

    if (start_line == 0 || end_line == 0) {
        throw std::runtime_error("Line numbers must be 1-based (start from 1)");
    }

    if (start_line > end_line) {
        throw std::runtime_error("Start line must be <= end line");
    }

    std::size_t total_lines = indexer.get()->get_num_lines();
    if (start_line > total_lines || end_line > total_lines) {
        throw std::runtime_error("Line numbers exceed total lines in file (" +
                                 std::to_string(total_lines) + ")");
    }

    processor.begin(start_line, end_line);

    std::vector<char> process_buffer(default_buffer_size);
    std::size_t buffer_usage = 0;

    std::vector<IndexerCheckpoint> checkpoints =
        indexer.get()->get_checkpoints_for_line_range(start_line, end_line);

    if (checkpoints.empty()) {
        std::size_t max_bytes = indexer.get()->get_max_bytes();
        line_byte_stream =
            stream_factory->create_line_stream(gz_path, 0, max_bytes);

        std::size_t current_line = 1;
        std::string line_accumulator;

        while (!line_byte_stream->is_finished() && current_line <= end_line) {
            std::size_t bytes_read =
                line_byte_stream->stream(process_buffer.data() + buffer_usage,
                                         default_buffer_size - buffer_usage);
            if (bytes_read == 0) break;

            buffer_usage += bytes_read;

            process_lines(process_buffer.data(), buffer_usage, current_line,
                          start_line, end_line, line_accumulator, processor);

            buffer_usage = 0;
        }

        if (!line_accumulator.empty() && current_line >= start_line &&
            current_line <= end_line) {
            processor.process(line_accumulator.c_str(),
                              line_accumulator.length());
        }
    } else {
        std::uint64_t total_start_offset = 0;
        std::uint64_t total_end_offset = 0;
        std::uint64_t first_line_in_data = 1;

        if (checkpoints[0].checkpoint_idx == 0) {
            total_start_offset = 0;
            first_line_in_data = 1;
        } else {
            auto all_checkpoints = indexer.get()->get_checkpoints();
            for (const auto &prev_ckpt : all_checkpoints) {
                if (prev_ckpt.checkpoint_idx ==
                    checkpoints[0].checkpoint_idx - 1) {
                    total_start_offset = prev_ckpt.uc_offset;
                    first_line_in_data = prev_ckpt.last_line_num + 1;
                    break;
                }
            }
        }

        const auto &last_checkpoint = checkpoints.back();
        total_end_offset = last_checkpoint.uc_offset + last_checkpoint.uc_size;

        // Use chunked reading instead of allocating huge buffer
        std::vector<char> chunk_buffer(default_buffer_size);
        std::string line_accumulator;
        std::size_t current_line = first_line_in_data;

        // Create stream for the range
        line_byte_stream = stream_factory->create_line_stream(
            gz_path, total_start_offset, total_end_offset);

        while (!line_byte_stream->is_finished() && current_line <= end_line) {
            std::size_t bytes_read = line_byte_stream->stream(
                chunk_buffer.data(), chunk_buffer.size());
            if (bytes_read == 0) break;

            process_lines(chunk_buffer.data(), bytes_read, current_line,
                          start_line, end_line, line_accumulator, processor);
        }

        if (!line_accumulator.empty() && current_line >= start_line &&
            current_line <= end_line) {
            processor.process(line_accumulator.c_str(),
                              line_accumulator.length());
        }
    }

    processor.end();
}

void GzipReader::read_line_bytes_with_processor(std::size_t start_bytes,
                                                std::size_t end_bytes,
                                                LineProcessor &processor) {
    check_reader_state(is_open, indexer.get());

    if (end_bytes > indexer.get()->get_max_bytes()) {
        end_bytes = indexer.get()->get_max_bytes();
    }

    if (start_bytes >= end_bytes) {
        return;
    }

    processor.begin(start_bytes, end_bytes);

    // Use the same approach as read_line_bytes to ensure consistency
    std::vector<char> buffer(default_buffer_size);

    if (stream_factory->needs_new_line_stream(line_byte_stream.get(), gz_path,
                                              start_bytes, end_bytes)) {
        line_byte_stream =
            stream_factory->create_line_stream(gz_path, start_bytes, end_bytes);
    }

    std::string line_accumulator;

    while (!line_byte_stream->is_finished()) {
        std::size_t bytes_read =
            line_byte_stream->stream(buffer.data(), buffer.size());
        if (bytes_read == 0) break;

        // Process the buffer line by line
        std::size_t pos = 0;
        while (pos < bytes_read) {
            const char *newline_ptr = static_cast<const char *>(
                std::memchr(buffer.data() + pos, '\n', bytes_read - pos));

            if (newline_ptr != nullptr) {
                // Found complete line
                std::size_t newline_pos = newline_ptr - buffer.data();

                if (!line_accumulator.empty()) {
                    // Complete a partial line from previous buffer
                    line_accumulator.append(buffer.data() + pos,
                                            newline_pos - pos);
                    processor.process(line_accumulator.c_str(),
                                      line_accumulator.length());
                    line_accumulator.clear();
                } else {
                    // Process complete line directly
                    processor.process(buffer.data() + pos, newline_pos - pos);
                }

                pos = newline_pos + 1;
            } else {
                // No newline found, accumulate remaining data
                line_accumulator.append(buffer.data() + pos, bytes_read - pos);
                break;
            }
        }
    }

    // Process any remaining data in line_accumulator
    if (!line_accumulator.empty()) {
        processor.process(line_accumulator.c_str(), line_accumulator.length());
    }

    processor.end();
}

bool GzipReader::is_valid() const { return is_open && indexer.get(); }

std::string GzipReader::get_format_name() const { return "GZIP"; }

std::size_t GzipReader::process_lines(
    const char *buffer_data, std::size_t buffer_size, std::size_t &current_line,
    std::size_t start_line, std::size_t end_line, std::string &line_accumulator,
    LineProcessor &processor) {
    std::size_t pos = 0;

    while (pos < buffer_size && current_line <= end_line) {
        const char *newline_ptr = static_cast<const char *>(
            std::memchr(buffer_data + pos, '\n', buffer_size - pos));

        if (newline_ptr != nullptr) {
            std::size_t newline_pos = newline_ptr - buffer_data;

            if (current_line >= start_line) {
                if (!line_accumulator.empty()) {
                    line_accumulator.append(buffer_data + pos,
                                            newline_pos - pos);
                    if (!processor.process(line_accumulator.c_str(),
                                           line_accumulator.length())) {
                        return pos;
                    }
                    line_accumulator.clear();
                } else {
                    if (!processor.process(buffer_data + pos,
                                           newline_pos - pos)) {
                        return pos;
                    }
                }
            } else {
                line_accumulator.clear();
            }

            current_line++;
            pos = newline_pos + 1;
        } else {
            line_accumulator.append(buffer_data + pos, buffer_size - pos);
            break;
        }
    }

    return pos;
}

}  // namespace dftracer::utils
