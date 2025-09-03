#ifndef DFTRACER_UTILS_READER_STREAMS_BYTE_STREAM_H
#define DFTRACER_UTILS_READER_STREAMS_BYTE_STREAM_H

#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/reader/streams/stream.h>

#include <cstddef>
#include <cstdint>
#include <vector>

class ByteStream : public Stream {
   public:
    ByteStream() : Stream() {}

    virtual void initialize(const std::string &gz_path, std::size_t start_bytes,
                            std::size_t end_bytes, Indexer &indexer) override {
        DFTRACER_UTILS_LOG_DEBUG(
            "ByteStream::initialize - start_bytes=%zu, end_bytes=%zu",
            start_bytes, end_bytes);
        Stream::initialize(gz_path, start_bytes, end_bytes, indexer);
        current_position_ = start_bytes;
        size_t current_pos = checkpoint_.uc_offset;
        DFTRACER_UTILS_LOG_DEBUG(
            "ByteStream::initialize - checkpoint uc_offset=%zu, "
            "using_checkpoint=%s",
            current_pos, use_checkpoint_ ? "true" : "false");
        if (start_bytes > current_pos) {
            DFTRACER_UTILS_LOG_DEBUG(
                "ByteStream::initialize - skipping %zu bytes to reach "
                "start_bytes",
                start_bytes - current_pos);
            skip(start_bytes);
        }
        DFTRACER_UTILS_LOG_DEBUG(
            "ByteStream::initialize - completed, current_position_=%zu",
            current_position_);
    }

    virtual std::size_t stream(char *buffer, std::size_t buffer_size) override {
#ifdef __GNUC__
        __builtin_prefetch(buffer, 1, 3);
#endif

        if (!decompression_initialized_) {
            throw ReaderError(ReaderError::INITIALIZATION_ERROR,
                              "Raw streaming session not properly initialized");
        }

        if (is_at_target_end()) {
            is_finished_ = true;
            return 0;
        }

        size_t max_read = target_end_bytes_ - current_position_;
        size_t read_size = std::min(buffer_size, max_read);

        size_t bytes_read;
        DFTRACER_UTILS_LOG_DEBUG(
            "ByteStream::stream - about to read: read_size=%zu, "
            "current_position_=%zu",
            read_size, current_position_);
        bool result = inflater_.read(file_handle_,
                                     reinterpret_cast<unsigned char *>(buffer),
                                     read_size, bytes_read);

        DFTRACER_UTILS_LOG_DEBUG(
            "ByteStream::stream - read result: result=%d, bytes_read=%zu",
            result, bytes_read);
        if (!result || bytes_read == 0) {
            DFTRACER_UTILS_LOG_DEBUG(
                "ByteStream::stream - marking as finished due to read failure "
                "or 0 bytes",
                "");
            is_finished_ = true;
            return 0;
        }

        current_position_ += bytes_read;

        DFTRACER_UTILS_LOG_DEBUG("Streamed %zu bytes (position: %zu / %zu)",
                                 bytes_read, current_position_,
                                 target_end_bytes_);

        return bytes_read;
    }
};

#endif  // DFTRACER_UTILS_READER_STREAMS_BYTE_STREAM_H
