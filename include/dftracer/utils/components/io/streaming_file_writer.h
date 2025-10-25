#ifndef DFTRACER_UTILS_COMPONENTS_IO_STREAMING_FILE_WRITER_H
#define DFTRACER_UTILS_COMPONENTS_IO_STREAMING_FILE_WRITER_H

#include <dftracer/utils/components/io/chunk_iterator.h>
#include <dftracer/utils/components/io/shared.h>

#include <fstream>
#include <stdexcept>

namespace dftracer::utils::components::io {

/**
 * @brief Streaming file writer for lazy chunk iteration.
 *
 * This writer writes chunks as they are produced by an iterator,
 * maintaining constant memory usage.
 *
 * Usage pattern (true streaming):
 * @code
 * // Read, compress, and write in one pass - constant memory!
 * auto reader = std::make_shared<StreamingFileReader>();
 * gzip::StreamingCompressor compressor(9);
 * StreamingFileWriter writer("/output.gz");
 *
 * ChunkRange chunks = reader->process(StreamReadInput{"/large/file.txt"});
 * for (const auto& chunk : chunks) {
 *     // Compress chunk
 *     auto compressed = compressor.compress_chunk(chunk);
 *
 *     // Write immediately (constant memory!)
 *     for (const auto& out_chunk : compressed) {
 *         writer.write_chunk(RawData{out_chunk.data});
 *     }
 * }
 *
 * // Finalize compression and write remaining data
 * auto final = compressor.finalize();
 * for (const auto& chunk : final) {
 *     writer.write_chunk(RawData{chunk.data});
 * }
 *
 * writer.close();
 * std::cout << "Wrote " << writer.total_bytes() << " bytes\n";
 * @endcode
 */
class StreamingFileWriter {
   private:
    std::ofstream file_;
    std::size_t total_bytes_ = 0;
    std::size_t total_chunks_ = 0;
    fs::path path_;
    bool closed_ = false;

   public:
    /**
     * @brief Open file for streaming write.
     *
     * @param path Output file path
     * @param append Append to existing file (default: false)
     * @param create_dirs Create parent directories (default: true)
     */
    explicit StreamingFileWriter(fs::path path, bool append = false,
                                 bool create_dirs = true)
        : path_(std::move(path)) {
        // Create parent directories if requested
        if (create_dirs && path_.has_parent_path()) {
            fs::path parent = path_.parent_path();
            if (!fs::exists(parent)) {
                fs::create_directories(parent);
            }
        }

        // Open file
        std::ios::openmode mode = std::ios::binary;
        if (append) {
            mode |= std::ios::app;
        } else {
            mode |= std::ios::trunc;
        }

        file_.open(path_, mode);
        if (!file_) {
            throw std::runtime_error("Cannot open file for writing: " +
                                     path_.string());
        }
    }

    ~StreamingFileWriter() {
        if (!closed_) {
            close();
        }
    }

    // Non-copyable
    StreamingFileWriter(const StreamingFileWriter&) = delete;
    StreamingFileWriter& operator=(const StreamingFileWriter&) = delete;

    /**
     * @brief Write a single chunk immediately.
     *
     * @param chunk Data chunk to write
     */
    void write_chunk(const RawData& chunk) {
        if (closed_) {
            throw std::runtime_error("Cannot write to closed file");
        }

        if (chunk.empty()) {
            return;
        }

        file_.write(reinterpret_cast<const char*>(chunk.data.data()),
                    static_cast<std::streamsize>(chunk.size()));

        if (!file_) {
            throw std::runtime_error("Error writing to file: " +
                                     path_.string());
        }

        total_bytes_ += chunk.size();
        total_chunks_++;
    }

    /**
     * @brief Flush and close the file.
     */
    void close() {
        if (!closed_) {
            file_.close();
            closed_ = true;
        }
    }

    std::size_t total_bytes() const { return total_bytes_; }
    std::size_t total_chunks() const { return total_chunks_; }
    const fs::path& path() const { return path_; }
    bool is_closed() const { return closed_; }
};

}  // namespace dftracer::utils::components::io

#endif  // DFTRACER_UTILS_COMPONENTS_IO_STREAMING_FILE_WRITER_H
