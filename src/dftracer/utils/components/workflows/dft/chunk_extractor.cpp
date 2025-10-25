#include <dftracer/utils/components/io/lines/line_range.h>
#include <dftracer/utils/components/io/lines/streaming_line_reader.h>
#include <dftracer/utils/components/workflows/dft/chunk_extractor.h>
#include <dftracer/utils/core/common/filesystem.h>
#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/utils/string.h>
#include <dftracer/utils/reader/reader_factory.h>
#include <xxhash.h>
#include <zlib.h>

#include <cstdio>
#include <fstream>

namespace dftracer::utils::components::workflows::dft {

using namespace io::lines;

DFTracerChunkExtractionResult DFTracerChunkExtractor::process(
    const DFTracerChunkExtractionInput& input) {
    DFTracerChunkExtractionResult result;
    result.chunk_index = input.chunk_index;
    result.success = false;

    try {
        return extract_and_write(input);
    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Failed to extract chunk %d: %s",
                                 input.chunk_index, e.what());
        result.output_path = input.output_dir + "/" + input.app_name + "-" +
                             std::to_string(input.chunk_index) + ".pfw";
        return result;
    }
}

DFTracerChunkExtractionResult DFTracerChunkExtractor::extract_and_write(
    const DFTracerChunkExtractionInput& input) {
    std::string output_path = input.output_dir + "/" + input.app_name + "-" +
                              std::to_string(input.chunk_index) + ".pfw";

    DFTracerChunkExtractionResult result;
    result.chunk_index = input.chunk_index;
    result.output_path = output_path;
    result.size_mb = 0.0;
    result.events = 0;
    result.success = false;

    // Open output file
    FILE* output_fp = std::fopen(output_path.c_str(), "w");
    if (!output_fp) {
        DFTRACER_UTILS_LOG_ERROR("Cannot open output file: %s",
                                 output_path.c_str());
        return result;
    }

    // Set larger buffer for better performance
    setvbuf(output_fp, nullptr, _IOFBF, 1024 * 1024);

    // Write JSON array opening
    std::fprintf(output_fp, "[\n");

    // Initialize hash state for computing chunk hash
    XXH3_state_t* hash_state = XXH3_createState();
    if (!hash_state) {
        DFTRACER_UTILS_LOG_ERROR("Failed to create XXH3 state for chunk %d",
                                 input.chunk_index);
        std::fclose(output_fp);
        return result;
    }
    XXH3_64bits_reset_withSeed(hash_state, 0);

    std::size_t total_events = 0;

    // Process each chunk spec in the manifest
    for (const auto& spec : input.manifest.specs) {
        // Use line-based reading when line info is available for accurate
        // extraction
        if (spec.has_line_info()) {
            // Read using line numbers for precise extraction
            LineRange line_range = StreamingLineReader::read(
                spec.file_path, spec.start_line, spec.end_line);

            // Process all lines in the line range
            while (line_range.has_next()) {
                Line line = line_range.next();

                // Validate and filter JSON events
                const char* trimmed;
                std::size_t trimmed_length;
                if (json_trim_and_validate(line.content.c_str(),
                                           line.content.length(), trimmed,
                                           trimmed_length) &&
                    trimmed_length > 8) {
                    // Write valid JSON event
                    std::fwrite(trimmed, 1, trimmed_length, output_fp);
                    std::fwrite("\n", 1, 1, output_fp);

                    // Update hash
                    XXH3_64bits_update(hash_state, trimmed, trimmed_length);
                    XXH3_64bits_update(hash_state, "\n", 1);

                    total_events++;
                }
            }
        } else {
            // Fallback to byte-based reading when line info not available
            LineBytesRange line_range;

            if (!spec.idx_path.empty()) {
                // Compressed/indexed file - use byte-based reading with Reader
                line_range = StreamingLineReader::read_bytes_indexed(
                    spec.file_path, spec.idx_path, spec.start_byte,
                    spec.end_byte);
            } else {
                // Plain text file - use byte-based reading
                line_range = StreamingLineReader::read_bytes(
                    spec.file_path, spec.start_byte, spec.end_byte);
            }

            // Process all lines in the byte range
            while (line_range.has_next()) {
                Line line = line_range.next();

                // Validate and filter JSON events
                const char* trimmed;
                std::size_t trimmed_length;
                if (json_trim_and_validate(line.content.c_str(),
                                           line.content.length(), trimmed,
                                           trimmed_length) &&
                    trimmed_length > 8) {
                    // Write valid JSON event
                    std::fwrite(trimmed, 1, trimmed_length, output_fp);
                    std::fwrite("\n", 1, 1, output_fp);

                    // Update hash
                    XXH3_64bits_update(hash_state, trimmed, trimmed_length);
                    XXH3_64bits_update(hash_state, "\n", 1);

                    total_events++;
                }
            }
        }
    }

    // Write JSON array closing
    std::fprintf(output_fp, "\n]\n");
    std::fclose(output_fp);

    // Finalize hash
    XXH3_freeState(hash_state);

    result.events = total_events;
    result.size_mb = input.manifest.total_size_mb;

    // Compress if requested
    if (input.compress && total_events > 0) {
        std::string compressed_path = output_path + ".gz";
        if (compress_output(output_path, compressed_path)) {
            if (fs::exists(compressed_path)) {
                fs::remove(output_path);
                result.output_path = compressed_path;
            }
        }
    }

    result.success = true;

    DFTRACER_UTILS_LOG_DEBUG("Chunk %d: %zu events, %.2f MB written to %s",
                             input.chunk_index, result.events, result.size_mb,
                             result.output_path.c_str());

    return result;
}

bool DFTracerChunkExtractor::compress_output(const std::string& input_path,
                                             const std::string& output_path) {
    std::ifstream infile(input_path, std::ios::binary);
    std::ofstream outfile(output_path, std::ios::binary);

    if (!infile || !outfile) {
        DFTRACER_UTILS_LOG_ERROR("%s", "Cannot open files for compression");
        return false;
    }

    z_stream strm{};
    if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK) {
        DFTRACER_UTILS_LOG_ERROR("%s", "Failed to initialize zlib");
        return false;
    }

    constexpr std::size_t BUFFER_SIZE = 64 * 1024;
    std::vector<unsigned char> in_buffer(BUFFER_SIZE);
    std::vector<unsigned char> out_buffer(BUFFER_SIZE);

    int flush = Z_NO_FLUSH;
    do {
        infile.read(reinterpret_cast<char*>(in_buffer.data()), BUFFER_SIZE);
        std::streamsize bytes_read = infile.gcount();

        if (bytes_read == 0) break;

        strm.avail_in = static_cast<uInt>(bytes_read);
        strm.next_in = in_buffer.data();
        flush = infile.eof() ? Z_FINISH : Z_NO_FLUSH;

        do {
            strm.avail_out = BUFFER_SIZE;
            strm.next_out = out_buffer.data();
            deflate(&strm, flush);

            std::size_t bytes_to_write = BUFFER_SIZE - strm.avail_out;
            outfile.write(reinterpret_cast<const char*>(out_buffer.data()),
                          bytes_to_write);
        } while (strm.avail_out == 0);
    } while (flush != Z_FINISH);

    deflateEnd(&strm);
    infile.close();
    outfile.close();

    return true;
}

}  // namespace dftracer::utils::components::workflows::dft
