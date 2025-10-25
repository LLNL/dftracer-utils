#include <dftracer/utils/components/io/lines/streaming_line_reader.h>
#include <dftracer/utils/components/workflows/dft/metadata_collector.h>
#include <dftracer/utils/components/workflows/indexed_file_reader.h>
#include <dftracer/utils/core/common/filesystem.h>
#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/utils/string.h>

#include <atomic>

namespace dftracer::utils::components::workflows::dft {

using namespace io::lines;

DFTracerMetadataOutput DFTracerMetadataCollector::process(
    const DFTracerMetadataInput& input) {
    DFTracerMetadataOutput meta;
    meta.file_path = input.file_path;
    meta.success = false;

    try {
        // Auto-detect file format based on extension
        std::string file_path = input.file_path;
        bool is_compressed = (file_path.size() > 3 &&
                              file_path.substr(file_path.size() - 3) == ".gz");

        if (is_compressed) {
            // Compressed file - generate index path if not provided
            DFTracerMetadataInput modified_input = input;
            if (modified_input.idx_path.empty()) {
                // Auto-generate index path
                modified_input.idx_path = file_path + ".idx";
            }
            meta.idx_path = modified_input.idx_path;
            return process_compressed(modified_input);
        } else {
            // Plain text file
            return process_plain(input);
        }
    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Failed to collect metadata for %s: %s",
                                 input.file_path.c_str(), e.what());
        return meta;
    }
}

DFTracerMetadataOutput DFTracerMetadataCollector::process_compressed(
    const DFTracerMetadataInput& input) {
    DFTracerMetadataOutput meta;
    meta.file_path = input.file_path;
    meta.idx_path = input.idx_path;

    // Use IndexedFileReader workflow to manage index building/validation
    IndexedFileReader indexed_reader_workflow;
    auto reader_input =
        IndexedReadInput{input.file_path, input.idx_path, input.checkpoint_size,
                         input.force_rebuild};
    auto reader = indexed_reader_workflow.process(reader_input);

    std::size_t total_lines = reader->get_num_lines();
    if (total_lines == 0) {
        DFTRACER_UTILS_LOG_DEBUG("File %s has no lines",
                                 input.file_path.c_str());
        return meta;
    }

    // Use StreamingLineReader to iterate through lines
    auto line_range =
        StreamingLineReader::read_from_reader(reader, 1, total_lines);

    std::size_t total_bytes = 0;
    std::size_t valid_events = 0;

    while (line_range.has_next()) {
        Line line = line_range.next();
        const char* trimmed;
        std::size_t trimmed_length;
        if (json_trim_and_validate(line.content.c_str(), line.content.length(),
                                   trimmed, trimmed_length) &&
            trimmed_length > 8) {
            total_bytes += line.content.length();
            valid_events++;
        }
    }

    meta.size_mb = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
    meta.start_line = 1;
    meta.end_line = total_lines;
    meta.valid_events = valid_events;
    meta.size_per_line = (valid_events > 0)
                             ? meta.size_mb / static_cast<double>(valid_events)
                             : 0;
    meta.success = true;

    DFTRACER_UTILS_LOG_DEBUG(
        "File %s: %.2f MB, %zu valid events from %zu lines, %.8f MB/event",
        input.file_path.c_str(), meta.size_mb, meta.valid_events, total_lines,
        meta.size_per_line);

    return meta;
}

DFTracerMetadataOutput DFTracerMetadataCollector::process_plain(
    const DFTracerMetadataInput& input) {
    DFTracerMetadataOutput meta;
    meta.file_path = input.file_path;
    meta.idx_path = "";

    // Use StreamingLineReader for plain text files
    auto line_range = StreamingLineReader::read_plain(input.file_path);

    std::size_t total_lines = 0;
    std::size_t total_bytes = 0;
    std::size_t valid_events = 0;

    while (line_range.has_next()) {
        Line line = line_range.next();
        total_lines++;
        const char* trimmed;
        std::size_t trimmed_length;
        if (json_trim_and_validate(line.content.c_str(), line.content.length(),
                                   trimmed, trimmed_length) &&
            trimmed_length > 8) {
            total_bytes += line.content.length();
            valid_events++;
        }
    }

    meta.size_mb = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
    meta.start_line = 1;
    meta.end_line = total_lines;
    meta.valid_events = valid_events;
    meta.size_per_line = (valid_events > 0)
                             ? meta.size_mb / static_cast<double>(valid_events)
                             : 0;
    meta.success = true;

    DFTRACER_UTILS_LOG_DEBUG(
        "File %s: %.2f MB, %zu valid events from %zu lines, %.8f MB/event",
        input.file_path.c_str(), meta.size_mb, valid_events, total_lines,
        meta.size_per_line);

    return meta;
}

}  // namespace dftracer::utils::components::workflows::dft
