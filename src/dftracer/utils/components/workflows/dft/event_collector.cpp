#include <dftracer/utils/components/workflows/dft/event_collector.h>
#include <dftracer/utils/core/common/filesystem.h>
#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/utils/string.h>
#include <dftracer/utils/indexer/indexer_factory.h>
#include <dftracer/utils/reader/reader_factory.h>
#include <yyjson.h>

#include <algorithm>
#include <fstream>

namespace dftracer::utils::components::workflows::dft {

/**
 * @brief LineProcessor that collects EventIds from JSON events.
 */
class EventIdCollector : public LineProcessor {
   public:
    std::vector<EventId>& events;

    explicit EventIdCollector(std::vector<EventId>& event_list)
        : events(event_list) {}

    bool process(const char* data, std::size_t length) override {
        const char* trimmed;
        std::size_t trimmed_length;
        if (!json_trim_and_validate(data, length, trimmed, trimmed_length) ||
            trimmed_length <= 8) {
            return true;
        }

        yyjson_doc* doc = yyjson_read(trimmed, trimmed_length, 0);
        if (!doc) return true;

        yyjson_val* root = yyjson_doc_get_root(doc);
        if (!yyjson_is_obj(root)) {
            yyjson_doc_free(doc);
            return true;
        }

        EventId event;
        yyjson_val* id_val = yyjson_obj_get(root, "id");
        if (id_val && yyjson_is_int(id_val)) {
            event.id = yyjson_get_int(id_val);
        }

        yyjson_val* pid_val = yyjson_obj_get(root, "pid");
        if (pid_val && yyjson_is_int(pid_val)) {
            event.pid = yyjson_get_int(pid_val);
        }

        yyjson_val* tid_val = yyjson_obj_get(root, "tid");
        if (tid_val && yyjson_is_int(tid_val)) {
            event.tid = yyjson_get_int(tid_val);
        }

        if (event.is_valid()) {
            events.push_back(event);
        }

        yyjson_doc_free(doc);
        return true;
    }
};

EventCollectionOutput EventCollectorFromMetadata::process(
    const EventCollectionFromMetadataInput& input) {
    std::vector<EventId> events;

    for (const auto& file : input.metadata) {
        if (!file.success) continue;

        EventIdCollector collector(events);

        if (!file.idx_path.empty()) {
            // Indexed/compressed file
            auto reader = ReaderFactory::create(file.file_path, file.idx_path);
            reader->read_lines_with_processor(file.start_line, file.end_line,
                                              collector);
        } else {
            // Plain text file
            std::ifstream infile(file.file_path);
            if (!infile.is_open()) {
                DFTRACER_UTILS_LOG_WARN("Cannot open file: %s",
                                        file.file_path.c_str());
                continue;
            }

            std::string line;
            std::size_t current_line = 0;
            while (std::getline(infile, line)) {
                current_line++;
                if (current_line < file.start_line) continue;
                if (current_line > file.end_line) break;
                collector.process(line.c_str(), line.length());
            }
        }
    }

    // Sort events for consistent hashing
    std::sort(events.begin(), events.end());

    return events;
}

EventCollectionOutput EventCollectorFromChunks::process(
    const EventCollectionFromChunksInput& input) {
    std::vector<EventId> events;

    for (const auto& chunk : input.chunks) {
        if (!chunk.success) continue;

        EventIdCollector collector(events);

        bool is_compressed =
            (chunk.output_path.size() > 3 &&
             chunk.output_path.substr(chunk.output_path.size() - 3) == ".gz");

        if (is_compressed) {
            // Compressed chunk - need to create index first
            fs::path tmp_idx =
                fs::temp_directory_path() /
                (fs::path(chunk.output_path).filename().string() + ".idx");

            if (fs::exists(tmp_idx)) {
                fs::remove(tmp_idx);
            }

            std::size_t checkpoint_size =
                input.checkpoint_size > 0 ? input.checkpoint_size
                                          : Indexer::DEFAULT_CHECKPOINT_SIZE;

            auto indexer = IndexerFactory::create(
                chunk.output_path, tmp_idx.string(), checkpoint_size, true);
            indexer->build();

            auto reader = ReaderFactory::create(indexer);
            std::size_t num_lines = reader->get_num_lines();
            if (num_lines > 0) {
                reader->read_lines_with_processor(1, num_lines, collector);
            }

            if (fs::exists(tmp_idx)) {
                fs::remove(tmp_idx);
            }
        } else {
            // Uncompressed chunk
            std::ifstream chunk_file(chunk.output_path);
            if (!chunk_file.is_open()) {
                DFTRACER_UTILS_LOG_WARN("Cannot open chunk file: %s",
                                        chunk.output_path.c_str());
                continue;
            }

            std::string line;
            while (std::getline(chunk_file, line)) {
                collector.process(line.c_str(), line.length());
            }
        }
    }

    // Sort events for consistent hashing
    std::sort(events.begin(), events.end());

    return events;
}

}  // namespace dftracer::utils::components::workflows::dft
