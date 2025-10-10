#include <dftracer/utils/common/config.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/task_tag.h>
#include <dftracer/utils/reader/line_processor.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/reader/reader_factory.h>
#include <dftracer/utils/utils/filesystem.h>
#include <dftracer/utils/utils/string.h>
#include <xxhash.h>
#include <zlib.h>

#include <algorithm>
#include <argparse/argparse.hpp>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>
#include <vector>

using namespace dftracer::utils;

struct FileMetadata {
    std::string file_path;
    std::string idx_path;
    double size_mb;
    std::size_t start_line;
    std::size_t end_line;
    std::size_t valid_events;
    double size_per_line;
    bool success;
};

struct ChunkSpec {
    std::string file_path;
    std::string idx_path;
    double size_mb;
    std::size_t start_line;
    std::size_t end_line;
};

struct ChunkData {
    int chunk_index;
    std::vector<ChunkSpec> specs;
    double total_size_mb;
};

struct ChunkResult {
    int chunk_index;
    std::string output_path;
    double size_mb;
    std::size_t events;
    bool success;
};

class SizeEstimator : public LineProcessor {
   public:
    std::atomic<std::size_t> total_bytes{0};
    std::atomic<std::size_t> valid_lines{0};

    bool process(const char* data, std::size_t length) override {
        const char* trimmed;
        std::size_t trimmed_length;
        if (json_trim_and_validate(data, length, trimmed, trimmed_length) &&
            trimmed_length > 8) {
            total_bytes += length;
            valid_lines++;
        }
        return true;
    }
};

struct EventId {
    char prefix[256];

    bool operator<(const EventId& other) const {
        return std::strcmp(prefix, other.prefix) < 0;
    }
};

class EventIdCollector : public LineProcessor {
   public:
    std::vector<EventId>& events;

    EventIdCollector(std::vector<EventId>& event_list) : events(event_list) {}

    bool process(const char* data, std::size_t length) override {
        const char* trimmed;
        std::size_t trimmed_length;
        if (!json_trim_and_validate(data, length, trimmed, trimmed_length) ||
            trimmed_length <= 8) {
            return true;
        }

        EventId event;
        std::size_t copy_len =
            std::min(trimmed_length, sizeof(event.prefix) - 1);
        std::memcpy(event.prefix, trimmed, copy_len);
        event.prefix[copy_len] = '\0';

        events.push_back(event);
        return true;
    }
};

static std::uint64_t compute_event_hash(
    const std::vector<FileMetadata>& files) {
    std::vector<EventId> events;

    for (const auto& file : files) {
        if (!file.success) continue;

        if (!file.idx_path.empty()) {
            auto reader = ReaderFactory::create(file.file_path, file.idx_path);
            EventIdCollector collector(events);
            reader->read_lines_with_processor(file.start_line, file.end_line,
                                              collector);
        } else {
            std::ifstream infile(file.file_path);
            if (!infile.is_open()) continue;

            EventIdCollector collector(events);
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

    std::sort(events.begin(), events.end());

    XXH3_state_t* state = XXH3_createState();
    if (!state) {
        DFTRACER_UTILS_LOG_ERROR("%s", "Failed to create XXH3 state");
        return 0;
    }
    XXH3_64bits_reset_withSeed(state, 0);

    for (const auto& event : events) {
        XXH3_64bits_update(state, event.prefix, std::strlen(event.prefix));
    }

    std::uint64_t hash = XXH3_64bits_digest(state);
    XXH3_freeState(state);
    return hash;
}

static std::vector<EventId> collect_output_events(const ChunkResult& result,
                                                  std::size_t checkpoint_size,
                                                  TaskContext&) {
    std::vector<EventId> events;
    if (!result.success) return events;

    bool is_compressed =
        (result.output_path.size() > 3 &&
         result.output_path.substr(result.output_path.size() - 3) == ".gz");
    EventIdCollector collector(events);

    if (is_compressed) {
        fs::path tmp_idx =
            fs::temp_directory_path() /
            (fs::path(result.output_path).filename().string() + ".idx");

        if (fs::exists(tmp_idx)) {
            fs::remove(tmp_idx);
        }

        auto indexer = IndexerFactory::create(
            result.output_path, tmp_idx.string(), checkpoint_size, true);
        indexer->build();

        auto reader = ReaderFactory::create(indexer.get());
        std::size_t num_lines = reader->get_num_lines();
        if (num_lines > 0) {
            reader->read_lines_with_processor(1, num_lines, collector);
        }

        if (fs::exists(tmp_idx)) {
            fs::remove(tmp_idx);
        }
    } else {
        std::ifstream chunk_file(result.output_path);
        if (!chunk_file.is_open()) {
            DFTRACER_UTILS_LOG_ERROR("Cannot open chunk file: %s",
                                     result.output_path.c_str());
            return events;
        }

        std::string line;
        while (std::getline(chunk_file, line)) {
            collector.process(line.c_str(), line.length());
        }
    }
    return events;
}

static FileMetadata collect_metadata_gz(const std::string& gz_path,
                                        const std::string& idx_path,
                                        std::size_t checkpoint_size,
                                        bool force_rebuild, TaskContext&) {
    FileMetadata meta{gz_path, idx_path, 0, 0, 0, 0, 0, false};

    try {
        if (!fs::exists(idx_path) || force_rebuild) {
            if (force_rebuild && fs::exists(idx_path)) {
                fs::remove(idx_path);
            }
            DFTRACER_UTILS_LOG_DEBUG("Building index for %s", gz_path.c_str());
            auto indexer = IndexerFactory::create(gz_path, idx_path,
                                                  checkpoint_size, true);
            indexer->build();
        } else {
            auto indexer = IndexerFactory::create(gz_path, idx_path,
                                                  checkpoint_size, false);
            if (indexer->need_rebuild()) {
                DFTRACER_UTILS_LOG_DEBUG("Rebuilding index for %s",
                                         gz_path.c_str());
                fs::remove(idx_path);
                auto new_indexer = IndexerFactory::create(
                    gz_path, idx_path, checkpoint_size, true);
                new_indexer->build();
            }
        }

        auto reader = ReaderFactory::create(gz_path, idx_path);
        std::size_t total_lines = reader->get_num_lines();

        if (total_lines == 0) {
            DFTRACER_UTILS_LOG_DEBUG("File %s has no lines", gz_path.c_str());
            return meta;
        }

        SizeEstimator estimator;
        reader->read_lines_with_processor(1, total_lines, estimator);

        meta.size_mb = static_cast<double>(estimator.total_bytes.load()) /
                       (1024.0 * 1024.0);
        meta.start_line = 1;
        meta.end_line = total_lines;
        meta.valid_events = estimator.valid_lines.load();

        if (meta.valid_events > 0) {
            meta.size_per_line =
                meta.size_mb / static_cast<double>(meta.valid_events);
        } else {
            meta.size_per_line = 0;
        }

        meta.success = true;

        DFTRACER_UTILS_LOG_DEBUG(
            "File %s: %.2f MB, %zu valid events from %zu lines, %.8f MB/event",
            gz_path.c_str(), meta.size_mb, meta.valid_events, total_lines,
            meta.size_per_line);

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Failed to process %s: %s", gz_path.c_str(),
                                 e.what());
    }

    return meta;
}

static FileMetadata collect_metadata_pfw(const std::string& pfw_path,
                                         TaskContext&) {
    FileMetadata meta{pfw_path, "", 0, 0, 0, 0, 0, false};

    try {
        std::ifstream file(pfw_path);
        if (!file.is_open()) {
            DFTRACER_UTILS_LOG_ERROR("Cannot open file: %s", pfw_path.c_str());
            return meta;
        }

        std::string line;
        std::size_t total_lines = 0;
        std::size_t total_bytes = 0;
        std::size_t valid_events = 0;

        while (std::getline(file, line)) {
            total_lines++;
            const char* trimmed;
            std::size_t trimmed_length;
            if (json_trim_and_validate(line.c_str(), line.length(), trimmed,
                                       trimmed_length) &&
                trimmed_length > 8) {
                total_bytes += line.length();
                valid_events++;
            }
        }

        meta.size_mb = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
        meta.start_line = 1;
        meta.end_line = total_lines;
        meta.valid_events = valid_events;

        if (valid_events > 0) {
            meta.size_per_line =
                meta.size_mb / static_cast<double>(valid_events);
        } else {
            meta.size_per_line = 0;
        }

        meta.success = true;

        DFTRACER_UTILS_LOG_DEBUG(
            "File %s: %.2f MB, %zu valid events from %zu lines, %.8f MB/event",
            pfw_path.c_str(), meta.size_mb, valid_events, total_lines,
            meta.size_per_line);

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Error processing file %s: %s",
                                 pfw_path.c_str(), e.what());
    }

    return meta;
}

static std::vector<FileMetadata> collect_all_metadata(
    const std::vector<std::string>& files, std::size_t checkpoint_size,
    bool force_rebuild, const std::string& index_dir, TaskContext& p_ctx) {
    std::vector<TaskResult<FileMetadata>::Future> futures;
    futures.reserve(files.size());

    auto process_file = [checkpoint_size, force_rebuild, &index_dir](
                            std::string file_path,
                            TaskContext& ctx) -> FileMetadata {
        const std::string pfw_gz_suffix = ".pfw.gz";
        const std::string pfw_suffix = ".pfw";

        if (file_path.size() >= pfw_gz_suffix.size() &&
            file_path.compare(file_path.size() - pfw_gz_suffix.size(),
                              pfw_gz_suffix.size(), pfw_gz_suffix) == 0) {
            fs::path idx_dir = index_dir.empty() ? fs::temp_directory_path()
                                                 : fs::path(index_dir);
            std::string base_name = fs::path(file_path).filename().string();
            std::string idx_path = (idx_dir / (base_name + ".idx")).string();
            return collect_metadata_gz(file_path, idx_path, checkpoint_size,
                                       force_rebuild, ctx);
        } else if (file_path.size() >= pfw_suffix.size() &&
                   file_path.compare(file_path.size() - pfw_suffix.size(),
                                     pfw_suffix.size(), pfw_suffix) == 0) {
            return collect_metadata_pfw(file_path, ctx);
        } else {
            DFTRACER_UTILS_LOG_ERROR("Unknown file type: %s",
                                     file_path.c_str());
            return FileMetadata{file_path, "", 0, 0, 0, 0, 0, false};
        }
    };

    for (const auto& file_path : files) {
        auto task_result = p_ctx.emit<std::string, FileMetadata>(
            process_file, Input{file_path});
        futures.push_back(std::move(task_result.future()));
    }

    std::vector<FileMetadata> results;
    results.reserve(files.size());

    for (auto& future : futures) {
        results.push_back(future.get());
    }

    return results;
}

static std::vector<ChunkData> create_chunk_mappings(
    const std::vector<FileMetadata>& metadata, double chunk_size_mb) {
    std::vector<ChunkData> chunks;
    ChunkData current_chunk;
    current_chunk.chunk_index = 1;
    current_chunk.total_size_mb = 0;

    for (const auto& file : metadata) {
        if (!file.success || file.size_mb <= 0 || file.valid_events == 0)
            continue;

        std::size_t remaining_events = file.valid_events;
        std::size_t current_start = file.start_line;
        std::size_t total_lines = file.end_line - file.start_line + 1;

        while (remaining_events > 0) {
            double available_space =
                chunk_size_mb - current_chunk.total_size_mb;

            std::size_t events_that_fit = 0;
            if (available_space > 0 && file.size_per_line > 0) {
                events_that_fit = static_cast<std::size_t>(
                    std::floor(available_space / file.size_per_line));
            }

            // Always respect chunk size limit
            std::size_t events_to_take =
                (events_that_fit > 0)
                    ? std::min(remaining_events, events_that_fit)
                    : remaining_events;

            if (events_to_take == 0 && remaining_events > 0) {
                if (!current_chunk.specs.empty()) {
                    chunks.push_back(current_chunk);
                    current_chunk = ChunkData();
                    current_chunk.chunk_index =
                        static_cast<int>(chunks.size() + 1);
                    current_chunk.total_size_mb = 0;
                }
                continue;
            }

            double event_ratio = static_cast<double>(events_to_take) /
                                 static_cast<double>(file.valid_events);
            std::size_t lines_to_take = static_cast<std::size_t>(
                std::ceil(event_ratio * static_cast<double>(total_lines)));

            std::size_t available_lines = file.end_line - current_start + 1;
            if (lines_to_take > available_lines) {
                lines_to_take = available_lines;
            }

            double size_to_take =
                static_cast<double>(events_to_take) * file.size_per_line;

            ChunkSpec spec;
            spec.file_path = file.file_path;
            spec.idx_path = file.idx_path;
            spec.size_mb = size_to_take;
            spec.start_line = current_start;
            spec.end_line = current_start + lines_to_take - 1;
            if (spec.end_line > file.end_line) {
                spec.end_line = file.end_line;
            }

            current_chunk.specs.push_back(spec);
            current_chunk.total_size_mb += size_to_take;

            current_start = spec.end_line + 1;
            remaining_events -= events_to_take;

            if (current_chunk.total_size_mb >= chunk_size_mb * 0.95) {
                chunks.push_back(current_chunk);
                current_chunk = ChunkData();
                current_chunk.chunk_index = static_cast<int>(chunks.size() + 1);
                current_chunk.total_size_mb = 0;
            }
        }
    }

    if (!current_chunk.specs.empty()) {
        chunks.push_back(current_chunk);
    }

    return chunks;
}

static ChunkResult extract_chunk(const ChunkData& chunk,
                                 const std::string& output_dir,
                                 const std::string& app_name, bool compress,
                                 TaskContext&) {
    std::string output_path = output_dir + "/" + app_name + "-" +
                              std::to_string(chunk.chunk_index) + ".pfw";

    ChunkResult result{chunk.chunk_index, output_path, 0, 0, false};

    try {
        FILE* output_fp = std::fopen(output_path.c_str(), "w");
        if (!output_fp) {
            DFTRACER_UTILS_LOG_ERROR("Cannot open output file: %s",
                                     output_path.c_str());
            return result;
        }

        setvbuf(output_fp, nullptr, _IOFBF, 1024 * 1024);
        std::fprintf(output_fp, "[\n");

        std::size_t total_events = 0;
        XXH3_state_t* hash_state = XXH3_createState();
        if (!hash_state) {
            DFTRACER_UTILS_LOG_ERROR("Failed to create XXH3 state for chunk %d",
                                     chunk.chunk_index);
            std::fclose(output_fp);
            result.success = false;
            return result;
        }
        XXH3_64bits_reset_withSeed(hash_state, 0);

        for (const auto& spec : chunk.specs) {
            class ChunkWriter : public LineProcessor {
               public:
                FILE* fp;
                XXH3_state_t* hasher;
                std::atomic<std::size_t>& event_count;

                ChunkWriter(FILE* file, XXH3_state_t* hash_state,
                            std::atomic<std::size_t>& count)
                    : fp(file), hasher(hash_state), event_count(count) {}

                bool process(const char* data, std::size_t length) override {
                    const char* trimmed;
                    std::size_t trimmed_length;
                    if (json_trim_and_validate(data, length, trimmed,
                                               trimmed_length) &&
                        trimmed_length > 8) {
                        std::fwrite(trimmed, 1, trimmed_length, fp);
                        std::fwrite("\n", 1, 1, fp);
                        XXH3_64bits_update(hasher, trimmed, trimmed_length);
                        XXH3_64bits_update(hasher, "\n", 1);
                        event_count++;
                    }
                    return true;
                }
            };

            std::atomic<std::size_t> spec_events{0};

            if (!spec.idx_path.empty()) {
                auto reader =
                    ReaderFactory::create(spec.file_path, spec.idx_path);
                ChunkWriter writer(output_fp, hash_state, spec_events);
                reader->read_lines_with_processor(spec.start_line,
                                                  spec.end_line, writer);
            } else {
                std::ifstream infile(spec.file_path);
                if (!infile.is_open()) {
                    DFTRACER_UTILS_LOG_ERROR("Cannot open input file: %s",
                                             spec.file_path.c_str());
                    continue;
                }

                std::string line;
                std::size_t current_line = 0;

                while (std::getline(infile, line)) {
                    current_line++;
                    if (current_line < spec.start_line) continue;
                    if (current_line > spec.end_line) break;

                    const char* trimmed;
                    std::size_t trimmed_length;
                    if (json_trim_and_validate(line.c_str(), line.length(),
                                               trimmed, trimmed_length) &&
                        trimmed_length > 8) {
                        std::fwrite(trimmed, 1, trimmed_length, output_fp);
                        std::fwrite("\n", 1, 1, output_fp);
                        XXH3_64bits_update(hash_state, trimmed, trimmed_length);
                        XXH3_64bits_update(hash_state, "\n", 1);
                        spec_events++;
                    }
                }
            }

            total_events += spec_events.load();
        }

        std::fprintf(output_fp, "\n]\n");
        std::fclose(output_fp);

        XXH3_freeState(hash_state);

        result.events = total_events;
        result.size_mb = chunk.total_size_mb;

        if (compress && total_events > 0) {
            std::string compressed_path = output_path + ".gz";
            std::ifstream infile(output_path, std::ios::binary);
            std::ofstream outfile(compressed_path, std::ios::binary);

            if (infile && outfile) {
                z_stream strm{};
                if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                                 15 + 16, 8, Z_DEFAULT_STRATEGY) == Z_OK) {
                    constexpr std::size_t BUFFER_SIZE = 64 * 1024;
                    std::vector<unsigned char> in_buffer(BUFFER_SIZE);
                    std::vector<unsigned char> out_buffer(BUFFER_SIZE);

                    int flush = Z_NO_FLUSH;
                    do {
                        infile.read(reinterpret_cast<char*>(in_buffer.data()),
                                    BUFFER_SIZE);
                        std::streamsize bytes_read = infile.gcount();

                        if (bytes_read == 0) break;

                        strm.avail_in = static_cast<uInt>(bytes_read);
                        strm.next_in = in_buffer.data();
                        flush = infile.eof() ? Z_FINISH : Z_NO_FLUSH;

                        do {
                            strm.avail_out = BUFFER_SIZE;
                            strm.next_out = out_buffer.data();
                            deflate(&strm, flush);

                            std::size_t bytes_to_write =
                                BUFFER_SIZE - strm.avail_out;
                            outfile.write(reinterpret_cast<const char*>(
                                              out_buffer.data()),
                                          bytes_to_write);
                        } while (strm.avail_out == 0);
                    } while (flush != Z_FINISH);

                    deflateEnd(&strm);
                    infile.close();
                    outfile.close();

                    if (fs::exists(compressed_path)) {
                        fs::remove(output_path);
                        result.output_path = compressed_path;
                    }
                }
            }
        }

        result.success = true;

        DFTRACER_UTILS_LOG_DEBUG("Chunk %d: %zu events, %.2f MB written to %s",
                                 chunk.chunk_index, result.events,
                                 result.size_mb, result.output_path.c_str());

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Failed to extract chunk %d: %s",
                                 chunk.chunk_index, e.what());
    }

    return result;
}

static std::vector<ChunkResult> extract_all_chunks(
    const std::vector<ChunkData>& chunks, const std::string& output_dir,
    const std::string& app_name, bool compress, TaskContext& p_ctx) {
    std::vector<TaskResult<ChunkResult>::Future> futures;
    futures.reserve(chunks.size());

    auto extract_fn = [output_dir, app_name, compress](
                          ChunkData chunk, TaskContext& ctx) -> ChunkResult {
        return extract_chunk(chunk, output_dir, app_name, compress, ctx);
    };

    for (const auto& chunk : chunks) {
        auto task_result =
            p_ctx.emit<ChunkData, ChunkResult>(extract_fn, Input{chunk});
        futures.push_back(std::move(task_result.future()));
    }

    std::vector<ChunkResult> results;
    results.reserve(chunks.size());

    for (auto& future : futures) {
        results.push_back(future.get());
    }

    return results;
}

int main(int argc, char** argv) {
    DFTRACER_UTILS_LOGGER_INIT();

    auto default_checkpoint_size_str =
        std::to_string(Indexer::DEFAULT_CHECKPOINT_SIZE) + " B (" +
        std::to_string(Indexer::DEFAULT_CHECKPOINT_SIZE / (1024 * 1024)) +
        " MB)";

    argparse::ArgumentParser program("dftracer_split",
                                     DFTRACER_UTILS_PACKAGE_VERSION);
    program.add_description(
        "Split DFTracer traces into equal-sized chunks using pipeline "
        "processing");

    program.add_argument("-n", "--app-name")
        .help("Application name for output files")
        .default_value<std::string>("app");

    program.add_argument("-d", "--directory")
        .help("Input directory containing .pfw or .pfw.gz files")
        .default_value<std::string>(".");

    program.add_argument("-o", "--output")
        .help("Output directory for split files")
        .default_value<std::string>("./split");

    program.add_argument("-s", "--chunk-size")
        .help("Chunk size in MB")
        .scan<'d', int>()
        .default_value(4);

    program.add_argument("-f", "--force")
        .help("Override existing files and force index recreation")
        .flag();

    program.add_argument("-c", "--compress")
        .help("Compress output files with gzip")
        .flag()
        .default_value(true);

    program.add_argument("-v", "--verbose").help("Enable verbose mode").flag();

    program.add_argument("--checkpoint-size")
        .help("Checkpoint size for indexing in bytes (default: " +
              default_checkpoint_size_str + ")")
        .scan<'d', std::size_t>()
        .default_value(
            static_cast<std::size_t>(Indexer::DEFAULT_CHECKPOINT_SIZE));

    program.add_argument("--threads")
        .help(
            "Number of threads for parallel processing (default: number of CPU "
            "cores)")
        .scan<'d', std::size_t>()
        .default_value(
            static_cast<std::size_t>(std::thread::hardware_concurrency()));

    program.add_argument("--index-dir")
        .help("Directory to store index files (default: system temp directory)")
        .default_value<std::string>("");

    program.add_argument("--verify")
        .help("Verify output chunks match input by comparing event IDs")
        .flag();

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception& err) {
        DFTRACER_UTILS_LOG_ERROR("Error occurred: %s", err.what());
        std::cerr << program << std::endl;
        return 1;
    }

    std::string app_name = program.get<std::string>("--app-name");
    std::string log_dir = program.get<std::string>("--directory");
    std::string output_dir = program.get<std::string>("--output");
    int chunk_size_mb = program.get<int>("--chunk-size");
    bool force = program.get<bool>("--force");
    bool compress = program.get<bool>("--compress");
    bool verify = program.get<bool>("--verify");
    std::size_t checkpoint_size = program.get<std::size_t>("--checkpoint-size");
    std::size_t num_threads = program.get<std::size_t>("--threads");
    std::string index_dir = program.get<std::string>("--index-dir");

    log_dir = fs::absolute(log_dir).string();
    output_dir = fs::absolute(output_dir).string();

    std::printf("==========================================\n");
    std::printf("Arguments:\n");
    std::printf("  App name: %s\n", app_name.c_str());
    std::printf("  Override: %s\n", force ? "true" : "false");
    std::printf("  Compress: %s\n", compress ? "true" : "false");
    std::printf("  Data dir: %s\n", log_dir.c_str());
    std::printf("  Output dir: %s\n", output_dir.c_str());
    std::printf("  Chunk size: %d MB\n", chunk_size_mb);
    std::printf("  Threads: %zu\n", num_threads);
    std::printf("==========================================\n");

    if (!fs::exists(output_dir)) {
        fs::create_directories(output_dir);
    }

    std::vector<std::string> input_files;
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        if (entry.is_regular_file()) {
            std::string path = entry.path().string();
            const std::string pfw_gz_suffix = ".pfw.gz";
            const std::string pfw_suffix = ".pfw";

            if (path.size() >= pfw_gz_suffix.size() &&
                path.compare(path.size() - pfw_gz_suffix.size(),
                             pfw_gz_suffix.size(), pfw_gz_suffix) == 0) {
                input_files.push_back(path);
            } else if (path.size() >= pfw_suffix.size() &&
                       path.compare(path.size() - pfw_suffix.size(),
                                    pfw_suffix.size(), pfw_suffix) == 0) {
                input_files.push_back(path);
            }
        }
    }

    if (input_files.empty()) {
        DFTRACER_UTILS_LOG_ERROR(
            "No .pfw or .pfw.gz files found in directory: %s", log_dir.c_str());
        return 1;
    }

    DFTRACER_UTILS_LOG_INFO("Found %zu files to process", input_files.size());

    auto start_time = std::chrono::high_resolution_clock::now();

    // Phase 1: Collect metadata in parallel
    DFTRACER_UTILS_LOG_INFO("%s", "Phase 1: Collecting file metadata...");
    Pipeline metadata_pipeline;
    auto metadata_task =
        metadata_pipeline
            .add_task<std::vector<std::string>, std::vector<FileMetadata>>(
                [checkpoint_size, force, index_dir](
                    std::vector<std::string> file_list,
                    TaskContext& ctx) -> std::vector<FileMetadata> {
                    return collect_all_metadata(file_list, checkpoint_size,
                                                force, index_dir, ctx);
                });

    ThreadExecutor executor(num_threads);
    executor.execute(metadata_pipeline, input_files);
    std::vector<FileMetadata> all_metadata = metadata_task.get();

    std::size_t successful_files = 0;
    double total_size_mb = 0;
    for (const auto& meta : all_metadata) {
        if (meta.success) {
            successful_files++;
            total_size_mb += meta.size_mb;
        }
    }

    DFTRACER_UTILS_LOG_INFO(
        "Collected metadata from %zu/%zu files, total size: %.2f MB",
        successful_files, all_metadata.size(), total_size_mb);

    if (successful_files == 0) {
        DFTRACER_UTILS_LOG_ERROR("%s", "No files were successfully processed");
        return 1;
    }

    // Phase 2: Create chunk mappings
    DFTRACER_UTILS_LOG_INFO("%s", "Phase 2: Creating chunk mappings...");
    std::vector<ChunkData> chunks =
        create_chunk_mappings(all_metadata, static_cast<double>(chunk_size_mb));

    DFTRACER_UTILS_LOG_INFO("Created %zu chunks", chunks.size());

    if (chunks.empty()) {
        DFTRACER_UTILS_LOG_ERROR("%s", "No chunks created");
        return 1;
    }

    // Phase 3: Extract chunks in parallel
    DFTRACER_UTILS_LOG_INFO("%s", "Phase 3: Extracting chunks...");
    Pipeline extract_pipeline;
    auto extract_task =
        extract_pipeline
            .add_task<std::vector<ChunkData>, std::vector<ChunkResult>>(
                [output_dir, app_name, compress](
                    std::vector<ChunkData> chunk_list,
                    TaskContext& ctx) -> std::vector<ChunkResult> {
                    return extract_all_chunks(chunk_list, output_dir, app_name,
                                              compress, ctx);
                });

    executor.execute(extract_pipeline, chunks);
    std::vector<ChunkResult> results = extract_task.get();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end_time - start_time;

    std::sort(results.begin(), results.end(),
              [](const ChunkResult& a, const ChunkResult& b) {
                  return a.chunk_index < b.chunk_index;
              });

    std::size_t successful_chunks = 0;
    std::size_t total_events = 0;

    for (const auto& result : results) {
        if (result.success) {
            successful_chunks++;
            total_events += result.events;
        } else {
            DFTRACER_UTILS_LOG_ERROR("Failed to create chunk %d",
                                     result.chunk_index);
        }
    }

    std::printf("\n");
    std::printf("Split completed in %.2f seconds\n", duration.count() / 1000.0);
    std::printf("  Input: %zu files, %.2f MB\n", successful_files,
                total_size_mb);
    std::printf("  Output: %zu/%zu chunks, %zu events\n", successful_chunks,
                results.size(), total_events);

    if (verify) {
        auto verify_start = std::chrono::high_resolution_clock::now();
        std::printf("  Validating output chunks match input...\n");
        std::vector<FileMetadata> chunk_metadata;
        for (const auto& chunk : chunks) {
            for (const auto& spec : chunk.specs) {
                FileMetadata meta;
                meta.file_path = spec.file_path;
                meta.idx_path = spec.idx_path;
                meta.start_line = spec.start_line;
                meta.end_line = spec.end_line;
                meta.success = true;
                chunk_metadata.push_back(meta);
            }
        }
        std::uint64_t input_hash = compute_event_hash(chunk_metadata);

        Pipeline verify_pipeline;
        auto verify_task = verify_pipeline.add_task<
            std::vector<ChunkResult>, std::vector<std::vector<EventId>>>(
            [checkpoint_size](
                std::vector<ChunkResult> result_list,
                TaskContext& p_ctx) -> std::vector<std::vector<EventId>> {
                std::vector<TaskResult<std::vector<EventId>>::Future> futures;
                futures.reserve(result_list.size());

                for (const auto& result : result_list) {
                    auto task_result =
                        p_ctx.emit<ChunkResult, std::vector<EventId>>(
                            [checkpoint_size](
                                ChunkResult chunk_result,
                                TaskContext& ctx) -> std::vector<EventId> {
                                return collect_output_events(
                                    chunk_result, checkpoint_size, ctx);
                            },
                            Input{result});
                    futures.push_back(std::move(task_result.future()));
                }

                std::vector<std::vector<EventId>> all_events;
                all_events.reserve(result_list.size());
                for (auto& future : futures) {
                    all_events.push_back(future.get());
                }
                return all_events;
            });

        executor.execute(verify_pipeline, results);
        auto all_output_events = verify_task.get();

        std::vector<EventId> output_events;
        for (const auto& events : all_output_events) {
            output_events.insert(output_events.end(), events.begin(),
                                 events.end());
        }

        std::sort(output_events.begin(), output_events.end());

        XXH3_state_t* output_state = XXH3_createState();
        XXH3_64bits_reset_withSeed(output_state, 0);
        for (const auto& event : output_events) {
            XXH3_64bits_update(output_state, event.prefix,
                               std::strlen(event.prefix));
        }
        std::uint64_t output_hash = XXH3_64bits_digest(output_state);
        XXH3_freeState(output_state);

        auto verify_end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> verify_duration =
            verify_end - verify_start;

        if (input_hash == output_hash) {
            std::printf(
                "  \u2713 Verification: PASSED - all events present in output "
                "(%.2f seconds)\n",
                verify_duration.count() / 1000.0);
        } else {
            std::printf(
                "  \u2717 Verification: FAILED - event mismatch detected (%.2f "
                "seconds)\n",
                verify_duration.count() / 1000.0);
            DFTRACER_UTILS_LOG_ERROR(
                "Hash mismatch: input=%016llx output=%016llx",
                (unsigned long long)input_hash,
                (unsigned long long)output_hash);
        }
    }

    std::printf("All chunks processed in %.2f ms", duration.count());

    return successful_chunks == results.size() ? 0 : 1;
}
