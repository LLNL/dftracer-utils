#include <dftracer/utils/core/common/config.h>
#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/pipeline/executors/thread_executor.h>
#include <dftracer/utils/core/pipeline/pipeline.h>
#include <dftracer/utils/core/common/filesystem.h>
#include <zlib.h>

#include <argparse/argparse.hpp>
#include <atomic>
#include <chrono>
#include <fstream>
#include <vector>

using namespace dftracer::utils;

struct CompressedResult {
    std::string file_path;
    bool success;
    std::size_t original_size;
    std::size_t compressed_size;
};

static CompressedResult compress_file(const std::string& input_path,
                                      TaskContext&) {
    CompressedResult result{input_path, false, 0, 0};

    std::ifstream infile(input_path, std::ios::binary);
    if (!infile) {
        DFTRACER_UTILS_LOG_ERROR("Cannot open file: %s", input_path.c_str());
        return result;
    }

    // Get file size
    infile.seekg(0, std::ios::end);
    result.original_size = infile.tellg();
    infile.seekg(0, std::ios::beg);

    // Use deflate stream with gzip wrapper for maximum efficiency
    std::string output_path = input_path + ".gz";
    std::ofstream outfile(output_path, std::ios::binary);
    if (!outfile) {
        DFTRACER_UTILS_LOG_ERROR("Cannot create output file: %s",
                                 output_path.c_str());
        return result;
    }

    // Initialize deflate stream with gzip wrapper (15 + 16)
    z_stream strm{};
    if (deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK) {
        DFTRACER_UTILS_LOG_ERROR("Failed to initialize deflate for: %s",
                                 input_path.c_str());
        return result;
    }

    constexpr std::size_t BUFFER_SIZE = 64 * 1024;  // 64KB chunks
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

            int ret = deflate(&strm, flush);
            if (ret == Z_STREAM_ERROR) {
                deflateEnd(&strm);
                DFTRACER_UTILS_LOG_ERROR("Deflate stream error for: %s",
                                         input_path.c_str());
                fs::remove(output_path);
                return result;
            }

            std::size_t bytes_to_write = BUFFER_SIZE - strm.avail_out;
            outfile.write(reinterpret_cast<const char*>(out_buffer.data()),
                          bytes_to_write);
            if (!outfile) {
                deflateEnd(&strm);
                DFTRACER_UTILS_LOG_ERROR("Write error for: %s",
                                         output_path.c_str());
                fs::remove(output_path);
                return result;
            }
        } while (strm.avail_out == 0);

    } while (flush != Z_FINISH);

    deflateEnd(&strm);
    infile.close();
    outfile.close();

    if (!outfile) {
        DFTRACER_UTILS_LOG_ERROR("Failed to close gzip file: %s",
                                 output_path.c_str());
        fs::remove(output_path);
        return result;
    }

    if (fs::exists(output_path)) {
        result.compressed_size = fs::file_size(output_path);
        try {
            fs::remove(input_path);
            result.success = true;
        } catch (const std::exception& e) {
            DFTRACER_UTILS_LOG_ERROR("Failed to remove original file %s: %s",
                                     input_path.c_str(), e.what());
            result.success = true;
        }
    }

    return result;
}

static std::vector<CompressedResult> process_files_parallel(
    const std::vector<std::string>& files, TaskContext& ctx) {
    std::vector<TaskResult<CompressedResult>::Future> futures;
    futures.reserve(files.size());

    for (const auto& file_path : files) {
        auto task_result = ctx.emit<std::string, CompressedResult>(
            compress_file, Input{file_path});
        futures.push_back(std::move(task_result.future()));
    }

    std::vector<CompressedResult> results;
    results.reserve(files.size());

    for (auto& future : futures) {
        results.push_back(future.get());
    }

    return results;
}

int main(int argc, char** argv) {
    DFTRACER_UTILS_LOGGER_INIT();

    argparse::ArgumentParser program("dftracer_pgzip",
                                     DFTRACER_UTILS_PACKAGE_VERSION);
    program.add_description(
        "Parallel gzip compression for DFTracer .pfw files");

    program.add_argument("-d", "--directory")
        .help("Directory containing .pfw files")
        .default_value<std::string>(".");

    program.add_argument("-v", "--verbose")
        .help("Enable verbose output")
        .flag();

    program.add_argument("--threads")
        .help("Number of threads for parallel processing")
        .scan<'d', std::size_t>()
        .default_value(
            static_cast<std::size_t>(std::thread::hardware_concurrency()));

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception& err) {
        DFTRACER_UTILS_LOG_ERROR("Error occurred: %s", err.what());
        std::cerr << program;
        return 1;
    }

    std::string input_dir = program.get<std::string>("--directory");
    bool verbose = program.get<bool>("--verbose");
    std::size_t num_threads = program.get<std::size_t>("--threads");

    input_dir = fs::absolute(input_dir).string();

    // Find .pfw files
    std::vector<std::string> pfw_files;
    for (const auto& entry : fs::directory_iterator(input_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".pfw") {
            pfw_files.push_back(entry.path().string());
        }
    }

    if (pfw_files.empty()) {
        DFTRACER_UTILS_LOG_ERROR("No .pfw files found in directory: %s",
                                 input_dir.c_str());
        return 1;
    }

    DFTRACER_UTILS_LOG_INFO("Found %zu .pfw files to compress",
                            pfw_files.size());
    if (verbose) {
        DFTRACER_UTILS_LOG_INFO("Using %zu threads for processing",
                                num_threads);
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    Pipeline pipeline;
    auto task_result =
        pipeline
            .add_task<std::vector<std::string>, std::vector<CompressedResult>>(
                process_files_parallel);

    ThreadExecutor executor(num_threads);
    executor.execute(pipeline, pfw_files);
    std::vector<CompressedResult> results = task_result.get();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end_time - start_time;

    // Report results
    std::size_t successful = 0;
    std::size_t total_original_size = 0;
    std::size_t total_compressed_size = 0;

    for (const auto& result : results) {
        if (result.success) {
            successful++;
            total_original_size += result.original_size;
            total_compressed_size += result.compressed_size;
            if (verbose) {
                double ratio =
                    total_original_size > 0
                        ? (double)result.compressed_size /
                              static_cast<double>(result.original_size) * 100.0
                        : 0.0;
                DFTRACER_UTILS_LOG_INFO(
                    "Compressed %s: %zu -> %zu bytes (%.1f%%)",
                    fs::path(result.file_path).filename().c_str(),
                    result.original_size, result.compressed_size, ratio);
            }
        } else {
            DFTRACER_UTILS_LOG_ERROR("Failed to compress: %s",
                                     result.file_path.c_str());
        }
    }

    double overall_ratio = total_original_size > 0
                               ? static_cast<double>(total_compressed_size) /
                                     static_cast<double>(total_original_size) *
                                     100.0
                               : 0.0;

    printf("Gzip Completed. Processed %zu/%zu files in %.2f ms\n", successful,
           results.size(), duration.count());
    printf("Total: %zu -> %zu bytes (%.1f%% compression ratio)\n",
           total_original_size, total_compressed_size, overall_ratio);

    return successful == results.size() ? 0 : 1;
}
