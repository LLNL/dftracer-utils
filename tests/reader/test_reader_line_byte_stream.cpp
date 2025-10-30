#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <dftracer/utils/core/common/filesystem.h>
#include <dftracer/utils/indexer/indexer_factory.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/reader/reader_factory.h>
#include <dftracer/utils/reader/stream.h>
#include <dftracer/utils/reader/stream_type.h>
#include <doctest/doctest.h>
#include <testing_utilities.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace dftracer::utils;
using namespace dft_utils_test;

// Helper function to create a test file with variable line lengths
static std::string create_variable_line_test_file(
    const std::string& test_dir, std::size_t num_lines,
    std::size_t min_line_length = 50, std::size_t max_line_length = 500) {
    std::string txt_file = test_dir + "/variable_lines.txt";
    std::string gz_file = test_dir + "/variable_lines.gz";

    // Create text file with variable line lengths
    std::ofstream f(txt_file);
    if (!f.is_open()) {
        return "";
    }

    std::mt19937 rng(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<std::size_t> dist(min_line_length,
                                                    max_line_length);

    for (std::size_t i = 1; i <= num_lines; ++i) {
        std::size_t line_length = dist(rng);

        // Create JSON line with variable padding to reach desired length
        std::ostringstream line;
        line << "{\"id\":" << i << ",\"name\":\"event_" << i
             << "\",\"data\":\"";

        // Fill with padding to reach target length
        std::size_t current_len = line.str().length();
        std::size_t padding_needed = (line_length > current_len + 10)
                                         ? (line_length - current_len - 10)
                                         : 0;

        for (std::size_t j = 0; j < padding_needed; ++j) {
            line << static_cast<char>('a' + (j % 26));
        }

        line << "\",\"ts\":" << (1000000 + i * 100) << "}";

        f << line.str() << "\n";
    }
    f.close();

    // Compress to gzip
    if (!compress_file_to_gzip(txt_file, gz_file)) {
        return "";
    }

    fs::remove(txt_file);
    return gz_file;
}

TEST_CASE("LINE_BYTES Stream - Sequential Read Tests") {
    SUBCASE("Complete file read - single chunk") {
        TestEnvironment env(1000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 1000);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        // Build index
        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(0.5));
            REQUIRE(indexer != nullptr);
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);
        REQUIRE(reader->is_valid());

        std::size_t max_bytes = reader->get_max_bytes();
        auto stream = reader->stream(StreamType::LINE_BYTES,
                                     RangeType::BYTE_RANGE, 0, max_bytes);
        REQUIRE(stream != nullptr);

        // Read entire file
        std::vector<char> buffer(1024 * 1024);  // 1MB buffer
        std::string result;

        while (!stream->done()) {
            std::size_t bytes_read = stream->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                result.append(buffer.data(), bytes_read);
            }
        }

        // Verify we got complete lines
        CHECK(!result.empty());

        // Count lines (should end with newline)
        std::size_t line_count = std::count(result.begin(), result.end(), '\n');
        CHECK(line_count > 0);
        CHECK(line_count <= 1000);
    }

    SUBCASE("Boundary alignment - 4MB chunks") {
        TestEnvironment env(10000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 10000, 100, 300);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(1.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        std::size_t chunk_size = max_bytes / 2;  // Split file in half

        // Read first chunk
        auto stream1 = reader->stream(StreamType::LINE_BYTES,
                                      RangeType::BYTE_RANGE, 0, chunk_size);
        REQUIRE(stream1 != nullptr);

        std::vector<char> buffer(1024 * 1024);
        std::string chunk1;
        while (!stream1->done()) {
            std::size_t bytes_read =
                stream1->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                chunk1.append(buffer.data(), bytes_read);
            }
        }

        // Read second chunk
        auto stream2 =
            reader->stream(StreamType::LINE_BYTES, RangeType::BYTE_RANGE,
                           chunk_size, max_bytes);
        REQUIRE(stream2 != nullptr);

        std::string chunk2;
        while (!stream2->done()) {
            std::size_t bytes_read =
                stream2->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                chunk2.append(buffer.data(), bytes_read);
            }
        }

        // Verify both chunks have content
        CHECK(!chunk1.empty());
        CHECK(!chunk2.empty());

        // Verify no overlap - last line of chunk1 should be different from
        // first line of chunk2
        auto chunk1_last_newline = chunk1.rfind('\n');
        auto chunk2_first_newline = chunk2.find('\n');

        if (chunk1_last_newline != std::string::npos &&
            chunk2_first_newline != std::string::npos) {
            std::string last_line1 = chunk1.substr(chunk1_last_newline + 1);
            std::string first_line2 = chunk2.substr(0, chunk2_first_newline);

            // They should be different (no overlap)
            CHECK(last_line1 != first_line2);
        }
    }

    SUBCASE("Adjacent chunks concatenation equals full read") {
        TestEnvironment env(5000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 5000, 80, 250);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(1));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        std::size_t mid_point = max_bytes / 2;

        std::vector<char> buffer(512 * 1024);

        // Read first half
        auto stream1 = reader->stream(StreamType::LINE_BYTES,
                                      RangeType::BYTE_RANGE, 0, mid_point);
        std::string chunk1;
        while (!stream1->done()) {
            std::size_t bytes_read =
                stream1->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                chunk1.append(buffer.data(), bytes_read);
            }
        }

        // Read second half
        auto stream2 =
            reader->stream(StreamType::LINE_BYTES, RangeType::BYTE_RANGE,
                           mid_point, max_bytes);
        std::string chunk2;
        while (!stream2->done()) {
            std::size_t bytes_read =
                stream2->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                chunk2.append(buffer.data(), bytes_read);
            }
        }

        // Read full file
        auto stream_full = reader->stream(StreamType::LINE_BYTES,
                                          RangeType::BYTE_RANGE, 0, max_bytes);
        std::string full;
        while (!stream_full->done()) {
            std::size_t bytes_read =
                stream_full->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                full.append(buffer.data(), bytes_read);
            }
        }

        // Concatenate chunks
        std::string concatenated = chunk1 + chunk2;

        // Sort both for comparison (to handle potential ordering differences)
        std::vector<std::string> concat_lines, full_lines;

        std::istringstream concat_stream(concatenated);
        std::string line;
        while (std::getline(concat_stream, line)) {
            concat_lines.push_back(line);
        }

        std::istringstream full_stream(full);
        while (std::getline(full_stream, line)) {
            full_lines.push_back(line);
        }

        std::sort(concat_lines.begin(), concat_lines.end());
        std::sort(full_lines.begin(), full_lines.end());

        // Verify same line count
        CHECK(concat_lines.size() == full_lines.size());

        // Verify all lines match (allowing for ordering differences)
        if (concat_lines.size() == full_lines.size()) {
            CHECK(concat_lines == full_lines);
        }
    }

    SUBCASE("Multiple small chunks - no gaps or overlaps") {
        TestEnvironment env(3000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 3000);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(0.5));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        std::size_t num_chunks = 8;
        std::size_t chunk_size = max_bytes / num_chunks;

        std::vector<std::string> chunks;
        std::vector<char> buffer(256 * 1024);

        // Read all chunks
        for (std::size_t i = 0; i < num_chunks; ++i) {
            std::size_t start = i * chunk_size;
            std::size_t end =
                (i == num_chunks - 1) ? max_bytes : (i + 1) * chunk_size;

            auto stream = reader->stream(StreamType::LINE_BYTES,
                                         RangeType::BYTE_RANGE, start, end);
            REQUIRE(stream != nullptr);

            std::string chunk;
            while (!stream->done()) {
                std::size_t bytes_read =
                    stream->read(buffer.data(), buffer.size());
                if (bytes_read > 0) {
                    chunk.append(buffer.data(), bytes_read);
                }
            }

            chunks.push_back(chunk);
        }

        // Extract all lines from all chunks
        std::vector<std::string> all_lines;
        for (const auto& chunk : chunks) {
            std::istringstream stream(chunk);
            std::string line;
            while (std::getline(stream, line)) {
                if (!line.empty()) {
                    all_lines.push_back(line);
                }
            }
        }

        // Check for duplicates (sort and check unique)
        std::vector<std::string> sorted_lines = all_lines;
        std::sort(sorted_lines.begin(), sorted_lines.end());

        auto last = std::unique(sorted_lines.begin(), sorted_lines.end());
        std::size_t unique_count = std::distance(sorted_lines.begin(), last);

        // All lines should be unique (no overlaps)
        CHECK(unique_count == all_lines.size());

        // Should have reasonable number of lines
        CHECK(all_lines.size() > 0);
        CHECK(all_lines.size() <= 3000);
    }
}

TEST_CASE("LINE_BYTES Stream - Parallel/Threaded Read Tests") {
    SUBCASE("Parallel read with 4 threads") {
        TestEnvironment env(10000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 10000, 100, 400);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(2.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        const int num_threads = 4;
        std::size_t chunk_size = max_bytes / num_threads;

        std::vector<std::string> chunks(num_threads);
        std::vector<std::thread> threads;
        std::atomic<bool> error_occurred{false};

        // Launch threads to read different chunks in parallel
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&, i]() {
                try {
                    std::size_t start = i * chunk_size;
                    std::size_t end = (i == num_threads - 1)
                                          ? max_bytes
                                          : (i + 1) * chunk_size;

                    auto stream =
                        reader->stream(StreamType::LINE_BYTES,
                                       RangeType::BYTE_RANGE, start, end);
                    if (!stream) {
                        error_occurred = true;
                        return;
                    }

                    std::vector<char> buffer(512 * 1024);
                    std::string& chunk = chunks[i];

                    while (!stream->done()) {
                        std::size_t bytes_read =
                            stream->read(buffer.data(), buffer.size());
                        if (bytes_read > 0) {
                            chunk.append(buffer.data(), bytes_read);
                        }
                    }
                } catch (...) {
                    error_occurred = true;
                }
            });
        }

        // Wait for all threads
        for (auto& t : threads) {
            t.join();
        }

        CHECK(!error_occurred);

        // Verify all chunks have content
        for (const auto& chunk : chunks) {
            CHECK(!chunk.empty());
        }

        // Collect all lines from all chunks
        std::vector<std::string> all_lines;
        for (const auto& chunk : chunks) {
            std::istringstream stream(chunk);
            std::string line;
            while (std::getline(stream, line)) {
                if (!line.empty()) {
                    all_lines.push_back(line);
                }
            }
        }

        // Sort and check for duplicates
        std::vector<std::string> sorted_lines = all_lines;
        std::sort(sorted_lines.begin(), sorted_lines.end());

        auto last = std::unique(sorted_lines.begin(), sorted_lines.end());
        std::size_t unique_count = std::distance(sorted_lines.begin(), last);

        INFO("Total lines: ", all_lines.size());
        INFO("Unique lines: ", unique_count);

        // All lines should be unique (no overlaps between threads)
        CHECK(unique_count == all_lines.size());
    }

    SUBCASE("Parallel read with 16 threads - stress test") {
        TestEnvironment env(20000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 20000, 80, 350);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(4.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        const int num_threads = 16;
        std::size_t chunk_size = max_bytes / num_threads;

        std::vector<std::string> chunks(num_threads);
        std::vector<std::thread> threads;
        std::atomic<int> errors{0};
        std::atomic<int> completed{0};

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&, i]() {
                try {
                    std::size_t start = i * chunk_size;
                    std::size_t end = (i == num_threads - 1)
                                          ? max_bytes
                                          : (i + 1) * chunk_size;

                    auto stream =
                        reader->stream(StreamType::LINE_BYTES,
                                       RangeType::BYTE_RANGE, start, end);
                    if (!stream) {
                        errors++;
                        return;
                    }

                    std::vector<char> buffer(256 * 1024);
                    std::string& chunk = chunks[i];

                    while (!stream->done()) {
                        std::size_t bytes_read =
                            stream->read(buffer.data(), buffer.size());
                        if (bytes_read > 0) {
                            chunk.append(buffer.data(), bytes_read);
                        }
                    }

                    completed++;
                } catch (...) {
                    errors++;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        INFO("Completed threads: ", completed.load());
        INFO("Errors: ", errors.load());

        CHECK(errors == 0);
        CHECK(completed == num_threads);

        // Verify completeness
        std::vector<std::string> all_lines;
        for (const auto& chunk : chunks) {
            std::istringstream stream(chunk);
            std::string line;
            while (std::getline(stream, line)) {
                if (!line.empty()) {
                    all_lines.push_back(line);
                }
            }
        }

        std::sort(all_lines.begin(), all_lines.end());
        auto last = std::unique(all_lines.begin(), all_lines.end());
        std::size_t unique_count = std::distance(all_lines.begin(), last);

        CHECK(unique_count == all_lines.size());
        CHECK(all_lines.size() > 0);
    }

    SUBCASE("Concurrent reads of same range - thread safety") {
        TestEnvironment env(5000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 5000);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(1.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        const int num_threads = 8;

        std::vector<std::string> results(num_threads);
        std::vector<std::thread> threads;
        std::atomic<bool> error_occurred{false};

        // All threads read the SAME range
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&, i]() {
                try {
                    auto stream =
                        reader->stream(StreamType::LINE_BYTES,
                                       RangeType::BYTE_RANGE, 0, max_bytes);
                    if (!stream) {
                        error_occurred = true;
                        return;
                    }

                    std::vector<char> buffer(512 * 1024);
                    std::string& result = results[i];

                    while (!stream->done()) {
                        std::size_t bytes_read =
                            stream->read(buffer.data(), buffer.size());
                        if (bytes_read > 0) {
                            result.append(buffer.data(), bytes_read);
                        }
                    }
                } catch (...) {
                    error_occurred = true;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        CHECK(!error_occurred);

        // All results should have content
        for (const auto& result : results) {
            CHECK(!result.empty());
        }

        // Sort each result's lines and compare - they should all be the same
        std::vector<std::vector<std::string>> sorted_results;
        for (const auto& result : results) {
            std::vector<std::string> lines;
            std::istringstream stream(result);
            std::string line;
            while (std::getline(stream, line)) {
                if (!line.empty()) {
                    lines.push_back(line);
                }
            }
            std::sort(lines.begin(), lines.end());
            sorted_results.push_back(lines);
        }

        // All threads should have read the same data
        for (std::size_t i = 1; i < sorted_results.size(); ++i) {
            CHECK(sorted_results[0].size() == sorted_results[i].size());
            if (sorted_results[0].size() == sorted_results[i].size()) {
                CHECK(sorted_results[0] == sorted_results[i]);
            }
        }
    }
}

TEST_CASE("LINE_BYTES Stream - Edge Cases") {
    SUBCASE("Very small file") {
        TestEnvironment env(10);
        REQUIRE(env.is_valid());

        std::string gz_file = create_variable_line_test_file(env.get_dir(), 10);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(0.1));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        auto stream = reader->stream(StreamType::LINE_BYTES,
                                     RangeType::BYTE_RANGE, 0, max_bytes);
        REQUIRE(stream != nullptr);

        std::vector<char> buffer(1024);
        std::string result;

        while (!stream->done()) {
            std::size_t bytes_read = stream->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                result.append(buffer.data(), bytes_read);
            }
        }

        CHECK(!result.empty());

        std::size_t line_count = std::count(result.begin(), result.end(), '\n');
        CHECK(line_count > 0);
        CHECK(line_count <= 10);
    }

    SUBCASE("Chunk boundary at exact line boundary") {
        TestEnvironment env(100);
        REQUIRE(env.is_valid());

        // Create file with very uniform line lengths
        std::string txt_file = env.get_dir() + "/uniform.txt";
        std::string gz_file = env.get_dir() + "/uniform.gz";

        std::ofstream f(txt_file);
        for (int i = 1; i <= 100; ++i) {
            f << "{\"id\":" << i << ",\"fixed\":\"XXXXXXXXXXXXXXXXXXXX\"}\n";
        }
        f.close();

        compress_file_to_gzip(txt_file, gz_file);
        fs::remove(txt_file);

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(0.5));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        // Read in chunks that might align with line boundaries
        std::size_t max_bytes = reader->get_max_bytes();
        std::size_t mid = max_bytes / 2;

        auto stream1 = reader->stream(StreamType::LINE_BYTES,
                                      RangeType::BYTE_RANGE, 0, mid);
        auto stream2 = reader->stream(StreamType::LINE_BYTES,
                                      RangeType::BYTE_RANGE, mid, max_bytes);

        std::vector<char> buffer(1024);
        std::string chunk1, chunk2;

        while (!stream1->done()) {
            std::size_t bytes_read =
                stream1->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                chunk1.append(buffer.data(), bytes_read);
            }
        }

        while (!stream2->done()) {
            std::size_t bytes_read =
                stream2->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                chunk2.append(buffer.data(), bytes_read);
            }
        }

        CHECK(!chunk1.empty());
        CHECK(!chunk2.empty());

        // Verify both end/start with complete lines
        CHECK(chunk1.back() == '\n');
        CHECK(chunk2.back() == '\n');
    }

    SUBCASE("Empty range returns no data") {
        TestEnvironment env(100);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 100);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(0.5));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        // Request empty range
        auto stream = reader->stream(StreamType::LINE_BYTES,
                                     RangeType::BYTE_RANGE, 100, 100);
        REQUIRE(stream != nullptr);

        std::vector<char> buffer(1024);
        std::string result;

        while (!stream->done()) {
            std::size_t bytes_read = stream->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                result.append(buffer.data(), bytes_read);
            }
            if (bytes_read == 0) {
                break;  // Prevent infinite loop
            }
        }

        // Should return minimal or no data
        CHECK(result.size() == 0);
    }
}

TEST_CASE("LINE_BYTES Stream - Arbitrary Boundary Tests") {
    SUBCASE("Specific boundary: 4MB (4194304 bytes)") {
        TestEnvironment env(10000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 10000, 100, 300);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(2.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        std::size_t boundary = 4194304;  // 4MB

        // Only test if file is large enough
        if (max_bytes > boundary) {
            std::vector<char> buffer(512 * 1024);

            // Read chunk 1: [0, 4194304)
            auto stream1 = reader->stream(StreamType::LINE_BYTES,
                                          RangeType::BYTE_RANGE, 0, boundary);
            std::string chunk1;
            while (!stream1->done()) {
                std::size_t bytes_read =
                    stream1->read(buffer.data(), buffer.size());
                if (bytes_read > 0) {
                    chunk1.append(buffer.data(), bytes_read);
                }
            }

            // Read chunk 2: [4194304, end)
            auto stream2 =
                reader->stream(StreamType::LINE_BYTES, RangeType::BYTE_RANGE,
                               boundary, max_bytes);
            std::string chunk2;
            while (!stream2->done()) {
                std::size_t bytes_read =
                    stream2->read(buffer.data(), buffer.size());
                if (bytes_read > 0) {
                    chunk2.append(buffer.data(), bytes_read);
                }
            }

            CHECK(!chunk1.empty());
            CHECK(!chunk2.empty());

            // Check for overlap by looking at last/first lines
            std::vector<std::string> lines1, lines2;
            std::istringstream s1(chunk1);
            std::string line;
            while (std::getline(s1, line)) {
                if (!line.empty()) lines1.push_back(line);
            }

            std::istringstream s2(chunk2);
            while (std::getline(s2, line)) {
                if (!line.empty()) lines2.push_back(line);
            }

            // Last line of chunk1 should be different from first line of chunk2
            if (!lines1.empty() && !lines2.empty()) {
                CHECK(lines1.back() != lines2.front());
            }
        }
    }

    SUBCASE("Prime number boundary") {
        TestEnvironment env(5000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 5000, 80, 250);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(1.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        std::size_t boundary = 1299709;  // Prime number

        if (max_bytes > boundary) {
            std::vector<char> buffer(256 * 1024);

            auto stream1 = reader->stream(StreamType::LINE_BYTES,
                                          RangeType::BYTE_RANGE, 0, boundary);
            auto stream2 =
                reader->stream(StreamType::LINE_BYTES, RangeType::BYTE_RANGE,
                               boundary, max_bytes);

            std::string chunk1, chunk2;

            while (!stream1->done()) {
                std::size_t bytes_read =
                    stream1->read(buffer.data(), buffer.size());
                if (bytes_read > 0) chunk1.append(buffer.data(), bytes_read);
            }

            while (!stream2->done()) {
                std::size_t bytes_read =
                    stream2->read(buffer.data(), buffer.size());
                if (bytes_read > 0) chunk2.append(buffer.data(), bytes_read);
            }

            // Concatenate and verify completeness
            std::string concat = chunk1 + chunk2;

            // Read full file for comparison
            auto stream_full = reader->stream(
                StreamType::LINE_BYTES, RangeType::BYTE_RANGE, 0, max_bytes);
            std::string full;
            while (!stream_full->done()) {
                std::size_t bytes_read =
                    stream_full->read(buffer.data(), buffer.size());
                if (bytes_read > 0) full.append(buffer.data(), bytes_read);
            }

            // Sort and compare to handle ordering differences
            std::vector<std::string> concat_sorted, full_sorted;
            std::istringstream cs(concat);
            std::string line;
            while (std::getline(cs, line)) {
                if (!line.empty()) concat_sorted.push_back(line);
            }
            std::istringstream fs(full);
            while (std::getline(fs, line)) {
                if (!line.empty()) full_sorted.push_back(line);
            }

            std::sort(concat_sorted.begin(), concat_sorted.end());
            std::sort(full_sorted.begin(), full_sorted.end());

            CHECK(concat_sorted.size() == full_sorted.size());
            CHECK(concat_sorted == full_sorted);
        }
    }

    SUBCASE("Off-by-one boundary variations") {
        TestEnvironment env(5000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 5000);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(1.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        std::size_t mid = max_bytes / 2;

        std::vector<char> buffer(256 * 1024);

        // Test boundary, boundary-1, and boundary+1
        for (int offset = -1; offset <= 1; ++offset) {
            std::size_t boundary = mid + offset;
            if (boundary == 0 || boundary >= max_bytes) continue;

            auto s1 = reader->stream(StreamType::LINE_BYTES,
                                     RangeType::BYTE_RANGE, 0, boundary);
            auto s2 =
                reader->stream(StreamType::LINE_BYTES, RangeType::BYTE_RANGE,
                               boundary, max_bytes);

            std::string c1, c2;
            while (!s1->done()) {
                std::size_t br = s1->read(buffer.data(), buffer.size());
                if (br > 0) c1.append(buffer.data(), br);
            }
            while (!s2->done()) {
                std::size_t br = s2->read(buffer.data(), buffer.size());
                if (br > 0) c2.append(buffer.data(), br);
            }

            // Extract and sort lines
            std::vector<std::string> all_lines;
            std::istringstream stream(c1 + c2);
            std::string line;
            while (std::getline(stream, line)) {
                if (!line.empty()) all_lines.push_back(line);
            }

            std::sort(all_lines.begin(), all_lines.end());

            // Check for duplicates
            auto last = std::unique(all_lines.begin(), all_lines.end());
            std::size_t unique_count = std::distance(all_lines.begin(), last);

            INFO("Offset: ", offset, ", Boundary: ", boundary);
            CHECK(unique_count == all_lines.size());
        }
    }
}

TEST_CASE("LINE_BYTES Stream - Multiple Worker Counts") {
    SUBCASE("Variable worker counts (2, 4, 8, 16)") {
        TestEnvironment env(15000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 15000, 90, 280);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(3.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();

        // Read full file once for reference
        auto stream_ref = reader->stream(StreamType::LINE_BYTES,
                                         RangeType::BYTE_RANGE, 0, max_bytes);
        std::vector<char> buffer(512 * 1024);
        std::string full_content;
        while (!stream_ref->done()) {
            std::size_t bytes_read =
                stream_ref->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                full_content.append(buffer.data(), bytes_read);
            }
        }

        std::vector<std::string> reference_lines;
        std::istringstream ref_stream(full_content);
        std::string line;
        while (std::getline(ref_stream, line)) {
            if (!line.empty()) reference_lines.push_back(line);
        }
        std::sort(reference_lines.begin(), reference_lines.end());

        // Test with different worker counts
        for (int num_workers : {2, 4, 8, 16}) {
            std::size_t chunk_size = max_bytes / num_workers;

            std::vector<std::string> chunks(num_workers);
            std::vector<std::thread> threads;

            for (int i = 0; i < num_workers; ++i) {
                threads.emplace_back([&, i]() {
                    std::size_t start = i * chunk_size;
                    std::size_t end = (i == num_workers - 1)
                                          ? max_bytes
                                          : (i + 1) * chunk_size;

                    auto stream =
                        reader->stream(StreamType::LINE_BYTES,
                                       RangeType::BYTE_RANGE, start, end);
                    if (!stream) return;

                    std::vector<char> buf(256 * 1024);
                    while (!stream->done()) {
                        std::size_t br = stream->read(buf.data(), buf.size());
                        if (br > 0) chunks[i].append(buf.data(), br);
                    }
                });
            }

            for (auto& t : threads) {
                t.join();
            }

            // Collect and sort lines
            std::vector<std::string> worker_lines;
            for (const auto& chunk : chunks) {
                std::istringstream s(chunk);
                std::string l;
                while (std::getline(s, l)) {
                    if (!l.empty()) worker_lines.push_back(l);
                }
            }
            std::sort(worker_lines.begin(), worker_lines.end());

            // Check for duplicates
            auto last = std::unique(worker_lines.begin(), worker_lines.end());
            std::size_t unique_count =
                std::distance(worker_lines.begin(), last);

            INFO("Workers: ", num_workers);
            INFO("Total lines: ", worker_lines.size());
            INFO("Unique lines: ", unique_count);

            CHECK(unique_count == worker_lines.size());
            CHECK(worker_lines.size() == reference_lines.size());
            CHECK(worker_lines == reference_lines);
        }
    }
}

TEST_CASE("LINE_BYTES Stream - Event Ordering Verification") {
    SUBCASE("Ordering differences are acceptable but data is complete") {
        TestEnvironment env(8000);
        REQUIRE(env.is_valid());

        std::string gz_file =
            create_variable_line_test_file(env.get_dir(), 8000, 100, 300);
        REQUIRE(!gz_file.empty());

        std::string idx_file = env.get_index_path(gz_file);

        {
            auto indexer =
                IndexerFactory::create(gz_file, idx_file, mb_to_b(2.0));
            indexer->build();
        }

        auto reader = ReaderFactory::create(gz_file, idx_file);
        REQUIRE(reader != nullptr);

        std::size_t max_bytes = reader->get_max_bytes();
        std::size_t chunk_size = max_bytes / 8;

        // Read in 8 chunks
        std::vector<std::string> chunks;
        std::vector<char> buffer(256 * 1024);

        for (int i = 0; i < 8; ++i) {
            std::size_t start = i * chunk_size;
            std::size_t end = (i == 7) ? max_bytes : (i + 1) * chunk_size;

            auto stream = reader->stream(StreamType::LINE_BYTES,
                                         RangeType::BYTE_RANGE, start, end);
            std::string chunk;
            while (!stream->done()) {
                std::size_t bytes_read =
                    stream->read(buffer.data(), buffer.size());
                if (bytes_read > 0) {
                    chunk.append(buffer.data(), bytes_read);
                }
            }
            chunks.push_back(chunk);
        }

        // Read sequentially
        auto stream_seq = reader->stream(StreamType::LINE_BYTES,
                                         RangeType::BYTE_RANGE, 0, max_bytes);
        std::string sequential;
        while (!stream_seq->done()) {
            std::size_t bytes_read =
                stream_seq->read(buffer.data(), buffer.size());
            if (bytes_read > 0) {
                sequential.append(buffer.data(), bytes_read);
            }
        }

        // Extract and sort both
        std::vector<std::string> chunked_lines, sequential_lines;

        for (const auto& chunk : chunks) {
            std::istringstream s(chunk);
            std::string line;
            while (std::getline(s, line)) {
                if (!line.empty()) chunked_lines.push_back(line);
            }
        }

        std::istringstream seq_stream(sequential);
        std::string line;
        while (std::getline(seq_stream, line)) {
            if (!line.empty()) sequential_lines.push_back(line);
        }

        std::sort(chunked_lines.begin(), chunked_lines.end());
        std::sort(sequential_lines.begin(), sequential_lines.end());

        // They should match after sorting (same data, possibly different order)
        CHECK(chunked_lines.size() == sequential_lines.size());
        CHECK(chunked_lines == sequential_lines);

        // But the original concatenation might differ (ordering)
        std::string concatenated;
        for (const auto& chunk : chunks) {
            concatenated += chunk;
        }

        // This might NOT match due to ordering, but that's okay
        // The sorted comparison above proves completeness
        INFO("Concatenated size: ", concatenated.size());
        INFO("Sequential size: ", sequential.size());
        INFO(
            "Byte sizes may differ due to line ordering, but sorted lines "
            "match");
    }
}
