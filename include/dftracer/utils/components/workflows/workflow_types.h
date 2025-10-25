#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_WORKFLOW_TYPES_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_WORKFLOW_TYPES_H

#include <dftracer/utils/core/common/filesystem.h>
#include <dftracer/utils/indexer/indexer.h>

#include <cstddef>
#include <string>
#include <vector>

namespace dftracer::utils::components::workflows {

/**
 * @brief Input for processing files in a directory.
 */
struct DirectoryProcessInput {
    std::string directory_path;
    bool recursive = false;
    std::vector<std::string> extensions;  // e.g., {".pfw", ".pfw.gz"}

    DirectoryProcessInput() = default;

    DirectoryProcessInput(std::string path,
                          std::vector<std::string> exts = {".pfw", ".pfw.gz"},
                          bool recurse = false)
        : directory_path(std::move(path)),
          recursive(recurse),
          extensions(std::move(exts)) {}

    static DirectoryProcessInput from_directory(std::string path) {
        DirectoryProcessInput input;
        input.directory_path = std::move(path);
        return input;
    }

    DirectoryProcessInput& with_extensions(std::vector<std::string> exts) {
        extensions = std::move(exts);
        return *this;
    }

    DirectoryProcessInput& with_recursive(bool rec) {
        recursive = rec;
        return *this;
    }

    bool operator==(const DirectoryProcessInput& other) const {
        return directory_path == other.directory_path &&
               recursive == other.recursive && extensions == other.extensions;
    }

    bool operator!=(const DirectoryProcessInput& other) const {
        return !(*this == other);
    }
};

/**
 * @brief Input for reading an indexed file.
 */
struct IndexedReadInput {
    std::string file_path;
    std::string idx_path;
    std::size_t checkpoint_size = Indexer::DEFAULT_CHECKPOINT_SIZE;
    bool force_rebuild = false;

    IndexedReadInput() = default;

    IndexedReadInput(std::string fpath, std::string ipath,
                     std::size_t ckpt_size = Indexer::DEFAULT_CHECKPOINT_SIZE,
                     bool force = false)
        : file_path(std::move(fpath)),
          idx_path(std::move(ipath)),
          checkpoint_size(ckpt_size),
          force_rebuild(force) {}
};

/**
 * @brief Input for batch processing lines from a file.
 */
struct LineBatchInput {
    std::string file_path;
    std::string idx_path;        // Empty for plain text files
    std::size_t start_line = 0;  // 0 = from beginning
    std::size_t end_line = 0;    // 0 = to end
    std::size_t checkpoint_size = Indexer::DEFAULT_CHECKPOINT_SIZE;

    LineBatchInput() = default;

    LineBatchInput(std::string fpath, std::string ipath = "",
                   std::size_t start = 0, std::size_t end = 0)
        : file_path(std::move(fpath)),
          idx_path(std::move(ipath)),
          start_line(start),
          end_line(end) {}
};

/**
 * @brief Output from processing a single file.
 */
struct FileProcessOutput {
    std::string file_path;
    bool success = false;
    std::size_t items_processed = 0;
    std::string error_message;

    FileProcessOutput() = default;

    FileProcessOutput(std::string path, bool succ, std::size_t items = 0,
                      std::string error = "")
        : file_path(std::move(path)),
          success(succ),
          items_processed(items),
          error_message(std::move(error)) {}

    static FileProcessOutput success_output(const std::string& path,
                                            std::size_t items) {
        return FileProcessOutput(path, true, items, "");
    }

    static FileProcessOutput error_output(const std::string& path,
                                          const std::string& error) {
        return FileProcessOutput(path, false, 0, error);
    }
};

/**
 * @brief Aggregated output from processing multiple files.
 */
template <typename T>
struct BatchProcessOutput {
    std::vector<T> results;
    std::size_t successful_count = 0;
    std::size_t failed_count = 0;
    std::size_t total_items_processed = 0;

    BatchProcessOutput() = default;

    void add(T result) { results.push_back(std::move(result)); }

    void finalize() {
        successful_count = 0;
        failed_count = 0;
        total_items_processed = 0;

        for (const auto& result : results) {
            if constexpr (std::is_same_v<T, FileProcessOutput>) {
                if (result.success) {
                    successful_count++;
                    total_items_processed += result.items_processed;
                } else {
                    failed_count++;
                }
            }
        }
    }
};

}  // namespace dftracer::utils::components::workflows

// Hash specializations for using workflow types in hash-based containers
namespace std {

template <>
struct hash<dftracer::utils::components::workflows::DirectoryProcessInput> {
    std::size_t operator()(
        const dftracer::utils::components::workflows::DirectoryProcessInput&
            input) const {
        std::size_t h1 = std::hash<std::string>{}(input.directory_path);
        std::size_t h2 = std::hash<bool>{}(input.recursive);
        // Hash the extensions vector
        std::size_t h3 = 0;
        for (const auto& ext : input.extensions) {
            h3 ^= std::hash<std::string>{}(ext);
        }
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

}  // namespace std

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_WORKFLOW_TYPES_H
