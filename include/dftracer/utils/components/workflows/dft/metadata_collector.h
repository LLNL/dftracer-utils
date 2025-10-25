#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_METADATA_COLLECTOR_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_METADATA_COLLECTOR_H

#include <dftracer/utils/core/tasks/task_context.h>
#include <dftracer/utils/core/utilities/utilities.h>
#include <dftracer/utils/indexer/indexer_factory.h>
#include <dftracer/utils/reader/reader_factory.h>

#include <string>

namespace dftracer::utils::components::workflows::dft {

/**
 * @brief Input for DFTracer file metadata collection.
 */
struct DFTracerMetadataInput {
    std::string file_path;
    std::string idx_path;  // Empty for plain files
    std::size_t checkpoint_size = Indexer::DEFAULT_CHECKPOINT_SIZE;
    bool force_rebuild = false;

    DFTracerMetadataInput() = default;

    DFTracerMetadataInput(std::string fpath, std::string ipath = "",
                          std::size_t ckpt = Indexer::DEFAULT_CHECKPOINT_SIZE,
                          bool force = false)
        : file_path(std::move(fpath)),
          idx_path(std::move(ipath)),
          checkpoint_size(ckpt),
          force_rebuild(force) {}

    static DFTracerMetadataInput from_file(std::string path) {
        DFTracerMetadataInput input;
        input.file_path = std::move(path);
        return input;
    }

    DFTracerMetadataInput& with_index(std::string idx) {
        idx_path = std::move(idx);
        return *this;
    }

    DFTracerMetadataInput& with_checkpoint_size(std::size_t size) {
        checkpoint_size = size;
        return *this;
    }

    DFTracerMetadataInput& with_force_rebuild(bool force) {
        force_rebuild = force;
        return *this;
    }

    bool operator==(const DFTracerMetadataInput& other) const {
        return file_path == other.file_path && idx_path == other.idx_path &&
               checkpoint_size == other.checkpoint_size &&
               force_rebuild == other.force_rebuild;
    }
};

/**
 * @brief Output from DFTracer file metadata collection.
 */
struct DFTracerMetadataOutput {
    std::string file_path;
    std::string idx_path;
    double size_mb = 0;
    std::size_t start_line = 0;
    std::size_t end_line = 0;
    std::size_t valid_events = 0;
    double size_per_line = 0;
    bool success = false;

    DFTracerMetadataOutput() = default;

    bool operator==(const DFTracerMetadataOutput& other) const {
        return file_path == other.file_path && idx_path == other.idx_path &&
               size_mb == other.size_mb && start_line == other.start_line &&
               end_line == other.end_line &&
               valid_events == other.valid_events &&
               size_per_line == other.size_per_line && success == other.success;
    }
};

/**
 * @brief Collects metadata (size, line count, events) from DFTracer trace
 * files.
 *
 * Supports both plain (.pfw) and compressed (.pfw.gz) files.
 * For compressed files, builds/uses an index for efficient access.
 */
class DFTracerMetadataCollector
    : public utilities::Utility<DFTracerMetadataInput, DFTracerMetadataOutput> {
   public:
    DFTracerMetadataCollector() = default;

    DFTracerMetadataOutput process(const DFTracerMetadataInput& input) override;

   private:
    DFTracerMetadataOutput process_compressed(
        const DFTracerMetadataInput& input);
    DFTracerMetadataOutput process_plain(const DFTracerMetadataInput& input);
};

}  // namespace dftracer::utils::components::workflows::dft

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_METADATA_COLLECTOR_H
