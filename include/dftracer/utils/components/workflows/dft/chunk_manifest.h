#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_CHUNK_MANIFEST_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_CHUNK_MANIFEST_H

#include <dftracer/utils/components/workflows/dft/chunk_spec.h>

#include <vector>

namespace dftracer::utils::components::workflows::dft {

/**
 * @brief DFTracer-specific manifest with line tracking.
 *
 * Contains multiple DFTracerChunkSpec objects, each with line information.
 */
struct DFTracerChunkManifest {
    std::vector<DFTracerChunkSpec> specs;
    double total_size_mb;

    DFTracerChunkManifest() : total_size_mb(0.0) {}

    void add_spec(DFTracerChunkSpec spec) {
        total_size_mb += spec.size_mb;
        specs.push_back(std::move(spec));
    }

    std::size_t total_lines() const {
        std::size_t total = 0;
        for (const auto& spec : specs) {
            total += spec.num_lines();
        }
        return total;
    }

    bool operator==(const DFTracerChunkManifest& other) const {
        return specs == other.specs && total_size_mb == other.total_size_mb;
    }

    bool operator!=(const DFTracerChunkManifest& other) const {
        return !(*this == other);
    }
};

}  // namespace dftracer::utils::components::workflows::dft

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_CHUNK_MANIFEST_H
