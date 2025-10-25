#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_CHUNK_MANIFEST_MAPPER_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_CHUNK_MANIFEST_MAPPER_H

#include <dftracer/utils/components/io/types/types.h>
#include <dftracer/utils/components/workflows/dft/chunk_manifest.h>
#include <dftracer/utils/components/workflows/dft/metadata_collector.h>
#include <dftracer/utils/core/utilities/utilities.h>

#include <cstddef>
#include <vector>

namespace dftracer::utils::components::workflows::dft {

/**
 * @brief Input for DFTracer chunk manifest mapping.
 */
struct DFTracerChunkManifestMapperInput {
    std::vector<DFTracerMetadataOutput> file_metadata;
    double target_chunk_size_mb;

    DFTracerChunkManifestMapperInput() : target_chunk_size_mb(0) {}

    static DFTracerChunkManifestMapperInput from_metadata(
        std::vector<DFTracerMetadataOutput> metadata) {
        DFTracerChunkManifestMapperInput input;
        input.file_metadata = std::move(metadata);
        return input;
    }

    DFTracerChunkManifestMapperInput& with_target_size(double size_mb) {
        target_chunk_size_mb = size_mb;
        return *this;
    }
};

/**
 * @brief Output: vector of DFTracer chunk manifests with line tracking.
 */
using DFTracerChunkManifestMapperOutput = std::vector<DFTracerChunkManifest>;

/**
 * @brief Workflow for mapping DFTracer file metadata to chunk manifests with
 * line tracking.
 *
 * This workflow takes DFTracer-specific file metadata (line counts, sizes,
 * event counts) and distributes files across chunks to achieve a target chunk
 * size. It creates DFTracerChunkManifest objects with both byte offsets and
 * line numbers.
 *
 * Algorithm:
 * - Greedily fills chunks up to target size
 * - Splits large files across multiple chunks if needed
 * - Tracks both byte offsets and line ranges for each chunk
 * - Approximates byte offsets from line-based metadata
 * - Assumes uniform byte distribution across lines
 * - Line boundary alignment happens during extraction (LineBytesRange)
 *
 * Usage:
 * @code
 * DFTracerChunkManifestMapper mapper;
 *
 * auto input = DFTracerChunkManifestMapperInput::from_metadata(file_metadata)
 *                  .with_target_size(100.0);  // 100 MB chunks
 *
 * auto manifests = mapper.process(input);
 * // manifests[i] contains DFTracerChunkSpec objects with line tracking
 * @endcode
 */
class DFTracerChunkManifestMapper
    : public utilities::Utility<DFTracerChunkManifestMapperInput,
                                DFTracerChunkManifestMapperOutput> {
   public:
    DFTracerChunkManifestMapperOutput process(
        const DFTracerChunkManifestMapperInput& input) override;
};

}  // namespace dftracer::utils::components::workflows::dft

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_CHUNK_MANIFEST_MAPPER_H
