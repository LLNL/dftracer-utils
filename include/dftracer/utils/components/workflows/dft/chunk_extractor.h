#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_CHUNK_EXTRACTOR_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_CHUNK_EXTRACTOR_H

#include <dftracer/utils/components/io/types/types.h>
#include <dftracer/utils/components/workflows/dft/chunk_manifest.h>
#include <dftracer/utils/core/utilities/utilities.h>

#include <cstddef>
#include <string>

namespace dftracer::utils::components::workflows::dft {

/**
 * @brief Input for DFTracer chunk extraction.
 *
 * Accepts DFTracerChunkManifest with line tracking, but converts to
 * byte-based io::ChunkManifest for extraction.
 */
struct DFTracerChunkExtractionInput {
    int chunk_index;  // Application-level: which output chunk number
    DFTracerChunkManifest manifest;
    std::string output_dir;
    std::string app_name;
    bool compress = false;

    DFTracerChunkExtractionInput() : chunk_index(0), compress(false) {}

    static DFTracerChunkExtractionInput from_manifest(int index,
                                                      DFTracerChunkManifest m) {
        DFTracerChunkExtractionInput input;
        input.chunk_index = index;
        input.manifest = std::move(m);
        return input;
    }

    DFTracerChunkExtractionInput& with_output_dir(std::string dir) {
        output_dir = std::move(dir);
        return *this;
    }

    DFTracerChunkExtractionInput& with_app_name(std::string name) {
        app_name = std::move(name);
        return *this;
    }

    DFTracerChunkExtractionInput& with_compression(bool enabled) {
        compress = enabled;
        return *this;
    }

    // Convert to byte-based io::ChunkManifest for extraction
    io::ChunkManifest to_io_manifest() const {
        io::ChunkManifest io_manifest;
        io_manifest.total_size_mb = manifest.total_size_mb;
        for (const auto& dft_spec : manifest.specs) {
            io::ChunkSpec io_spec;
            io_spec.file_path = dft_spec.file_path;
            io_spec.idx_path = dft_spec.idx_path;
            io_spec.size_mb = dft_spec.size_mb;
            io_spec.start_byte = dft_spec.start_byte;
            io_spec.end_byte = dft_spec.end_byte;
            io_manifest.specs.push_back(io_spec);
        }
        return io_manifest;
    }

    bool operator==(const DFTracerChunkExtractionInput& other) const {
        return chunk_index == other.chunk_index && manifest == other.manifest &&
               output_dir == other.output_dir && app_name == other.app_name &&
               compress == other.compress;
    }
};

/**
 * @brief Result of DFTracer chunk extraction.
 */
struct DFTracerChunkExtractionResult {
    int chunk_index;
    std::string output_path;
    double size_mb;
    std::size_t events;  // DFTracer-specific: number of JSON events
    bool success;

    DFTracerChunkExtractionResult()
        : chunk_index(0), size_mb(0.0), events(0), success(false) {}

    DFTracerChunkExtractionResult(int index, std::string path, double mb,
                                  std::size_t event_count, bool succ)
        : chunk_index(index),
          output_path(std::move(path)),
          size_mb(mb),
          events(event_count),
          success(succ) {}

    bool operator==(const DFTracerChunkExtractionResult& other) const {
        return chunk_index == other.chunk_index &&
               output_path == other.output_path && size_mb == other.size_mb &&
               events == other.events && success == other.success;
    }

    bool operator!=(const DFTracerChunkExtractionResult& other) const {
        return !(*this == other);
    }
};

/**
 * @brief Workflow for extracting and merging chunks from DFTracer files.
 *
 * This workflow:
 * 1. Reads byte ranges from multiple file specs (compressed or plain)
 * 2. Filters valid JSON events
 * 3. Writes them to output file with hash computation
 * 4. Optionally compresses the result
 *
 * Uses byte-based ChunkSpec from io::ChunkSpec for precise I/O control.
 *
 * Composes:
 * - Reader API for byte-based reading
 * - JSON validation for filtering
 * - StreamingFileWriter for output
 * - Optional gzip compression
 *
 * Usage:
 * @code
 * DFTracerChunkExtractor extractor;
 *
 * auto input = DFTracerChunkExtractionInput::from_manifest(1, manifest)
 *                  .with_output_dir("/output")
 *                  .with_app_name("myapp")
 *                  .with_compression(true);
 *
 * auto result = extractor.process(input);
 * if (result.success) {
 *     std::cout << "Extracted " << result.events << " events\n";
 * }
 * @endcode
 */
class DFTracerChunkExtractor
    : public utilities::Utility<DFTracerChunkExtractionInput,
                                DFTracerChunkExtractionResult> {
   public:
    DFTracerChunkExtractionResult process(
        const DFTracerChunkExtractionInput& input) override;

   private:
    DFTracerChunkExtractionResult extract_and_write(
        const DFTracerChunkExtractionInput& input);
    bool compress_output(const std::string& input_path,
                         const std::string& output_path);
};

}  // namespace dftracer::utils::components::workflows::dft

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFTRACER_CHUNK_EXTRACTOR_H
