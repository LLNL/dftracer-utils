#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_EVENT_COLLECTOR_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_EVENT_COLLECTOR_H

#include <dftracer/utils/components/workflows/dft/chunk_extractor.h>
#include <dftracer/utils/components/workflows/dft/metadata_collector.h>
#include <dftracer/utils/core/utilities/utilities.h>
#include <dftracer/utils/reader/line_processor.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dftracer::utils::components::workflows::dft {

/**
 * @brief Unique identifier for a DFTracer event.
 */
struct EventId {
    std::int64_t id;
    std::int64_t pid;
    std::int64_t tid;

    EventId() : id(-1), pid(-1), tid(-1) {}
    EventId(std::int64_t i, std::int64_t p, std::int64_t t)
        : id(i), pid(p), tid(t) {}

    bool operator<(const EventId& other) const {
        if (id != other.id) return id < other.id;
        if (pid != other.pid) return pid < other.pid;
        return tid < other.tid;
    }

    bool operator==(const EventId& other) const {
        return id == other.id && pid == other.pid && tid == other.tid;
    }

    bool is_valid() const { return id >= 0; }
};

/**
 * @brief Input for event collection from DFTracer metadata.
 */
struct EventCollectionFromMetadataInput {
    std::vector<DFTracerMetadataOutput> metadata;

    static EventCollectionFromMetadataInput from_metadata(
        std::vector<DFTracerMetadataOutput> meta) {
        EventCollectionFromMetadataInput input;
        input.metadata = std::move(meta);
        return input;
    }
};

/**
 * @brief Input for event collection from chunk results.
 */
struct EventCollectionFromChunksInput {
    std::vector<DFTracerChunkExtractionResult> chunks;
    std::size_t checkpoint_size;

    static EventCollectionFromChunksInput from_chunks(
        std::vector<DFTracerChunkExtractionResult> results) {
        EventCollectionFromChunksInput input;
        input.chunks = std::move(results);
        input.checkpoint_size = 0;
        return input;
    }

    EventCollectionFromChunksInput& with_checkpoint_size(std::size_t size) {
        checkpoint_size = size;
        return *this;
    }
};

/**
 * @brief Output: vector of collected EventIds.
 */
using EventCollectionOutput = std::vector<EventId>;

/**
 * @brief Workflow for collecting event IDs from DFTracer metadata files.
 *
 * Reads files specified in metadata and extracts EventId (id, pid, tid)
 * from each valid JSON event.
 */
class EventCollectorFromMetadata
    : public utilities::Utility<EventCollectionFromMetadataInput,
                                EventCollectionOutput> {
   public:
    EventCollectionOutput process(
        const EventCollectionFromMetadataInput& input) override;
};

/**
 * @brief Workflow for collecting event IDs from output chunk files.
 *
 * Reads chunk output files and extracts EventId from each valid JSON event.
 * Handles both compressed and uncompressed chunk files.
 */
class EventCollectorFromChunks
    : public utilities::Utility<EventCollectionFromChunksInput,
                                EventCollectionOutput> {
   public:
    EventCollectionOutput process(
        const EventCollectionFromChunksInput& input) override;
};

}  // namespace dftracer::utils::components::workflows::dft

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_EVENT_COLLECTOR_H
