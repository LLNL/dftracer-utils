#include <dftracer/utils/components/workflows/dft/event_hasher.h>
#include <dftracer/utils/core/common/logging.h>
#include <xxhash.h>

namespace dftracer::utils::components::workflows::dft {

EventHashOutput EventHasher::process(const EventHashInput& input) {
    XXH3_state_t* state = XXH3_createState();
    if (!state) {
        DFTRACER_UTILS_LOG_ERROR("%s", "Failed to create XXH3 state");
        return 0;
    }

    XXH3_64bits_reset_withSeed(state, 0);

    for (const auto& event : input.events) {
        XXH3_64bits_update(state, &event.id, sizeof(event.id));
        XXH3_64bits_update(state, &event.pid, sizeof(event.pid));
        XXH3_64bits_update(state, &event.tid, sizeof(event.tid));
    }

    std::uint64_t hash = XXH3_64bits_digest(state);
    XXH3_freeState(state);

    return hash;
}

}  // namespace dftracer::utils::components::workflows::dft
