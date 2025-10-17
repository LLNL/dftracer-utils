#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/utils/timer.h>

#include <cstdio>

namespace dftracer::utils {

Timer::Timer(bool autostart, bool verbose)
    : verbose_(verbose), running_(false) {
    if (autostart) {
        start();
    }
}

Timer::Timer(const std::string& name, bool autostart, bool verbose)
    : verbose_(verbose), running_(false), name_(name) {
    if (autostart) {
        start();
    }
}

Timer::~Timer() {
    stop();
    if (verbose_) {
        if (name_.empty()) {
            printf("Elapsed time: %lld ns\n", elapsed());
            // DFTRACER_UTILS_LOG_INFO("Elapsed time: %lld ns", elapsed());
            // DFTRACER_UTILS_LOG_DEBUG("Elapsed time: %lld ns", elapsed());
        } else {
            printf("[%s] Elapsed time: %lld ns\n", name_.c_str(), elapsed());
            // DFTRACER_UTILS_LOG_INFO("[%s] Elapsed time: %lld ns",
            // name_.c_str(), elapsed());
            // DFTRACER_UTILS_LOG_DEBUG("[%s] Elapsed time: %lld ns",
            // name_.c_str(), elapsed());
        }
    }
}

void Timer::start() {
    start_time = Clock::now();
    running_ = true;
}

void Timer::stop() {
    if (running_) {
        end_time = Clock::now();
        running_ = false;
    }
}

std::int64_t Timer::elapsed() const {
    if (running_) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   Clock::now() - start_time)
            .count();
    } else {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time -
                                                                    start_time)
            .count();
    }
}

}  // namespace dftracer::utils
