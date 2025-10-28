#include <dftracer/utils/core/common/logging.h>
#include <dftracer/utils/core/pipeline/executor.h>
#include <dftracer/utils/core/tasks/task.h>
#include <dftracer/utils/core/tasks/task_context.h>

#include <chrono>
#include <exception>

namespace dftracer::utils {

Executor::Executor(size_t num_threads)
    : num_threads_(num_threads == 0 ? std::thread::hardware_concurrency()
                                    : num_threads),
      last_activity_time_(std::chrono::steady_clock::now()) {
    if (num_threads_ == 0) {
        num_threads_ = 2;  // Fallback if hardware_concurrency returns 0
    }
    DFTRACER_UTILS_LOG_DEBUG("Executor created with %zu threads", num_threads_);
}

Executor::~Executor() { shutdown(); }

void Executor::start() {
    if (running_) {
        DFTRACER_UTILS_LOG_WARN("%s", "Executor already running");
        return;
    }

    running_ = true;
    worker_threads_.clear();
    worker_threads_.reserve(num_threads_);

    for (size_t i = 0; i < num_threads_; ++i) {
        worker_threads_.emplace_back(&Executor::worker_thread, this);
    }

    DFTRACER_UTILS_LOG_DEBUG("Executor started with %zu worker threads",
                             num_threads_);
}

void Executor::shutdown() {
    if (!running_) {
        return;
    }

    DFTRACER_UTILS_LOG_DEBUG("%s", "Shutting down executor");
    running_ = false;
    queue_.shutdown();

    for (auto& thread : worker_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    worker_threads_.clear();
    DFTRACER_UTILS_LOG_DEBUG("%s", "Executor shutdown complete");
}

void Executor::reset() {
    // Queue will be reset by caller if needed
    DFTRACER_UTILS_LOG_DEBUG("%s", "Executor reset");
}

void Executor::set_completion_callback(CompletionCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    completion_callback_ = std::move(callback);
}

void Executor::worker_thread() {
    DFTRACER_UTILS_LOG_DEBUG("%s", "Executor worker thread started");

    while (running_) {
        // Pop task from queue (blocking)
        auto task_item_opt = queue_.pop(/* blocking */ true);

        if (!task_item_opt) {
            // Queue is shutdown and empty
            continue;
        }

        auto& task_item = *task_item_opt;
        if (!task_item.task) {
            DFTRACER_UTILS_LOG_WARN("%s", "Null task in queue");
            continue;
        }

        // Execute the task
        execute_task(task_item);
    }

    DFTRACER_UTILS_LOG_DEBUG("%s", "Executor worker thread terminated");
}

void Executor::execute_task(TaskItem& item) {
    auto task = item.task;
    auto input = item.input;

    // Mark task start
    mark_activity();
    ++tasks_started_;

    try {
        // Create task context
        TaskContext context(scheduler_, task->get_id());

        // Execute task
        DFTRACER_UTILS_LOG_DEBUG("Executing task ID %d ('%s')", task->get_id(),
                                 task->get_name().c_str());

        std::any result = task->execute(context, *input);

        // Fulfill promise
        task->fulfill_promise(std::move(result));

        DFTRACER_UTILS_LOG_DEBUG("Task ID %d ('%s') completed successfully",
                                 task->get_id(), task->get_name().c_str());

        // Mark task completion
        mark_activity();
        ++tasks_completed_;

        // Notify scheduler
        notify_completion(task);

    } catch (const std::exception& e) {
        DFTRACER_UTILS_LOG_ERROR("Task ID %d ('%s') failed: %s", task->get_id(),
                                 task->get_name().c_str(), e.what());

        // Fulfill promise with exception
        task->fulfill_promise_exception(std::current_exception());

        // Mark task completion (even on error)
        mark_activity();
        ++tasks_completed_;

        // Still notify scheduler (to handle error policy)
        notify_completion(task);

    } catch (...) {
        DFTRACER_UTILS_LOG_ERROR(
            "Task ID %d ('%s') failed with unknown exception", task->get_id(),
            task->get_name().c_str());

        // Fulfill promise with exception
        task->fulfill_promise_exception(std::current_exception());

        // Mark task completion (even on error)
        mark_activity();
        ++tasks_completed_;

        // Still notify scheduler
        notify_completion(task);
    }
}

void Executor::notify_completion(std::shared_ptr<Task> task) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    if (completion_callback_) {
        completion_callback_(task);
    }
}

void Executor::request_shutdown() {
    if (shutdown_requested_.load()) {
        return;  // Already requested
    }

    DFTRACER_UTILS_LOG_DEBUG("%s", "Shutdown requested for executor");
    shutdown_requested_ = true;

    // Stop accepting new tasks
    queue_.shutdown();
}

bool Executor::is_responsive() const {
    // If shutdown was requested, consider unresponsive
    if (shutdown_requested_.load()) {
        return false;
    }

    // If not running, not responsive
    if (!running_.load()) {
        return false;
    }

    // Check if we have pending tasks but no recent activity
    size_t queue_size = queue_.size();
    if (queue_size > 0) {
        // Get time since last activity
        std::lock_guard<std::mutex> lock(activity_mutex_);
        auto now = std::chrono::steady_clock::now();
        auto idle_time = now - last_activity_time_;

        // If idle for more than 5 seconds with pending tasks, consider
        // unresponsive
        if (idle_time > std::chrono::seconds(5)) {
            DFTRACER_UTILS_LOG_WARN(
                "Executor appears unresponsive: %zu tasks in queue, idle for "
                "%lld ms",
                queue_size,
                std::chrono::duration_cast<std::chrono::milliseconds>(idle_time)
                    .count());
            return false;
        }
    }

    // Check if all threads might be deadlocked
    // (all threads busy but no progress for a while)
    size_t started = tasks_started_.load();
    size_t completed = tasks_completed_.load();
    size_t active = started - completed;

    if (active >= num_threads_) {
        // All threads busy - check if making progress
        std::lock_guard<std::mutex> lock(activity_mutex_);
        auto now = std::chrono::steady_clock::now();
        auto idle_time = now - last_activity_time_;

        // If all threads busy but no activity for 10 seconds, likely deadlocked
        if (idle_time > std::chrono::seconds(10)) {
            DFTRACER_UTILS_LOG_WARN(
                "Executor appears deadlocked: %zu threads, %zu active tasks, "
                "idle for %lld ms",
                num_threads_, active,
                std::chrono::duration_cast<std::chrono::milliseconds>(idle_time)
                    .count());
            return false;
        }
    }

    return true;
}

void Executor::mark_activity() {
    std::lock_guard<std::mutex> lock(activity_mutex_);
    last_activity_time_ = std::chrono::steady_clock::now();
}

}  // namespace dftracer::utils
