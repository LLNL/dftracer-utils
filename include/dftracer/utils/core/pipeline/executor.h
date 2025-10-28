#ifndef DFTRACER_UTILS_CORE_PIPELINE_EXECUTOR_H
#define DFTRACER_UTILS_CORE_PIPELINE_EXECUTOR_H

#include <dftracer/utils/core/pipeline/task_queue.h>

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

namespace dftracer::utils {

class Task;
class TaskContext;
class Scheduler;

/**
 * Executor - Executes tasks from queue using worker thread pool
 *
 * Features:
 * - Large thread pool for CPU/IO-bound work
 * - Pulls tasks from queue
 * - Executes task functions
 * - Notifies scheduler on completion via callback
 *
 * Thread pool size: N threads (default: hardware_concurrency)
 */
class Executor {
   public:
    using CompletionCallback = std::function<void(std::shared_ptr<Task>)>;

   private:
    TaskQueue queue_;
    std::vector<std::thread> worker_threads_;
    std::atomic<bool> running_{false};
    size_t num_threads_;

    CompletionCallback completion_callback_;
    std::mutex callback_mutex_;

    // Reference to scheduler for dynamic task context
    Scheduler* scheduler_{nullptr};

    // Responsiveness tracking
    std::atomic<size_t> tasks_completed_{0};
    std::atomic<size_t> tasks_started_{0};
    std::chrono::steady_clock::time_point last_activity_time_;
    mutable std::mutex activity_mutex_;

    // Shutdown coordination
    std::atomic<bool> shutdown_requested_{false};

   public:
    /**
     * Constructor
     * @param num_threads Number of worker threads (0 = hardware_concurrency)
     */
    explicit Executor(size_t num_threads = 0);

    ~Executor();

    // Prevent copying
    Executor(const Executor&) = delete;
    Executor& operator=(const Executor&) = delete;

    // Prevent moving (threads are not movable once started)
    Executor(Executor&&) = delete;
    Executor& operator=(Executor&&) = delete;

    /**
     * Start the executor (spawn worker threads)
     */
    void start();

    /**
     * Shutdown the executor gracefully
     */
    void shutdown();

    /**
     * Reset the executor (prepare for new execution)
     */
    void reset();

    /**
     * Get reference to the task queue
     */
    TaskQueue& get_queue() { return queue_; }

    /**
     * Set completion callback (called when task finishes)
     */
    void set_completion_callback(CompletionCallback callback);

    /**
     * Set scheduler reference (for TaskContext)
     */
    void set_scheduler(Scheduler* scheduler) { scheduler_ = scheduler; }

    /**
     * Check if executor is running
     */
    bool is_running() const { return running_.load(); }

    /**
     * Get number of worker threads
     */
    size_t get_num_threads() const { return num_threads_; }

    /**
     * Request graceful shutdown
     * Stops accepting new tasks and waits for current tasks to complete
     */
    void request_shutdown();

    /**
     * Check if shutdown was requested
     */
    bool is_shutdown_requested() const { return shutdown_requested_.load(); }

    /**
     * Check if executor is responsive (making progress)
     *
     * Used by watchdog to detect if executor is hung.
     * Returns false if executor appears to be stuck or unresponsive.
     */
    bool is_responsive() const;

   private:
    /**
     * Worker thread main loop
     */
    void worker_thread();

    /**
     * Execute a single task
     */
    void execute_task(TaskItem& item);

    /**
     * Notify completion callback
     */
    void notify_completion(std::shared_ptr<Task> task);

    /**
     * Mark activity (task start or completion) for responsiveness tracking
     */
    void mark_activity();
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_CORE_PIPELINE_EXECUTOR_H
