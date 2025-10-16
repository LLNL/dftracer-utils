#ifndef DFTRACER_UTILS_REPLAY_REPLAY_H
#define DFTRACER_UTILS_REPLAY_REPLAY_H

#include <dftracer/utils/analyzers/trace.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/reader/line_processor.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dftracer::utils::analyzers {
    struct Trace; // Forward declaration
}

namespace dftracer::utils::replay {

/**
 * Configuration options for trace replay
 */
struct ReplayConfig {
    bool maintain_timing = true;            // Maintain original timing between operations
    bool dry_run = false;                   // Only parse and log operations, don't execute
    bool dftracer_mode = false;             // Use DFTracer sleep-based replay mode
    bool no_sleep = false;                  // Disable sleep calls in dftracer mode
    double timing_scale = 1.0;              // Scale timing (1.0 = original, 0.5 = 2x faster, 2.0 = 2x slower)
    std::uint64_t start_time_offset = 0;    // Offset to add to all timestamps
    std::unordered_set<std::string> filter_functions; // Only replay these functions (empty = all)
    std::unordered_set<std::string> exclude_functions; // Exclude these functions
    std::unordered_set<std::string> filter_categories;  // Only replay these categories
    bool verbose = false;                   // Verbose logging
    std::string output_directory;           // Directory for creating files (empty = use original paths)
    std::size_t max_file_size = 1024 * 1024 * 100; // Max file size to create (100MB default)
};

/**
 * Results and statistics from replay execution
 */
struct ReplayResult {
    std::size_t total_events = 0;
    std::size_t executed_events = 0;
    std::size_t filtered_events = 0;
    std::size_t failed_events = 0;
    std::chrono::microseconds total_duration;
    std::chrono::microseconds execution_duration;
    std::unordered_map<std::string, std::size_t> function_counts;
    std::unordered_map<std::string, std::size_t> category_counts;
    std::vector<std::string> error_messages;
};

/**
 * Interface for executing individual trace operations
 */
class TraceExecutor {
public:
    virtual ~TraceExecutor() = default;
    
    /**
     * Execute a single trace operation
     * @param trace The parsed trace event
     * @param config Replay configuration
     * @return true if successful, false otherwise
     */
    virtual bool execute(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) = 0;
    
    /**
     * Check if this executor can handle the given trace
     * @param trace The trace event to check
     * @return true if this executor can handle the trace
     */
    virtual bool can_handle(const dftracer::utils::analyzers::Trace& trace) const = 0;
    
    /**
     * Get human-readable name for this executor
     */
    virtual std::string get_name() const = 0;
};

/**
 * Executor for POSIX file operations (read, write, open, close, etc.)
 */
class PosixExecutor : public TraceExecutor {
public:
    bool execute(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) override;
    bool can_handle(const dftracer::utils::analyzers::Trace& trace) const override;
    std::string get_name() const override { return "POSIX"; }

private:
    std::unordered_map<std::string, int> open_files_; // Track file descriptors
    int next_fd_ = 1000; // Start with high FD numbers to avoid conflicts
    
    bool execute_open(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config);
    bool execute_close(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config);
    bool execute_read(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config);
    bool execute_write(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config);
    bool execute_seek(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config);
    bool execute_stat(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config);
};

/**
Stub DFtracer executor: Need to expand on thsi to integrate with DFtracer
*/

class DFTracerExecutor : public TraceExecutor {
public:
    bool execute(const dftracer::utils::analyzers::Trace& trace, const ReplayConfig& config) override;
    bool can_handle(const dftracer::utils::analyzers::Trace& trace) const override;
    std::string get_name() const override { return "DFTracer"; }

private:
    void sleep_for_duration(double duration_microseconds);
    bool dftracer_initialized_ = false;
};

/**
 * Main replay engine that coordinates trace reading and execution
 */
class ReplayEngine {
    friend class ReplayLineProcessor;
public:
    explicit ReplayEngine(const ReplayConfig& config);
    ~ReplayEngine();
    
    /**
     * Add a custom executor for specific trace types
     * @param executor Unique pointer to executor (engine takes ownership)
     */
    void add_executor(std::unique_ptr<TraceExecutor> executor);
    
    /**
     * Replay traces from a single file
     * @param trace_file Path to trace file (.pfw or .pfw.gz)
     * @param index_file Optional path to index file (for .pfw.gz files)
     * @return Replay results and statistics
     */
    ReplayResult replay_file(const std::string& trace_file, 
                            const std::string& index_file = "");
    
    /**
     * Replay traces from multiple files
     * @param trace_files Vector of trace file paths
     * @return Aggregated replay results and statistics
     */
    ReplayResult replay_files(const std::vector<std::string>& trace_files);

private:
    ReplayConfig config_;
    std::vector<std::unique_ptr<TraceExecutor>> executors_;
    std::chrono::steady_clock::time_point replay_start_time_;
    std::uint64_t first_trace_timestamp_ = 0;
    bool first_timestamp_set_ = false;
    
    /**
     * Process a single trace line (JSON)
     */
    bool process_trace_line(const std::string& line, ReplayResult& result);
    
    /**
     * Parse JSON trace into Trace structure
     */
    bool parse_trace_json(const std::string& json_line, dftracer::utils::analyzers::Trace& trace);
    
    /**
     * Apply timing logic before executing trace
     */
    void apply_timing(const dftracer::utils::analyzers::Trace& trace);
    
    /**
     * Check if trace should be executed based on filters
     */
    bool should_execute_trace(const dftracer::utils::analyzers::Trace& trace) const;
    
    /**
     * Find appropriate executor for trace
     */
    TraceExecutor* find_executor(const dftracer::utils::analyzers::Trace& trace);
    
    /**
     * Get file path for replay (handles output directory override)
     */
    std::string get_replay_file_path(const std::string& original_path) const;
};

/**
 * Line processor for handling trace lines during replay
 */
class ReplayLineProcessor : public LineProcessor {
public:
    explicit ReplayLineProcessor(ReplayEngine& engine, ReplayResult& result);
    
    bool process(const char* data, std::size_t length) override;

private:
    ReplayEngine& engine_;
    ReplayResult& result_;
};

}  // namespace dftracer::utils::replay

#endif  // DFTRACER_UTILS_REPLAY_REPLAY_H