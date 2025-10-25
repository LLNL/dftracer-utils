#ifndef DFTRACER_UTILS_COMPONENTS_FILESYSTEM_H
#define DFTRACER_UTILS_COMPONENTS_FILESYSTEM_H

/**
 * @file filesystem.h
 * @brief Convenience header for filesystem component utilities.
 *
 * This header provides composable utilities for filesystem operations:
 * - DirectoryScanner: Scan directories and enumerate files
 * - PatternDirectoryScanner: Scan directories with pattern/extension filtering
 *
 * Usage:
 * @code
 * #include <dftracer/utils/components/filesystem/filesystem.h>
 *
 * using namespace dftracer::utils::components::filesystem;
 *
 * auto scanner = std::make_shared<DirectoryScanner>();
 * auto files = scanner->process(Directory{"/path/to/dir"});
 *
 * auto pattern_scanner = std::make_shared<PatternDirectoryScanner>();
 * auto pfw_files = pattern_scanner->process(
 *     PatternDirectory{"/path/to/dir", {".pfw", ".pfw.gz"}}
 * );
 * @endcode
 */

#include <dftracer/utils/components/filesystem/directory_scanner.h>
#include <dftracer/utils/components/filesystem/pattern_directory_scanner.h>

#endif  // DFTRACER_UTILS_COMPONENTS_FILESYSTEM_H
