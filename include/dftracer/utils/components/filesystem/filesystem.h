#ifndef DFTRACER_UTILS_COMPONENTS_FILESYSTEM_H
#define DFTRACER_UTILS_COMPONENTS_FILESYSTEM_H

/**
 * @file filesystem.h
 * @brief Convenience header for filesystem component utilities.
 *
 * This header provides composable utilities for filesystem operations:
 * - DirectoryScanner: Scan directories and enumerate files
 *
 * Usage:
 * @code
 * #include <dftracer/utils/components/filesystem/filesystem.h>
 *
 * using namespace dftracer::utils::components::filesystem;
 *
 * auto scanner = std::make_shared<DirectoryScanner>();
 * auto files = scanner->process(Directory{"/path/to/dir"});
 * @endcode
 */

#include <dftracer/utils/components/filesystem/directory_scanner.h>

#endif  // DFTRACER_UTILS_COMPONENTS_FILESYSTEM_H
