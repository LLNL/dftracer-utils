#ifndef DFTRACER_UTILS_COMPONENTS_COMPRESSION_H
#define DFTRACER_UTILS_COMPONENTS_COMPRESSION_H

/**
 * @file compression.h
 * @brief Top-level header for all compression component utilities.
 *
 * This header provides access to all compression utilities:
 * - gzip: Gzip compression and decompression
 *
 * Usage:
 * @code
 * #include <dftracer/utils/components/compression/compression.h>
 *
 * // Use specific compression type
 * using namespace dftracer::utils::components::compression;
 *
 * auto compressor = std::make_shared<gzip::Compressor>();
 * gzip::RawData input("Hello, World!");
 * gzip::CompressedData output = compressor->process(input);
 * @endcode
 */

#include <dftracer/utils/components/compression/gzip/gzip.h>

#endif  // DFTRACER_UTILS_COMPONENTS_COMPRESSION_H
