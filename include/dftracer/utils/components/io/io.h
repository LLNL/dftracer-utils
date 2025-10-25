#ifndef DFTRACER_UTILS_COMPONENTS_IO_IO_H
#define DFTRACER_UTILS_COMPONENTS_IO_IO_H

/**
 * @file io.h
 * @brief Convenience header that includes all I/O components.
 *
 * This header provides access to:
 * - File readers (StreamingFileReader, FileReader, BinaryFileReader)
 * - File writers (StreamingFileWriter)
 * - Streaming utilities (ChunkIterator, ChunkRange)
 * - I/O types (RawData, CompressedData, ChunkSpec, ChunkManifest)
 * - Line-based I/O (LineRange, LineBytesRange, StreamingLineReader)
 *
 * Usage:
 * @code
 * #include <dftracer/utils/components/io/io.h>
 *
 * // All I/O components are now available
 * auto reader = std::make_shared<StreamingFileReader>();
 * StreamReadInput input{"/path/to/file.txt", 64 * 1024};
 * ChunkRange chunks = reader->process(input);
 * @endcode
 */

// File readers and writers
#include <dftracer/utils/components/io/binary_file_reader.h>
#include <dftracer/utils/components/io/file_reader.h>
#include <dftracer/utils/components/io/streaming_file_reader.h>
#include <dftracer/utils/components/io/streaming_file_writer.h>

// Streaming utilities
#include <dftracer/utils/components/io/chunk_iterator.h>
#include <dftracer/utils/components/io/streaming.h>

// I/O types
#include <dftracer/utils/components/io/types/types.h>

// Line-based I/O
#include <dftracer/utils/components/io/lines/lines.h>

#endif  // DFTRACER_UTILS_COMPONENTS_IO_IO_H
