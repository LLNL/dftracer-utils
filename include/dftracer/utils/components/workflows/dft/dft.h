#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_DFT_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_DFT_H

/**
 * @file dft.h
 * @brief Convenience header for all DFTracer workflows.
 *
 * This header provides a single include for all DFTracer-specific workflows
 * and related types.
 */

// Core workflows
#include <dftracer/utils/components/workflows/dft/chunk_extractor.h>
#include <dftracer/utils/components/workflows/dft/chunk_manifest_mapper.h>
#include <dftracer/utils/components/workflows/dft/event_collector.h>
#include <dftracer/utils/components/workflows/dft/event_hasher.h>
#include <dftracer/utils/components/workflows/dft/metadata_collector.h>

// DFTracer-specific types
#include <dftracer/utils/components/workflows/dft/chunk_manifest.h>
#include <dftracer/utils/components/workflows/dft/chunk_spec.h>

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_DFT_DFT_H
