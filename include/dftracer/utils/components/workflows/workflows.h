#ifndef DFTRACER_UTILS_COMPONENTS_WORKFLOWS_WORKFLOWS_H
#define DFTRACER_UTILS_COMPONENTS_WORKFLOWS_WORKFLOWS_H

/**
 * @file workflows.h
 * @brief Convenience header for all workflows.
 *
 * This header provides a single include for all general-purpose workflows,
 * DFTracer-specific workflows, and related types.
 */

// Core workflow types
#include <dftracer/utils/components/workflows/workflow_types.h>

// Generic workflows
#include <dftracer/utils/components/workflows/batch_processor.h>
#include <dftracer/utils/components/workflows/chunk_verifier.h>
#include <dftracer/utils/components/workflows/directory_file_processor.h>

// DFTracer-specific workflows
#include <dftracer/utils/components/workflows/dft/dft.h>

#endif  // DFTRACER_UTILS_COMPONENTS_WORKFLOWS_WORKFLOWS_H
