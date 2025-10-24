# PythonHelpers.cmake - Utilities for Python packaging with scikit-build-core

#[=[
Creates a Python wrapper script in venv/bin that calls the actual binary in site-packages.

This is useful for wheel builds where binaries are installed to site-packages/package/bin/
but need to be accessible from the virtualenv's bin directory.

Usage:
  create_python_wrapper(target_name)

Arguments:
  target_name - The name of the executable target to create a wrapper for

Example:
  create_python_wrapper(dftracer_reader)

This will:
1. Create a Python script that locates the real binary in site-packages
2. Install it to venv/bin/ with the same name as the binary
3. The wrapper forwards all arguments to the real binary
#]=]
function(create_python_wrapper target_name)
  if(NOT SKBUILD)
    message(WARNING "create_python_wrapper called but SKBUILD is not set. Skipping wrapper creation.")
    return()
  endif()

  if(NOT DEFINED CMAKE_INSTALL_VENV_BIN_DIR)
    message(FATAL_ERROR "create_python_wrapper requires CMAKE_INSTALL_VENV_BIN_DIR to be set")
  endif()

  # Generate the Python wrapper script
  file(GENERATE OUTPUT ${CMAKE_BINARY_DIR}/venv_wrapper_${target_name} CONTENT "#!/usr/bin/env python3
import os, sys, subprocess
from pathlib import Path

# Binary is in site-packages/dftracer/bin/, wrapper is in venv/bin/
wrapper = Path(__file__).resolve()
site_pkg = wrapper.parent.parent / 'lib' / f'python{sys.version_info.major}.{sys.version_info.minor}' / 'site-packages'
binary = site_pkg / 'dftracer' / 'bin' / '${target_name}'

if not binary.exists():
    print(f'Error: Could not find binary at {binary}', file=sys.stderr)
    print(f'Wrapper location: {wrapper}', file=sys.stderr)
    print(f'Site-packages: {site_pkg}', file=sys.stderr)
    sys.exit(1)

# Execute the binary with all arguments
sys.exit(subprocess.call([str(binary)] + sys.argv[1:]))
")

  # Install the wrapper to venv/bin with the same name as the target
  install(PROGRAMS ${CMAKE_BINARY_DIR}/venv_wrapper_${target_name}
          DESTINATION ${CMAKE_INSTALL_VENV_BIN_DIR}
          RENAME ${target_name})

  message(STATUS "Created Python wrapper for ${target_name} -> ${CMAKE_INSTALL_VENV_BIN_DIR}/${target_name}")
endfunction()
