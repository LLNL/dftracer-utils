C++ API Reference
=================

This section contains the C++ API documentation for dftracer utilities.

.. note::
   The C++ API documentation is generated using Doxygen and Breathe.
   Make sure to run Doxygen before building the documentation.

.. toctree::
   :maxdepth: 2
   :caption: C++ Components:

   reader
   indexer
   pipeline

Overview
--------

The dftracer utilities C++ library is organized into several namespaces:

- ``dftracer::utils::core`` - Core utilities and data structures
- ``dftracer::utils::reader`` - Trace file reading
- ``dftracer::utils::indexer`` - Indexing capabilities
- ``dftracer::utils::pipeline`` - Processing pipelines

Main Classes
------------

.. doxygennamespace:: dftracer::utils
   :project: dftracer-utils
   :members:
