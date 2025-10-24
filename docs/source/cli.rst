Command-Line Tools
==================

DFTracer Utils provides several command-line utilities for working with DFTracer trace files and compressed archives.

dftracer_reader
---------------

**Description:** DFTracer utility for reading and indexing compressed files (GZIP, TAR.GZ)

**Usage:**

.. code-block:: bash

   dftracer_reader [OPTIONS] file

**Arguments:**

- ``file`` - Compressed file to process (GZIP, TAR.GZ) [required]

**Options:**

- ``-i, --index <path>`` - Index file to use (default: auto-generated in temp directory)
- ``-s, --start <bytes>`` - Start position in bytes (default: -1)
- ``-e, --end <bytes>`` - End position in bytes (default: -1)
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for indexing in bytes (default: 33554432 B / 32 MB)
- ``-f, --force-rebuild`` - Force rebuild of index even if it exists
- ``--check`` - Check if index is valid
- ``--read-buffer-size <bytes>`` - Size of the read buffer in bytes (default: 1MB)
- ``--mode <mode>`` - Set the reading mode: bytes, line_bytes, or lines (default: bytes)
- ``--index-dir <path>`` - Directory to store index files (default: system temp directory)

**Example:**

.. code-block:: bash

   # Read bytes 100-200 from a compressed file
   dftracer_reader --start 100 --end 200 trace.pfw.gz

   # Read in line mode
   dftracer_reader --mode lines --start 1 --end 100 trace.pfw.gz

   # Build index with custom checkpoint size
   dftracer_reader --checkpoint-size 20971520 trace.pfw.gz

dftracer_info
-------------

**Description:** Display metadata and index information for DFTracer compressed files

**Usage:**

.. code-block:: bash

   dftracer_info [OPTIONS]

**Options:**

- ``--files <files...>`` - Compressed files to inspect (GZIP, TAR.GZ)
- ``-d, --directory <path>`` - Directory containing files to inspect
- ``-v, --verbose`` - Show detailed information including index details
- ``-f, --force-rebuild`` - Force rebuild index files
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for indexing in bytes (default: 33554432 B / 32 MB)
- ``--index-dir <path>`` - Directory to store index files (default: system temp directory)
- ``--threads <count>`` - Number of threads for parallel processing (default: number of CPU cores)

**Example:**

.. code-block:: bash

   # Show info for files in a directory
   dftracer_info -d ./logs

   # Show info for specific files with verbose output
   dftracer_info --files trace1.pfw.gz trace2.pfw.gz -v

   # Analyze with 4 threads
   dftracer_info --threads 4 -d ./traces

dftracer_merge
--------------

**Description:** Merge DFTracer .pfw or .pfw.gz files into a single JSON array file using pipeline processing

**Usage:**

.. code-block:: bash

   dftracer_merge [OPTIONS]

**Options:**

- ``-d, --directory <path>`` - Directory containing .pfw or .pfw.gz files (default: .)
- ``-o, --output <path>`` - Output file path (should have .pfw extension) (default: combined.pfw)
- ``-f, --force`` - Override existing output file and force index recreation
- ``-c, --compress`` - Compress output file with gzip
- ``-v, --verbose`` - Enable verbose mode
- ``-g, --gzip-only`` - Process only .pfw.gz files
- ``--checkpoint-size <bytes>`` - Checkpoint size for indexing in bytes (default: 33554432 B / 32 MB)
- ``--threads <count>`` - Number of threads for parallel processing (default: number of CPU cores)
- ``--index-dir <path>`` - Directory to store index files (default: system temp directory)

**Example:**

.. code-block:: bash

   # Merge all .pfw/.pfw.gz files in current directory
   dftracer_merge -o merged.pfw

   # Merge files from specific directory with compression
   dftracer_merge -d ./logs -o output.pfw -c

   # Merge with parallel processing and verbose output
   dftracer_merge -d ./traces -o combined.pfw --threads 8 -v

dftracer_split
--------------

**Description:** Split DFTracer traces into equal-sized chunks using pipeline processing

**Usage:**

.. code-block:: bash

   dftracer_split [OPTIONS]

**Options:**

- ``-n, --app-name <name>`` - Application name for output files (default: app)
- ``-d, --directory <path>`` - Input directory containing .pfw or .pfw.gz files (default: .)
- ``-o, --output <dir>`` - Output directory for split files (default: ./split)
- ``-s, --chunk-size <MB>`` - Chunk size in MB (default: 4)
- ``-f, --force`` - Override existing files and force index recreation
- ``-c, --compress`` - Compress output files with gzip (default: true)
- ``-v, --verbose`` - Enable verbose mode
- ``--checkpoint-size <bytes>`` - Checkpoint size for indexing in bytes (default: 33554432 B / 32 MB)
- ``--threads <count>`` - Number of threads for parallel processing (default: number of CPU cores)
- ``--index-dir <path>`` - Directory to store index files (default: system temp directory)
- ``--verify`` - Verify output chunks match input by comparing event IDs

**Example:**

.. code-block:: bash

   # Split files into 4MB chunks
   dftracer_split -d ./logs -o ./split_output

   # Split with 10MB chunks and custom app name
   dftracer_split -d ./traces -s 10 -n myapp -o ./chunks

   # Split without compression and verify output
   dftracer_split -d ./data -c false --verify -o ./output

dftracer_event_count
--------------------

**Description:** Count valid events in DFTracer .pfw or .pfw.gz files using pipeline processing

**Usage:**

.. code-block:: bash

   dftracer_event_count [OPTIONS]

**Options:**

- ``-d, --directory <path>`` - Directory containing .pfw or .pfw.gz files (default: .)
- ``-f, --force`` - Force index recreation
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for indexing in bytes (default: 33554432 B / 32 MB)
- ``--threads <count>`` - Number of threads for parallel processing (default: number of CPU cores)
- ``--index-dir <path>`` - Directory to store index files (default: system temp directory)

**Example:**

.. code-block:: bash

   # Count events in current directory
   dftracer_event_count

   # Count events in specific directory with 8 threads
   dftracer_event_count -d ./traces --threads 8

   # Force index rebuild
   dftracer_event_count -d ./logs -f

dftracer_pgzip
--------------

**Description:** Parallel gzip compression for DFTracer .pfw files

**Usage:**

.. code-block:: bash

   dftracer_pgzip [OPTIONS]

**Options:**

- ``-d, --directory <path>`` - Directory containing .pfw files (default: .)
- ``-v, --verbose`` - Enable verbose output
- ``--threads <count>`` - Number of threads for parallel processing (default: number of CPU cores)

**Example:**

.. code-block:: bash

   # Compress all .pfw files in current directory
   dftracer_pgzip

   # Compress files in specific directory with verbose output
   dftracer_pgzip -d ./logs -v

   # Compress with 16 threads
   dftracer_pgzip -d ./traces --threads 16
