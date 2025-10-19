Command-Line Tools
==================

DFTracer Utils provides several command-line utilities for working with DFTracer trace files and compressed archives.

dft_reader
----------

**Description:** DFTracer utility for reading and indexing compressed files (GZIP, TAR.GZ)

**Usage:**

.. code-block:: bash

   dft_reader [OPTIONS] file

**Arguments:**

- ``file`` - Compressed file to process (GZIP, TAR.GZ) [required]

**Options:**

- ``-i, --index <path>`` - Index file to use (default: auto-generated .idx file)
- ``-s, --start <bytes>`` - Start position in bytes
- ``-e, --end <bytes>`` - End position in bytes
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for indexing in bytes (default: 10 MB)
- ``-f, --force-rebuild`` - Force rebuild of index even if it exists
- ``--start-line <line>`` - Start line number
- ``--end-line <line>`` - End line number
- ``--num-lines <count>`` - Number of lines to read

**Example:**

.. code-block:: bash

   # Read lines 100-200 from a compressed file
   dft_reader --start-line 100 --end-line 200 trace.pfw.gz

   # Build index with custom checkpoint size
   dft_reader --checkpoint-size 20971520 trace.pfw.gz

dft_info
--------

**Description:** Display metadata and index information for DFTracer compressed files

**Usage:**

.. code-block:: bash

   dft_info [OPTIONS] files...

**Arguments:**

- ``files`` - One or more compressed files to analyze [required]

**Options:**

- ``-j, --json`` - Output in JSON format
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for indexing (default: 10 MB)
- ``-t, --threads <count>`` - Number of threads for parallel processing
- ``--rebuild`` - Force rebuild of indices

**Example:**

.. code-block:: bash

   # Show info for a single file
   dft_info trace.pfw.gz

   # Show info for multiple files in JSON format
   dft_info --json trace1.pfw.gz trace2.pfw.gz

   # Analyze with 4 threads
   dft_info --threads 4 trace*.pfw.gz

dft_merge
---------

**Description:** Merge DFTracer .pfw or .pfw.gz files into a single JSON array file using pipeline processing

**Usage:**

.. code-block:: bash

   dft_merge [OPTIONS] -o output files...

**Arguments:**

- ``files`` - Input .pfw or .pfw.gz files to merge [required]

**Options:**

- ``-o, --output <path>`` - Output JSON file [required]
- ``-t, --threads <count>`` - Number of worker threads for parallel processing
- ``-b, --batch-size <count>`` - Batch size for processing
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for indexing (default: 10 MB)

**Example:**

.. code-block:: bash

   # Merge multiple trace files into one JSON file
   dft_merge -o merged.json trace1.pfw.gz trace2.pfw.gz trace3.pfw.gz

   # Merge with parallel processing
   dft_merge -t 8 -o output.json trace*.pfw.gz

dft_split
---------

**Description:** Split DFTracer traces into equal-sized chunks using pipeline processing

**Usage:**

.. code-block:: bash

   dft_split [OPTIONS] -o output_dir files...

**Arguments:**

- ``files`` - Input .pfw or .pfw.gz files to split [required]

**Options:**

- ``-o, --output <dir>`` - Output directory for split files [required]
- ``-n, --num-splits <count>`` - Number of output files to create
- ``-s, --split-size <count>`` - Number of events per output file
- ``-t, --threads <count>`` - Number of worker threads
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for indexing (default: 10 MB)
- ``-f, --format <format>`` - Output format (pfw, pfw.gz, json)

**Example:**

.. code-block:: bash

   # Split into 10 equal chunks
   dft_split -n 10 -o split_output/ large_trace.pfw.gz

   # Split with specific event count per file
   dft_split --split-size 100000 -o chunks/ trace.pfw.gz

dft_event_count
---------------

**Description:** Count valid events in DFTracer .pfw or .pfw.gz files using pipeline processing

**Usage:**

.. code-block:: bash

   dft_event_count [OPTIONS] files...

**Arguments:**

- ``files`` - Input .pfw or .pfw.gz files to count [required]

**Options:**

- ``-t, --threads <count>`` - Number of worker threads for parallel processing
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for indexing (default: 10 MB)
- ``-v, --verbose`` - Show detailed progress information

**Example:**

.. code-block:: bash

   # Count events in a single file
   dft_event_count trace.pfw.gz

   # Count events across multiple files with 8 threads
   dft_event_count -t 8 trace*.pfw.gz

dft_pgzip
---------

**Description:** Parallel gzip compression for DFTracer .pfw files

**Usage:**

.. code-block:: bash

   dft_pgzip [OPTIONS] files...

**Arguments:**

- ``files`` - Input .pfw files to compress [required]

**Options:**

- ``-t, --threads <count>`` - Number of compression threads (default: number of CPU cores)
- ``-c, --checkpoint-size <bytes>`` - Checkpoint size for chunking (default: 10 MB)
- ``-l, --level <1-9>`` - Compression level (default: 6)
- ``-o, --output <path>`` - Output file path (for single input file)
- ``-k, --keep`` - Keep original files after compression

**Example:**

.. code-block:: bash

   # Compress a file with default settings
   dft_pgzip trace.pfw

   # Compress with maximum compression and 16 threads
   dft_pgzip -l 9 -t 16 trace.pfw

   # Compress and keep original
   dft_pgzip -k trace.pfw
