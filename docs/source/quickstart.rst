Quick Start Guide
=================

This guide will help you get started with dftracer utilities quickly.

Python Quick Start
------------------

Reading Trace Files
~~~~~~~~~~~~~~~~~~~

The most common use case is reading trace files:

.. code-block:: python

   from dftracer.utils import Reader

   # Open a compressed trace file with index
   reader = Reader("trace.pfw.gz", "trace.pfw.gz.idx")

   # Read lines by line range
   lines = reader.read_lines(0, 100)  # Read first 100 lines
   for line in lines:
       print(line)

   # Or read lines as JSON objects
   json_lines = reader.read_lines_json(0, 100)
   for json_obj in json_lines:
       print(json_obj['field'])

Working with Indexer
~~~~~~~~~~~~~~~~~~~~

Create and use indexes for faster access:

.. code-block:: python

   from dftracer.utils import Indexer

   # Create an indexer
   indexer = Indexer("trace.pfw.gz", "trace.pfw.gz.idx")

   # Build the index if needed
   if indexer.need_rebuild():
       indexer.build()

   # Get index information
   print(f"Max bytes: {indexer.get_max_bytes()}")
   print(f"Num lines: {indexer.get_num_lines()}")

   # Get checkpoints
   checkpoints = indexer.get_checkpoints()
   for cp in checkpoints:
       print(f"Checkpoint {cp.checkpoint_idx}: {cp.num_lines} lines")

C++ Quick Start
---------------

Creating a Reader
~~~~~~~~~~~~~~~~~

Use the factory pattern to create a reader:

.. code-block:: cpp

   #include <dftracer/utils/reader/reader_factory.h>
   #include <dftracer/utils/indexer/indexer_factory.h>
   #include <iostream>
   #include <memory>

   int main() {
       // Create indexer first
       auto indexer = dftracer::utils::IndexerFactory::create(
           "trace.pfw.gz",
           "trace.pfw.gz.idx"
       );

       // Create reader with indexer (transfers ownership)
       auto reader = dftracer::utils::ReaderFactory::create(indexer.release());

       // Simple: Read lines by line range (returns string with all lines)
       std::string lines = reader->read_lines(1, 100);  // Lines 1-100
       std::cout << lines;

       // Advanced: Buffer-based reading for large files
       const size_t read_buffer_size = 1024 * 1024;  // 1MB buffer
       auto buffer = std::make_unique<char[]>(read_buffer_size);

       size_t start_bytes = 0;
       size_t end_bytes = reader->get_max_bytes();
       size_t bytes_written;

       // Read in chunks
       while (start_bytes < end_bytes &&
              (bytes_written = reader->read_line_bytes(
                   start_bytes, end_bytes,
                   buffer.get(), read_buffer_size)) > 0) {
           // Process the chunk
           std::cout.write(buffer.get(), bytes_written);
           start_bytes += bytes_written;  // Advance for next read
       }

       return 0;
   }

Reading with Line Processor
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use a custom line processor for efficient line-by-line processing:

.. code-block:: cpp

   #include <dftracer/utils/reader/reader_factory.h>
   #include <dftracer/utils/reader/line_processor.h>
   #include <iostream>

   // Custom line processor
   class MyLineProcessor : public dftracer::utils::LineProcessor {
   public:
       void process_line(const char* line, size_t length) override {
           // Process each line
           std::cout.write(line, length);
       }
   };

   int main() {
       auto indexer = dftracer::utils::IndexerFactory::create(
           "trace.pfw.gz", "trace.pfw.gz.idx"
       );
       auto reader = dftracer::utils::ReaderFactory::create(indexer.release());

       MyLineProcessor processor;

       // Process lines 1-1000 with custom processor
       reader->read_lines_with_processor(1, 1000, processor);

       return 0;
   }

Working with Indexer
~~~~~~~~~~~~~~~~~~~~~

Use the factory pattern to create an indexer:

.. code-block:: cpp

   #include <dftracer/utils/indexer/indexer_factory.h>

   int main() {
       // Create an indexer using the factory
       auto indexer = dftracer::utils::IndexerFactory::create(
           "trace.pfw.gz",           // Archive path
           "trace.pfw.gz.idx",       // Index path
           true                       // Force rebuild
       );

       // Build the index
       indexer->build();

       // Get index information
       std::cout << "Max bytes: " << indexer->get_max_bytes() << std::endl;
       std::cout << "Num lines: " << indexer->get_num_lines() << std::endl;

       return 0;
   }

C Quick Start
-------------

Reading Trace Files
~~~~~~~~~~~~~~~~~~~

Using the C API for reading trace files:

.. code-block:: c

   #include <dftracer/utils/reader/reader.h>
   #include <stdio.h>
   #include <stdlib.h>

   int main() {
       // Create reader
       dft_reader_handle_t reader = dft_reader_create(
           "trace.pfw.gz",
           "trace.pfw.gz.idx",
           1048576  // checkpoint_size
       );

       // Allocate buffer
       char *buffer = malloc(1024 * 1024);  // 1MB buffer

       // Read lines 1-100
       int result = dft_reader_read_lines(
           reader,
           1, 100,              // start_line, end_line
           buffer,
           1024 * 1024          // buffer_size
       );

       if (result == 0) {
           printf("%s", buffer);
       }

       // Cleanup
       free(buffer);
       dft_reader_destroy(reader);

       return 0;
   }

Working with Indexer
~~~~~~~~~~~~~~~~~~~~

Creating and using an indexer:

.. code-block:: c

   #include <dftracer/utils/indexer/indexer.h>
   #include <stdio.h>

   int main() {
       // Create indexer
       dft_indexer_handle_t indexer = dft_indexer_create(
           "trace.pfw.gz",
           "trace.pfw.gz.idx",
           1048576,  // checkpoint_size
           0         // force_rebuild
       );

       // Build index if needed
       if (dft_indexer_need_rebuild(indexer)) {
           printf("Building index...\n");
           dft_indexer_build(indexer);
       }

       // Get index information
       size_t max_bytes, num_lines;
       dft_indexer_get_max_bytes(indexer, &max_bytes);
       dft_indexer_get_num_lines(indexer, &num_lines);

       printf("Max bytes: %zu\n", max_bytes);
       printf("Num lines: %zu\n", num_lines);

       // Cleanup
       dft_indexer_destroy(indexer);

       return 0;
   }

Next Steps
----------

- Read the :doc:`api/index` for detailed Python API documentation
- Check :doc:`cpp_api/index` for C++ API reference
- See :doc:`developers` for development guidelines
