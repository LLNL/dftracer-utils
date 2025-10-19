Installation
============

This guide covers installation of dftracer-utils for both Python and C++ users.

Python Installation
-------------------

Using pip
~~~~~~~~~

The easiest way to install dftracer-utils is via pip:

.. code-block:: bash

   pip install dftracer-utils

From Source
~~~~~~~~~~~

To install from source:

.. code-block:: bash

   git clone https://github.com/your-org/dftracer-utils.git
   cd dftracer-utils
   pip install .

For development installation with optional dependencies:

.. code-block:: bash

   pip install -e ".[dev]"

C++ Installation
----------------

Prerequisites
~~~~~~~~~~~~~

Before building dftracer-utils, ensure you have:

- CMake 3.5 or higher
- C++17 compatible compiler (GCC, Clang, or MSVC)
- zlib development library
- SQLite3 development library
- pkg-config

On Ubuntu/Debian:

.. code-block:: bash

   sudo apt-get install cmake build-essential zlib1g-dev libsqlite3-dev pkg-config

On macOS:

.. code-block:: bash

   brew install cmake zlib sqlite pkg-config

Building from Source
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   git clone https://github.com/your-org/dftracer-utils.git
   cd dftracer-utils
   mkdir build && cd build
   cmake ..
   make
   sudo make install

Custom Installation Location
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To install to a custom location:

.. code-block:: bash

   mkdir build && cd build
   cmake .. -DCMAKE_INSTALL_PREFIX=/path/to/install
   make
   make install

Verifying Installation
----------------------

Python
~~~~~~

To verify your Python installation:

.. code-block:: python

   import dftracer.utils
   print(dftracer.utils.__version__)

C++
~~~

To verify your C++ installation, try compiling a simple example:

.. code-block:: cpp

   #include <dftracer/utils/indexer/indexer_factory.h>
   #include <iostream>

   int main() {
       // Create an indexer to verify installation
       auto indexer = dftracer::utils::IndexerFactory::create(
           "test.pfw.gz",
           "test.pfw.gz.idx",
           false  // Don't force rebuild
       );

       std::cout << "Library installed successfully!" << std::endl;
       std::cout << "Archive format: " << indexer->get_format_name() << std::endl;
       return 0;
   }

Compile with:

.. code-block:: bash

   g++ -std=c++17 example.cpp -ldftracer_utils -o example
   ./example
