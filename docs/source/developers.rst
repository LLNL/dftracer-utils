Developer's Guide
=================

This guide contains information for developers contributing to dftracer utilities.

For more detailed development information, see the `DEVELOPERS_GUIDE.md <https://github.com/LLNL/dftracer-utils/blob/main/DEVELOPERS_GUIDE.md>`_ in the repository.

Development Setup
-----------------

1. Clone the repository:

   .. code-block:: bash

      git clone https://github.com/LLNL/dftracer-utils.git
      cd dftracer-utils

2. Install development dependencies:

   .. code-block:: bash

      pip install -e ".[dev]"

3. Build the C++ components:

   .. code-block:: bash

      mkdir build && cd build
      cmake ..
      make

Running Tests
-------------

Python Tests
~~~~~~~~~~~~

.. code-block:: bash

   pytest tests/

C++ Tests
~~~~~~~~~

.. code-block:: bash

   cd build
   ctest

Code Coverage
-------------

To run tests with coverage:

.. code-block:: bash

   ./coverage.sh

Building Documentation
----------------------

To build the documentation locally:

.. code-block:: bash

   cd docs
   make html

The built documentation will be in ``docs/build/html/``.

Code Style
----------

Python
~~~~~~

This project uses ``ruff`` for Python code formatting:

.. code-block:: bash

   ruff check .
   ruff format .

C++
~~~

This project uses ``clang-format`` for C++ code formatting:

.. code-block:: bash

   clang-format -i src/**/*.cpp include/**/*.h

Contributing
------------

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and ensure they pass
5. Submit a pull request

Coding Guidelines
-----------------

- Follow the existing code style
- Write tests for new functionality
- Update documentation as needed
- Keep commits atomic and well-described
