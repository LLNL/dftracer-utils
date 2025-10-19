# dftracer-utils

A collection of utilities for DFTracer

[![Documentation Status](https://readthedocs.org/projects/dftracer/badge/?version=latest)](https://dftracer.readthedocs.io/projects/utils/en/latest/?badge=latest)

## Documentation

Full documentation is available at [Read the Docs](https://dftracer.readthedocs.io/projects/utils/).

To build documentation locally:

```bash
pip install .
cd docs
pip install -r requirements.txt
make html
```

See [docs/README.md](docs/README.md) for detailed documentation building instructions.

## Building

### Prerequisites

- CMake 3.5 or higher
- C++17 compatible compiler
- zlib development library
- SQLite3 development library
- pkg-config

### Build

```bash
mkdir build && cd build
cmake ..
make
```

## Installation

```bash
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=<LOCATION>
make
make install
```

## Developers Guide

Please see [Developers Guide](DEVELOPERS_GUIDE.md) for more information how to test, run coverage, etc.
