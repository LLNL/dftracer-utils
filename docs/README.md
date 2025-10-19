# Documentation

This directory contains the documentation for dftracer-utils.

## Building Documentation Locally

### Prerequisites

Install the required dependencies:

```bash
pip install -r requirements.txt
```

For C++ API documentation, you also need Doxygen:

```bash
# Ubuntu/Debian
sudo apt-get install doxygen graphviz

# macOS
brew install doxygen graphviz
```

### Build HTML Documentation

```bash
cd docs
make html
```

The generated documentation will be in `build/html/`. Open `build/html/index.html` in your browser.

### Build PDF Documentation

```bash
cd docs
make latexpdf
```

### Clean Build

```bash
cd docs
make clean
```

## Documentation Structure

- `source/` - ReStructuredText source files
- `source/conf.py` - Sphinx configuration
- `source/_static/` - Static files (images, CSS, etc.)
- `source/_templates/` - Custom templates
- `source/api/` - Python API documentation
- `source/cpp_api/` - C++ API documentation
- `Doxyfile` - Doxygen configuration for C++ documentation
- `requirements.txt` - Python dependencies for building docs

## Read the Docs

This project uses Read the Docs for hosting documentation. The configuration is in `.readthedocs.yaml` at the project root.

### Setting up Read the Docs

1. Go to [Read the Docs](https://readthedocs.org/)
2. Sign in with your GitHub account
3. Import your project
4. The documentation will build automatically on each commit

### Configuration

The Read the Docs build process:
1. Installs system dependencies (doxygen, cmake, etc.)
2. Runs Doxygen to generate C++ API docs
3. Installs Python dependencies from `requirements.txt`
4. Builds Sphinx documentation

## Writing Documentation

### Python Docstrings

Use Google-style or NumPy-style docstrings:

```python
def example_function(param1, param2):
    """Short description.

    Longer description that can span multiple lines.

    Args:
        param1 (str): Description of param1
        param2 (int): Description of param2

    Returns:
        bool: Description of return value

    Raises:
        ValueError: When param2 is negative
    """
    pass
```

### C++ Documentation

Use Doxygen-style comments:

```cpp
/**
 * @brief Short description
 *
 * Longer description that can span multiple lines.
 *
 * @param param1 Description of param1
 * @param param2 Description of param2
 * @return Description of return value
 */
int example_function(int param1, int param2);
```

### Adding New Pages

1. Create a new `.rst` file in `source/`
2. Add it to the appropriate `toctree` directive
3. Rebuild the documentation

## Troubleshooting

### "Module not found" errors

Make sure the package is installed:
```bash
pip install -e .
```

### Doxygen warnings

Check the `Doxyfile` configuration and ensure all paths are correct.

### Build failures on Read the Docs

Check the build log on Read the Docs for specific errors. Common issues:
- Missing system dependencies
- Python version mismatch
- Import errors

## Resources

- [Sphinx Documentation](https://www.sphinx-doc.org/)
- [Read the Docs Documentation](https://docs.readthedocs.io/)
- [Doxygen Manual](https://www.doxygen.nl/manual/)
- [Breathe Documentation](https://breathe.readthedocs.io/)
