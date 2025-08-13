.PHONY: coverage coverage-clean coverage-view test test-coverage test-py build clean help

# Default target
help:
	@echo "Available targets:"
	@echo "  coverage        - Build with coverage and generate HTML report"
	@echo "  coverage-clean  - Clean coverage build directory"
	@echo "  coverage-view   - Open coverage report in browser (macOS/Linux)"
	@echo "  test           - Build and run tests without coverage"
	@echo "  test-coverage  - Run tests with coverage (requires prior coverage build)"
	@echo "  test-py        - Run Python tests in isolated venv"
	@echo "  build          - Build project normally"
	@echo "  clean          - Clean all build directories"
	@echo "  help           - Show this help"

# Generate coverage report
coverage:
	@echo "Building with coverage and generating report..."
	@./coverage.sh

# Clean coverage build
coverage-clean:
	@echo "Cleaning coverage build directory..."
	@rm -rf build_coverage

# View coverage report
coverage-view:
	@if [ -f "build_coverage/coverage/html/index.html" ]; then \
		if [[ "$$OSTYPE" == "darwin"* ]]; then \
			open build_coverage/coverage/html/index.html; \
		elif [[ "$$OSTYPE" == "linux-gnu"* ]]; then \
			xdg-open build_coverage/coverage/html/index.html; \
		else \
			echo "Please open build_coverage/coverage/html/index.html in your browser"; \
		fi; \
	else \
		echo "Coverage report not found. Run 'make coverage' first."; \
	fi

# Build and run tests without coverage
test:
	@echo "Building and running tests..."
	@mkdir -p build_test
	@cd build_test && CMAKE_POLICY_VERSION_MINIMUM=3.5 cmake -DCMAKE_BUILD_TYPE=Debug -DDFTRACER_UTILS_TESTS=ON -DCMAKE_INSTALL_PREFIX=$$(pwd)/install .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4) && ctest --output-on-failure

# Run tests with coverage (requires coverage build)
test-coverage:
	@if [ -d "build_coverage" ]; then \
		cd build_coverage && ctest --output-on-failure; \
	else \
		echo "Coverage build not found. Run 'make coverage' first."; \
	fi

# Run Python tests in isolated environment
test-py:
	@echo "Running Python tests in isolated environment..."
	@rm -rf .venv_test_py
	@python3 -m venv .venv_test_py
	@.venv_test_py/bin/pip install --upgrade pip setuptools wheel
	@.venv_test_py/bin/pip install -e .[dev]
	@.venv_test_py/bin/pytest tests/py/test_reader.py -v
	@rm -rf .venv_test_py
	@echo "Python tests completed successfully!"

format:
	@echo "Formatting code..."
	@clang-format -i -style=file ./src/*/*.{cpp,h}
	@clang-format -i -style=file ./tests/*/*.{cpp,h}
	@clang-format -i -style=file ./py/src/*.{cpp,h}

# Normal build
build:
	@echo "Building project..."
	@mkdir -p build
	@cd build && CMAKE_POLICY_VERSION_MINIMUM=3.5 cmake .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

build-debug:
	@echo "Building project in debug mode..."
	@mkdir -p build_debug
	@cd build_debug && CMAKE_POLICY_VERSION_MINIMUM=3.5 cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

build-release:
	@echo "Building project in release mode..."
	@mkdir -p build
	@cd build && CMAKE_POLICY_VERSION_MINIMUM=3.5 cmake -DCMAKE_BUILD_TYPE=Release .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

install-debug: build
	@echo "Installing project..."
	@mkdir -p build

	@cd build
	CMAKE_POLICY_VERSION_MINIMUM=3.5 cmake -DCMAKE_BUILD_TYPE=Debug ..
	make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
	CMAKE_POLICY_VERSION_MINIMUM=3.5 make --install . --prefix ./install

# Clean all builds
clean:
	@echo "Cleaning all build directories..."
	@rm -rf build build_debug build_test build_coverage install .venv_test_py
