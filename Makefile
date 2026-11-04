PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=glossql
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
# The build names itself the DuckDB release the submodule pins, so the released
# duckdb loads the loadable; the pipeline sets the same from duckdb_version
OVERRIDE_GIT_DESCRIBE ?= v2.0.0

include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Lint: the format pass and the tidy pass, run here and by the pipeline's
# Format Check the same way. `make lint-tools` makes the project venv once
# (uv), the tools pinned so every machine formats alike. The formatter is the
# venv's clang-format over the sources, styled by the .clang-format here. The
# tidy pass names the sources itself, as the template's tidy-check regex
# ('src/.*/') matches none of them, and hands clang-tidy the SDK on macOS. A
# second tidy pass flags unused includes alone: the include-cleaner's
# missing-include side reads DuckDB's umbrella headers as gaps, so it stays off
LINT_VENV := $(PROJ_DIR).venv
LINT_CLANG_FORMAT := 23.1.3
LINT_CLANG_TIDY := 23.1.4
LINT_SOURCES := $(wildcard src/*.cpp src/include/*.hpp)
TIDY_SYSROOT := $(if $(filter Darwin,$(shell uname -s)),--extra-arg=-isysroot$(shell xcrun --show-sdk-path),)

lint-tools:
	uv venv --clear $(LINT_VENV)
	uv pip install --python $(LINT_VENV) 'clang-format==$(LINT_CLANG_FORMAT)' 'clang-tidy==$(LINT_CLANG_TIDY)'

lint: lint-format lint-tidy

lint-format:
	$(LINT_VENV)/bin/clang-format --dry-run -Werror $(LINT_SOURCES)

lint-tidy: release
	ls src/*.cpp | xargs -P 6 -n 1 $(LINT_VENV)/bin/clang-tidy -p build/release -quiet -header-filter='src/include/glossql.*' --extra-arg=-DDUCKDB_CLANG_TIDY $(TIDY_SYSROOT)
	ls src/*.cpp | xargs -P 6 -n 1 $(LINT_VENV)/bin/clang-tidy -p build/release -quiet --config="{Checks: '-*,misc-include-cleaner', WarningsAsErrors: '*', CheckOptions: {misc-include-cleaner.MissingIncludes: false}}" -header-filter='src/include/glossql.*' --extra-arg=-DDUCKDB_CLANG_TIDY $(TIDY_SYSROOT)

lint-fix:
	$(LINT_VENV)/bin/clang-format -i $(LINT_SOURCES)
