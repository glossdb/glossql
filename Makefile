PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=gloss
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
# The build names itself the DuckDB release the submodule pins, so the released
# duckdb loads the loadable; the pipeline sets the same from duckdb_version
OVERRIDE_GIT_DESCRIBE ?= v2.0.0

include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Lint: the pipeline's format and tidy checks, run here. `make lint-tools`
# makes the project venv once (uv). The pipeline pins clang-format 11, which
# has no arm64 build, so the local format pass runs the venv's clang-format
# with the version check off; the tidy pass names the sources itself, as the
# template's tidy-check regex ('src/.*/') matches none of them, and hands
# clang-tidy the SDK on macOS. A second tidy pass flags unused includes alone:
# the include-cleaner's missing-include side reads DuckDB's umbrella headers as
# gaps, so it stays off
LINT_VENV := $(PROJ_DIR).venv
LINT_FORMAT := PATH="$(LINT_VENV)/bin:$$PATH" DUCKDB_FORMAT_SKIP_VERSION_CHECKS=1 $(LINT_VENV)/bin/python duckdb/scripts/format.py --all --workdir $(PROJ_DIR) --directories src test
TIDY_SYSROOT := $(if $(filter Darwin,$(shell uname -s)),--extra-arg=-isysroot$(shell xcrun --show-sdk-path),)

lint-tools:
	uv venv $(LINT_VENV)
	uv pip install --python $(LINT_VENV) clang-format clang-tidy 'black==24.*' cmake-format cxxheaderparser pcpp

lint: release
	$(LINT_FORMAT) --check
	ls src/*.cpp | xargs -P 6 -n 1 $(LINT_VENV)/bin/clang-tidy -p build/release -quiet -header-filter='src/include/gloss.*' --extra-arg=-DDUCKDB_CLANG_TIDY $(TIDY_SYSROOT)
	ls src/*.cpp | xargs -P 6 -n 1 $(LINT_VENV)/bin/clang-tidy -p build/release -quiet --config="{Checks: '-*,misc-include-cleaner', WarningsAsErrors: '*', CheckOptions: {misc-include-cleaner.MissingIncludes: false}}" -header-filter='src/include/gloss.*' --extra-arg=-DDUCKDB_CLANG_TIDY $(TIDY_SYSROOT)

lint-fix:
	$(LINT_FORMAT) --fix --noconfirm
