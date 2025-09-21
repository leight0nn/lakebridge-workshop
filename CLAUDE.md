# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Databricks Labs Lakebridge is a migration tool designed to help organizations migrate data and workloads to the Databricks Lakehouse Platform. The project focuses on SQL transpilation, data reconciliation, and assessment capabilities.

## Development Environment

This project uses **Hatch** as the build tool and environment manager with Python 3.10+.

### Setup Commands
```bash
# Initial setup
make dev                    # Create virtual environment and install dependencies
source .venv/bin/activate   # Activate the virtual environment

# Alternative Python setup with pyenv
make setup_python          # Install and configure Python 3.10 with pyenv
```

## Common Development Commands

### Code Quality
```bash
make lint       # Run linting (calls `hatch run verify`)
make fmt        # Run formatting (calls `hatch run fmt`)

# Or use hatch directly:
hatch run verify    # Run black --check, ruff check, mypy, pylint
hatch run fmt       # Run black, ruff --fix, mypy, pylint
```

### Testing
```bash
make test                   # Unit tests (calls `hatch run test`)
make test-install           # Installation tests (calls `hatch run test-install`)
make integration            # Integration tests (calls `hatch run integration`)
make coverage               # Coverage report with HTML output

# Individual test commands:
hatch run test              # pytest --cov src --cov-report=xml tests/unit
hatch run integration       # pytest --cov src tests/integration/reconcile --durations 20
```

### Documentation
```bash
make docs-install      # Install documentation dependencies (yarn)
make docs-serve-dev    # Serve docs in development mode
make docs-build        # Build documentation
make docs-serve        # Serve built documentation
```

## Architecture Overview

The codebase is organized into several key modules:

### Core Components
- **`src/databricks/labs/lakebridge/`** - Main package root
- **`transpiler/`** - SQL transpilation engine with SQLGlot integration
  - `sqlglot/` - Extended SQLGlot functionality for multiple dialects (Snowflake, Presto, Oracle → Databricks)
  - `lsp/` - Language Server Protocol implementation
- **`reconcile/`** - Data reconciliation functionality
- **`assessments/`** - Migration assessment tools

### Key Dependencies
- **SQLGlot 26.1.3** - SQL parsing and transpilation (pinned version)
- **Databricks SDK** - Databricks platform integration
- **Blueprint** - Databricks Labs utilities and YAML support
- **DuckDB** - Local data processing capabilities

### Configuration Files
- **`pyproject.toml`** - All project configuration (dependencies, tools, scripts)
- **`Makefile`** - Development workflow shortcuts
- **`labs.yml`** - Databricks Labs project configuration

## Testing Strategy
- Unit tests in `tests/unit/`
- Integration tests in `tests/integration/`
- Coverage targets configured in `pyproject.toml`
- Pytest configuration includes async support and custom cache directory

## Code Style
- **Black** formatter with 120 character line length
- **Ruff** linter with extensive rule configuration
- **MyPy** type checking
- **Pylint** with Google Python Style Guide adaptations
- All configuration centralized in `pyproject.toml`
- Always exclude Claude instruction files from GitHub commits