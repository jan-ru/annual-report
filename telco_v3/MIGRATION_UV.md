# Migration Guide: requirements.txt → pyproject.toml + uv

## Overview

This project has been migrated from traditional `requirements.txt` to modern `pyproject.toml` with `uv` package management.

## Why uv?

- **10-100x faster** than pip for package installation
- **Built-in virtual environment management**
- **Deterministic dependency resolution**
- **Compatible with pip** - can still use pip if needed
- **Modern Python tooling** - follows PEP 621 standards

## Installation

### 1. Install uv

**macOS/Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Via pip:**
```bash
pip install uv
```

**Via Homebrew:**
```bash
brew install uv
```

### 2. Set up the project

```bash
# Create virtual environment
uv venv

# Activate virtual environment
source .venv/bin/activate  # macOS/Linux
# or
.venv\Scripts\activate  # Windows

# Install project with dependencies
uv pip install -e .
```

## Common Commands

### Install dependencies
```bash
uv pip install -e .              # Install project in editable mode
uv pip install -e ".[dev]"       # Install with dev dependencies
```

### Add new dependencies
```bash
# Add to pyproject.toml [project.dependencies] section, then:
uv pip install -e .

# Or install directly:
uv pip install package-name
```

### Update dependencies
```bash
uv pip install --upgrade package-name
uv pip install --upgrade -e .    # Upgrade all
```

### Sync environment (if using uv.lock)
```bash
uv sync                          # Sync from lock file
uv sync --all-extras            # Include optional dependencies
```

### Generate lock file
```bash
uv pip compile pyproject.toml -o requirements.lock
```

## What Changed

### Before (requirements.txt)
```bash
pip install -r requirements.txt
```

### After (pyproject.toml + uv)
```bash
uv pip install -e .
```

## Key Files

- **pyproject.toml** - Project metadata and dependencies (replaces requirements.txt)
- **.python-version** - Specifies Python version (3.11)
- **requirements.txt** - Kept for backwards compatibility (can be removed later)

## Backwards Compatibility

You can still use traditional pip if needed:
```bash
pip install -e .
```

The `pyproject.toml` is fully compatible with pip, setuptools, and other Python tools.

## Benefits

1. **Faster installations** - uv is significantly faster than pip
2. **Better dependency resolution** - Deterministic and reproducible
3. **Modern standards** - PEP 621 compliant
4. **Single source of truth** - All project metadata in one file
5. **Optional dependencies** - Easy to define dev/test/docs extras

## Troubleshooting

### "uv: command not found"
Install uv following the instructions above.

### Virtual environment issues
```bash
# Remove old venv
rm -rf .venv

# Create new one with uv
uv venv
source .venv/bin/activate
uv pip install -e .
```

### Dependency conflicts
```bash
# Clear cache and reinstall
uv cache clean
uv pip install -e . --reinstall
```

## Next Steps

1. ✅ Install uv
2. ✅ Create virtual environment with `uv venv`
3. ✅ Install project with `uv pip install -e .`
4. ✅ Run tests to verify: `python test/telco_test_module.py --all`
5. Optional: Generate lock file with `uv pip compile pyproject.toml -o uv.lock`

## Resources

- [uv Documentation](https://github.com/astral-sh/uv)
- [PEP 621 - Storing project metadata in pyproject.toml](https://peps.python.org/pep-0621/)
- [Python Packaging User Guide](https://packaging.python.org/)
