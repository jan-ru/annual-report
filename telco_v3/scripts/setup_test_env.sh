#!/bin/bash
# Setup script for Telco module testing

echo "🚀 Setting up Telco Module Testing Environment"
echo "============================================="

# Create test directory structure
mkdir -p test
cd test

# Copy the test script
echo "📁 Creating test directory structure..."

# Create a simple README for the test directory
cat > README.md << 'EOF'
# Telco Module Testing

This directory contains tools for testing individual report modules.

## Quick Start

```bash
# List available modules
python telco_test_module.py --list

# Test a specific module
python telco_test_module.py --income_statement

# Test with rendering
python telco_test_module.py --balance_sheet --render

# Test all modules
python telco_test_module.py --all --render

# Clean up
python telco_test_module.py --clean
```

## Module Testing Workflow

1. **Setup**: `python telco_test_module.py --setup`
2. **Test individual modules**: `python telco_test_module.py --income_statement`
3. **Render reports**: Add `--render` flag to create HTML output
4. **Test everything**: `python telco_test_module.py --all --render`

## Directory Structure

```
test/
├── telco_test_module.py     # Main testing script
├── module_tests/            # Generated test environment
│   ├── report_modules/      # Individual Python modules
│   ├── processed_data/      # Sample data files
│   └── test_*.py           # Generated test scripts
└── README.md               # This file
```

## Debugging Individual Modules

You can also test modules manually:

```bash
cd module_tests
python -c "
exec(open('report_modules/data_loader.py').read())
exec(open('report_modules/income_statement.py').read())
"
```
EOF

echo "✅ Test environment setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Copy telco_test_module.py to the test/ directory"
echo "2. Run: python telco_test_module.py --list"
echo "3. Start testing: python telco_test_module.py --setup"
echo ""
echo "🎯 Example usage:"
echo "   python telco_test_module.py --income_statement --render"
echo "   python telco_test_module.py --all --render"