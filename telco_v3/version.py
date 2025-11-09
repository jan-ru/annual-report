"""
Financial Reporting System Version Information
"""

__version__ = "0.3.0"
__version_info__ = (0, 3, 0)

# Version history
VERSION_HISTORY = {
    "0.3.0": "Fixed Great Tables styling API compatibility, improved test suite",
    "0.2.0": "Modular architecture with separated data processing and presentation",
    "0.1.0": "Initial XAF processing and Polars-based financial statement generation"
}

def get_version():
    """Return the current version string."""
    return __version__

def get_version_info():
    """Return version as tuple (major, minor, patch)."""
    return __version_info__
