"""
Pipeline libraries for local and cloud storage operations.

Following inventory pattern - provides utilities for file I/O.
"""

from . import local
from . import aws
from . import spark

__all__ = [
    "local",
    "aws",
    "spark",
]
