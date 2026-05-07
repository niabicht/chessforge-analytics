# conftest.py is automatically loaded by pytest before any tests run.
# Place shared fixtures and path configuration here.

import sys
import os

# Ensure the src directory is on the path so tests can import chessforge modules without needing a pip install. Mirrors the ENV PYTHONPATH=/app/src in the Dockerfile.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
