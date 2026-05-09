"""Vercel entry point — exposes the Flask `app` from the project root."""

import sys
from pathlib import Path

# Ensure the project root is on sys.path so we can import app.py
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app  # noqa: E402,F401  (re-exported for Vercel)
